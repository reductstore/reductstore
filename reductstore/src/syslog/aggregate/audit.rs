// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use super::{AGGREGATION_WINDOW_SECS, FORCE_FLUSH_TIMEOUT_SECS};
use crate::core::sync::AsyncRwLock;
use crate::syslog::payload::audit::ApiAuditPayload;
use crate::syslog::{BoxedSystemLogger, LogSystemEvent, SystemEvent, SystemEventKind};
use async_trait::async_trait;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::time::{Duration, Instant};

pub(crate) const AUDIT_CHANNEL_SIZE: usize = 1024;

/// Future returned by a [`SystemEventHandler`] flush callback.
pub(crate) type SystemEventFlushFuture =
    Pin<Box<dyn Future<Output = Result<(), ReductError>> + Send>>;
/// Callback the audit aggregator invokes to persist a flushed event through the
/// shared system logger.
pub(crate) type SystemEventHandler =
    Arc<dyn Fn(SystemEvent) -> SystemEventFlushFuture + Send + Sync>;

/// The aggregating front-end an audit producer logs into. Only the API audit
/// aggregator implements it: it batches `api_call` events in a background
/// worker and flushes them through the handler.
#[async_trait]
pub(crate) trait SystemEventAggregator: Send + Sync {
    async fn log_event(&self, event: SystemEvent) -> Result<(), ReductError>;
}

pub(crate) type BoxedSystemEventAggregator = Box<dyn SystemEventAggregator + Send + Sync>;

/// Wrap an already-built `$system` writer into the aggregated audit logger: a
/// `LogSystemEvent` that funnels `api_call` events through the batching worker
/// and persists flushed events through `inner`.
pub(crate) fn aggregated_audit_logger(inner: BoxedSystemLogger) -> BoxedSystemLogger {
    let system_logger = Arc::new(Mutex::new(inner));
    let handler: SystemEventHandler = Arc::new(move |event| {
        let system_logger = Arc::clone(&system_logger);
        Box::pin(async move { system_logger.lock().await.log_event(event).await })
    });

    Box::new(AggregatedAuditLogger {
        aggregator: Box::new(ApiAuditEventAggregator::new(handler)),
    })
}

/// `LogSystemEvent` adapter over the audit aggregator.
struct AggregatedAuditLogger {
    aggregator: BoxedSystemEventAggregator,
}

#[async_trait]
impl LogSystemEvent for AggregatedAuditLogger {
    async fn log_event(&mut self, event: SystemEvent) -> Result<(), ReductError> {
        self.aggregator.log_event(event).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct AuditAggregateKey {
    pub instance: String,
    pub token_name: String,
    pub method: String,
    pub path: String,
    pub status: u16,
    pub message: String,
    pub client_ip: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct AuditAggregate {
    pub first_timestamp: u64,
    pub last_timestamp: u64,
    pub call_count: u64,
    pub total_duration: f64,
    pub flush_at: Instant,
    pub force_flush_at: Instant,
}

#[derive(Default)]
pub(crate) struct AuditState {
    pub aggregates: HashMap<AuditAggregateKey, AuditAggregate>,
}

pub(crate) fn make_event(key: AuditAggregateKey, aggregate: AuditAggregate) -> SystemEvent {
    let payload = ApiAuditPayload {
        token_name: key.token_name.clone(),
        method: key.method,
        path: key.path,
        client_ip: key.client_ip,
        call_count: aggregate.call_count,
        duration: aggregate.total_duration,
    };

    SystemEvent {
        kind: SystemEventKind::Audit,
        event_type: "api_call".to_string(),
        timestamp: aggregate.first_timestamp,
        instance: key.instance,
        entry_name: payload.entry_name(),
        status: key.status,
        message: key.message,
        payload: payload.to_value(),
    }
}

pub(crate) struct ApiAuditEventAggregator {
    tx: mpsc::Sender<SystemEvent>,
    #[cfg(test)]
    #[allow(dead_code)]
    pub state: Arc<AsyncRwLock<AuditState>>,
}

impl ApiAuditEventAggregator {
    pub(crate) fn new(handler: SystemEventHandler) -> Self {
        let state = Arc::new(AsyncRwLock::new(AuditState::default()));
        let (tx, rx) = mpsc::channel(AUDIT_CHANNEL_SIZE);

        tokio::spawn(Self::run_worker(rx, Arc::clone(&state), handler));

        Self {
            tx,
            #[cfg(test)]
            state,
        }
    }

    async fn enqueue(&self, event: SystemEvent) -> Result<(), ReductError> {
        self.tx
            .send(event)
            .await
            .map_err(|_| internal_server_error!("Audit worker is not available"))
    }

    async fn run_worker(
        mut rx: mpsc::Receiver<SystemEvent>,
        state: Arc<AsyncRwLock<AuditState>>,
        handler: SystemEventHandler,
    ) {
        loop {
            if let Some(deadline) = Self::next_deadline(&state).await {
                tokio::select! {
                    maybe_event = rx.recv() => {
                        match maybe_event {
                            Some(event) => {
                                if event.event_type == "api_call" {
                                    let _ = Self::aggregate_event(&state, event).await;
                                } else {
                                    let _ = handler(event).await;
                                }
                            }
                            None => {
                                let _ = Self::flush_all(&state, &handler).await;
                                break;
                            }
                        }
                    }
                    _ = tokio::time::sleep_until(deadline) => {
                        let _ = Self::flush_expired(&state, &handler).await;
                    }
                }
            } else {
                match rx.recv().await {
                    Some(event) => {
                        if event.event_type == "api_call" {
                            let _ = Self::aggregate_event(&state, event).await;
                        } else {
                            let _ = handler(event).await;
                        }
                    }
                    None => break,
                }
            }
        }
    }

    async fn aggregate_event(
        state: &Arc<AsyncRwLock<AuditState>>,
        event: SystemEvent,
    ) -> Result<(), ReductError> {
        let payload: ApiAuditPayload = serde_json::from_value(event.payload.clone())
            .map_err(|err| internal_server_error!("Invalid API audit payload: {}", err))?;
        let now = Instant::now();
        let key = AuditAggregateKey {
            instance: event.instance,
            token_name: payload.token_name,
            method: payload.method,
            path: payload.path,
            status: event.status,
            message: event.message,
            client_ip: payload.client_ip,
        };

        let mut state = state.write().await?;
        state
            .aggregates
            .entry(key)
            .and_modify(|aggregate| {
                aggregate.call_count += payload.call_count;
                aggregate.total_duration += payload.duration;
                aggregate.last_timestamp = event.timestamp;
                if event.timestamp < aggregate.first_timestamp {
                    aggregate.first_timestamp = event.timestamp;
                }
                aggregate.flush_at = now + Duration::from_secs(AGGREGATION_WINDOW_SECS);
            })
            .or_insert_with(|| AuditAggregate {
                first_timestamp: event.timestamp,
                last_timestamp: event.timestamp,
                call_count: payload.call_count,
                total_duration: payload.duration,
                flush_at: now + Duration::from_secs(AGGREGATION_WINDOW_SECS),
                force_flush_at: now + Duration::from_secs(FORCE_FLUSH_TIMEOUT_SECS),
            });

        Ok(())
    }

    async fn next_deadline(state: &Arc<AsyncRwLock<AuditState>>) -> Option<Instant> {
        let state = state.read().await.ok()?;
        state
            .aggregates
            .values()
            .map(|aggregate| aggregate.flush_at.min(aggregate.force_flush_at))
            .min()
    }

    async fn flush_expired(
        state: &Arc<AsyncRwLock<AuditState>>,
        handler: &SystemEventHandler,
    ) -> Result<(), ReductError> {
        let now = Instant::now();
        let events = {
            let mut state = state.write().await?;
            let expired_keys: Vec<_> = state
                .aggregates
                .iter()
                .filter(|(_, aggregate)| {
                    aggregate.flush_at <= now || aggregate.force_flush_at <= now
                })
                .map(|(key, _)| key.clone())
                .collect();

            expired_keys
                .into_iter()
                .filter_map(|key| {
                    state
                        .aggregates
                        .remove(&key)
                        .map(|aggregate| make_event(key, aggregate))
                })
                .collect::<Vec<_>>()
        };

        for event in events {
            handler(event).await?;
        }

        Ok(())
    }

    async fn flush_all(
        state: &Arc<AsyncRwLock<AuditState>>,
        handler: &SystemEventHandler,
    ) -> Result<(), ReductError> {
        let events = {
            let mut state = state.write().await?;
            state
                .aggregates
                .drain()
                .map(|(key, aggregate)| make_event(key, aggregate))
                .collect::<Vec<_>>()
        };

        for event in events {
            handler(event).await?;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl SystemEventAggregator for ApiAuditEventAggregator {
    async fn log_event(&self, event: SystemEvent) -> Result<(), ReductError> {
        self.enqueue(event).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::{fixture, rstest};
    use tokio::sync::Mutex;

    fn make_test_event(timestamp: u64) -> SystemEvent {
        let payload = ApiAuditPayload {
            token_name: "token-1".to_string(),
            method: "GET".to_string(),
            path: "/api/v1/info".to_string(),
            client_ip: None,
            call_count: 1,
            duration: 0.1,
        };

        SystemEvent {
            kind: SystemEventKind::Audit,
            event_type: "api_call".to_string(),
            timestamp,
            instance: "instance-a".to_string(),
            entry_name: payload.entry_name(),
            status: 200,
            message: "".to_string(),
            payload: payload.to_value(),
        }
    }

    fn make_test_handler(events: Arc<Mutex<Vec<SystemEvent>>>) -> SystemEventHandler {
        Arc::new(move |event| {
            let events = Arc::clone(&events);
            Box::pin(async move {
                events.lock().await.push(event);
                Ok(())
            })
        })
    }

    fn make_test_key(
        instance: &str,
        token_name: &str,
        method: &str,
        path: &str,
        status: u16,
        message: &str,
    ) -> AuditAggregateKey {
        AuditAggregateKey {
            instance: instance.to_string(),
            token_name: token_name.to_string(),
            method: method.to_string(),
            path: path.to_string(),
            status,
            message: message.to_string(),
            client_ip: None,
        }
    }

    fn make_test_aggregate(
        first_timestamp: u64,
        call_count: u64,
        total_duration: f64,
        flush_at: Instant,
        force_flush_at: Instant,
    ) -> AuditAggregate {
        AuditAggregate {
            first_timestamp,
            last_timestamp: first_timestamp,
            call_count,
            total_duration,
            flush_at,
            force_flush_at,
        }
    }

    #[fixture]
    fn state() -> Arc<AsyncRwLock<AuditState>> {
        Arc::new(AsyncRwLock::new(AuditState::default()))
    }

    #[fixture]
    fn flushed_events() -> Arc<Mutex<Vec<SystemEvent>>> {
        Arc::new(Mutex::new(Vec::new()))
    }

    #[rstest]
    fn make_event_uses_first_timestamp_and_aggregates_duration() {
        let event = make_event(
            make_test_key("instance-a", "token-1", "GET", "/api/v1/info", 200, ""),
            AuditAggregate {
                first_timestamp: 10,
                last_timestamp: 20,
                call_count: 3,
                total_duration: 4.5,
                flush_at: Instant::now(),
                force_flush_at: Instant::now(),
            },
        );

        assert_eq!(event.timestamp, 10);
        assert_eq!(event.instance, "instance-a");
        assert_eq!(event.status, 200);
        let payload: ApiAuditPayload = serde_json::from_value(event.payload).unwrap();
        assert_eq!(payload.token_name, "token-1");
        assert_eq!(payload.method, "GET");
        assert_eq!(payload.path, "/api/v1/info");
        assert_eq!(payload.call_count, 3);
        assert!((payload.duration - 4.5).abs() < 1e-9);
    }

    #[rstest]
    #[tokio::test]
    async fn next_deadline_returns_none_when_empty(state: Arc<AsyncRwLock<AuditState>>) {
        assert!(ApiAuditEventAggregator::next_deadline(&state)
            .await
            .is_none());
    }

    #[rstest]
    #[tokio::test]
    async fn next_deadline_returns_earliest_deadline(state: Arc<AsyncRwLock<AuditState>>) {
        let earlier = Instant::now() + Duration::from_secs(1);
        let later = Instant::now() + Duration::from_secs(5);
        let much_later = Instant::now() + Duration::from_secs(20);
        let mut guard = state.write().await.unwrap();
        guard.aggregates.insert(
            make_test_key("instance-a", "token-1", "GET", "/api/v1/info", 200, ""),
            make_test_aggregate(1, 1, 1.0, later, much_later),
        );
        guard.aggregates.insert(
            make_test_key("instance-a", "token-2", "GET", "/api/v1/info", 200, ""),
            make_test_aggregate(2, 1, 2.0, much_later, earlier),
        );
        drop(guard);

        assert_eq!(
            ApiAuditEventAggregator::next_deadline(&state).await,
            Some(earlier)
        );
    }

    #[rstest]
    #[tokio::test]
    async fn aggregate_event_updates_existing_entry(state: Arc<AsyncRwLock<AuditState>>) {
        ApiAuditEventAggregator::aggregate_event(&state, make_test_event(20))
            .await
            .unwrap();
        ApiAuditEventAggregator::aggregate_event(&state, make_test_event(10))
            .await
            .unwrap();

        let guard = state.read().await.unwrap();
        let aggregate = guard.aggregates.values().next().unwrap();
        assert_eq!(aggregate.call_count, 2);
        assert!((aggregate.total_duration - 0.2).abs() < 1e-9);
        assert_eq!(aggregate.first_timestamp, 10);
        assert_eq!(aggregate.last_timestamp, 10);
    }

    #[rstest]
    #[tokio::test]
    async fn aggregate_event_extends_sliding_flush_only(state: Arc<AsyncRwLock<AuditState>>) {
        ApiAuditEventAggregator::aggregate_event(&state, make_test_event(1))
            .await
            .unwrap();
        let (initial_flush_at, initial_force_flush_at) = {
            let guard = state.read().await.unwrap();
            let aggregate = guard.aggregates.values().next().unwrap();
            (aggregate.flush_at, aggregate.force_flush_at)
        };

        tokio::time::sleep(Duration::from_millis(10)).await;

        ApiAuditEventAggregator::aggregate_event(&state, make_test_event(2))
            .await
            .unwrap();
        let guard = state.read().await.unwrap();
        let aggregate = guard.aggregates.values().next().unwrap();
        assert!(aggregate.flush_at > initial_flush_at);
        assert_eq!(aggregate.force_flush_at, initial_force_flush_at);
    }

    #[rstest]
    #[tokio::test]
    async fn flush_expired_flushes_only_expired_entries(
        state: Arc<AsyncRwLock<AuditState>>,
        flushed_events: Arc<Mutex<Vec<SystemEvent>>>,
    ) {
        let handler = make_test_handler(Arc::clone(&flushed_events));
        let now = Instant::now();

        let mut guard = state.write().await.unwrap();
        guard.aggregates.insert(
            make_test_key("instance-a", "expired", "GET", "/expired", 200, ""),
            make_test_aggregate(
                1,
                1,
                1.0,
                now - Duration::from_millis(1),
                now + Duration::from_secs(10),
            ),
        );
        guard.aggregates.insert(
            make_test_key("instance-a", "pending", "GET", "/pending", 200, ""),
            make_test_aggregate(
                2,
                1,
                1.0,
                now + Duration::from_secs(10),
                now + Duration::from_secs(10),
            ),
        );
        drop(guard);

        ApiAuditEventAggregator::flush_expired(&state, &handler)
            .await
            .unwrap();

        let events = flushed_events.lock().await;
        assert_eq!(events.len(), 1);
        let payload: ApiAuditPayload = serde_json::from_value(events[0].payload.clone()).unwrap();
        assert_eq!(payload.token_name, "expired");
        drop(events);

        let guard = state.read().await.unwrap();
        assert_eq!(guard.aggregates.len(), 1);
        assert!(guard
            .aggregates
            .keys()
            .any(|key| key.token_name == "pending"));
    }

    #[rstest]
    #[tokio::test]
    async fn flush_expired_flushes_entry_when_force_deadline_expires(
        state: Arc<AsyncRwLock<AuditState>>,
        flushed_events: Arc<Mutex<Vec<SystemEvent>>>,
    ) {
        let handler = make_test_handler(Arc::clone(&flushed_events));
        let now = Instant::now();
        let mut guard = state.write().await.unwrap();
        guard.aggregates.insert(
            make_test_key("instance-a", "forced", "GET", "/forced", 200, ""),
            make_test_aggregate(
                3,
                2,
                2.0,
                now + Duration::from_secs(10),
                now - Duration::from_millis(1),
            ),
        );
        drop(guard);

        ApiAuditEventAggregator::flush_expired(&state, &handler)
            .await
            .unwrap();

        let events = flushed_events.lock().await;
        assert_eq!(events.len(), 1);
        let payload: ApiAuditPayload = serde_json::from_value(events[0].payload.clone()).unwrap();
        assert_eq!(payload.token_name, "forced");
        assert!(state.read().await.unwrap().aggregates.is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn flush_all_drains_all_entries(
        state: Arc<AsyncRwLock<AuditState>>,
        flushed_events: Arc<Mutex<Vec<SystemEvent>>>,
    ) {
        let handler = make_test_handler(Arc::clone(&flushed_events));

        let mut guard = state.write().await.unwrap();
        for (idx, token_name) in ["token-1", "token-2"].into_iter().enumerate() {
            guard.aggregates.insert(
                make_test_key("instance-a", token_name, "GET", "/api/v1/info", 200, ""),
                make_test_aggregate(idx as u64 + 1, 1, 1.0, Instant::now(), Instant::now()),
            );
        }
        drop(guard);

        ApiAuditEventAggregator::flush_all(&state, &handler)
            .await
            .unwrap();

        assert_eq!(flushed_events.lock().await.len(), 2);
        assert!(state.read().await.unwrap().aggregates.is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn log_event_returns_internal_error_when_worker_channel_is_closed() {
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let aggregator = ApiAuditEventAggregator {
            tx,
            state: Arc::new(AsyncRwLock::new(AuditState::default())),
        };

        let err = aggregator.log_event(make_test_event(1)).await.unwrap_err();
        assert_eq!(
            err.status,
            reduct_base::error::ErrorCode::InternalServerError
        );
        assert_eq!(err.message, "Audit worker is not available");
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn run_worker_flushes_all_when_channel_is_closed(
        flushed_events: Arc<Mutex<Vec<SystemEvent>>>,
    ) {
        let handler = make_test_handler(Arc::clone(&flushed_events));
        let state = Arc::new(AsyncRwLock::new(AuditState::default()));
        let (tx, rx) = mpsc::channel(4);

        tx.send(make_test_event(1)).await.unwrap();
        drop(tx);

        ApiAuditEventAggregator::run_worker(rx, Arc::clone(&state), handler).await;

        let events = flushed_events.lock().await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].timestamp, 1);
        assert!(state.read().await.unwrap().aggregates.is_empty());
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn run_worker_forwards_non_api_event_without_aggregation(
        flushed_events: Arc<Mutex<Vec<SystemEvent>>>,
    ) {
        let handler = make_test_handler(Arc::clone(&flushed_events));
        let state = Arc::new(AsyncRwLock::new(AuditState::default()));
        let (tx, rx) = mpsc::channel(4);

        tx.send(SystemEvent {
            kind: SystemEventKind::Lifecycle,
            event_type: "lifecycle_run".to_string(),
            timestamp: 1,
            instance: "instance-a".to_string(),
            entry_name: "system-lifecycle".to_string(),
            status: 200,
            message: "ok".to_string(),
            payload: serde_json::json!({"policy_name":"p1"}),
        })
        .await
        .unwrap();
        drop(tx);

        ApiAuditEventAggregator::run_worker(rx, Arc::clone(&state), handler).await;
        let events = flushed_events.lock().await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "lifecycle_run");
        assert!(state.read().await.unwrap().aggregates.is_empty());
    }
}
