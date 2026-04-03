// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::audit::AuditEvent;
use crate::core::sync::AsyncRwLock;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};

#[cfg(not(test))]
pub(super) const AGGREGATION_WINDOW_SECS: u64 = 5;
#[cfg(test)]
pub(super) const AGGREGATION_WINDOW_SECS: u64 = 1;
pub(super) const FORCE_FLUSH_TIMEOUT_SECS: u64 = 60;
pub(super) const AUDIT_CHANNEL_SIZE: usize = 1024;

pub(super) type FlushFuture = Pin<Box<dyn Future<Output = Result<(), ReductError>> + Send>>;
pub(super) type FlushHandler = Arc<dyn Fn(AuditEvent) -> FlushFuture + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct AuditAggregateKey {
    pub instance: String,
    pub token_name: String,
    pub endpoint: String,
    pub status: u16,
    pub message: String,
    pub client_ip: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) struct AuditAggregate {
    pub first_timestamp: u64,
    pub last_timestamp: u64,
    pub call_count: u64,
    pub total_duration: f64,
    pub flush_at: Instant,
    pub force_flush_at: Instant,
}

#[derive(Default)]
pub(super) struct AuditState {
    pub aggregates: HashMap<AuditAggregateKey, AuditAggregate>,
}

pub(super) fn make_event(key: AuditAggregateKey, aggregate: AuditAggregate) -> AuditEvent {
    AuditEvent {
        timestamp: aggregate.first_timestamp,
        instance: key.instance,
        token_name: key.token_name,
        endpoint: key.endpoint,
        status: key.status,
        message: key.message,
        client_ip: key.client_ip,
        call_count: aggregate.call_count,
        duration: aggregate.total_duration,
    }
}

pub(super) struct AuditAggregator {
    tx: mpsc::Sender<AuditEvent>,
    #[cfg(test)]
    pub state: Arc<AsyncRwLock<AuditState>>,
}

impl AuditAggregator {
    pub(super) fn new(handler: FlushHandler) -> Self {
        let state = Arc::new(AsyncRwLock::new(AuditState::default()));
        let (tx, rx) = mpsc::channel(AUDIT_CHANNEL_SIZE);

        tokio::spawn(Self::run_worker(rx, Arc::clone(&state), handler));

        Self {
            tx,
            #[cfg(test)]
            state,
        }
    }

    pub(super) async fn log_event(&self, event: AuditEvent) -> Result<(), ReductError> {
        self.tx
            .send(event)
            .await
            .map_err(|_| internal_server_error!("Audit worker is not available"))
    }

    async fn run_worker(
        mut rx: mpsc::Receiver<AuditEvent>,
        state: Arc<AsyncRwLock<AuditState>>,
        handler: FlushHandler,
    ) {
        loop {
            if let Some(deadline) = Self::next_deadline(&state).await {
                tokio::select! {
                    maybe_event = rx.recv() => {
                        match maybe_event {
                            Some(event) => {
                                let _ = Self::aggregate_event(&state, event).await;
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
                        let _ = Self::aggregate_event(&state, event).await;
                    }
                    None => break,
                }
            }
        }
    }

    async fn aggregate_event(
        state: &Arc<AsyncRwLock<AuditState>>,
        event: AuditEvent,
    ) -> Result<(), ReductError> {
        let now = Instant::now();
        let key = AuditAggregateKey {
            instance: event.instance,
            token_name: event.token_name,
            endpoint: event.endpoint,
            status: event.status,
            message: event.message,
            client_ip: event.client_ip,
        };

        let mut state = state.write().await?;
        state
            .aggregates
            .entry(key)
            .and_modify(|aggregate| {
                aggregate.call_count += event.call_count;
                aggregate.total_duration += event.duration;
                aggregate.last_timestamp = event.timestamp;
                if event.timestamp < aggregate.first_timestamp {
                    aggregate.first_timestamp = event.timestamp;
                }
                aggregate.flush_at = now + Duration::from_secs(AGGREGATION_WINDOW_SECS);
            })
            .or_insert_with(|| AuditAggregate {
                first_timestamp: event.timestamp,
                last_timestamp: event.timestamp,
                call_count: event.call_count,
                total_duration: event.duration,
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
        handler: &FlushHandler,
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
        handler: &FlushHandler,
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

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::{fixture, rstest};
    use tokio::sync::Mutex;

    fn make_test_event(timestamp: u64) -> AuditEvent {
        AuditEvent {
            timestamp,
            instance: "instance-a".to_string(),
            token_name: "token-1".to_string(),
            endpoint: "GET /api/v1/info".to_string(),
            status: 200,
            message: "".to_string(),
            client_ip: None,
            call_count: 1,
            duration: 0.1,
        }
    }

    fn make_test_handler(events: Arc<Mutex<Vec<AuditEvent>>>) -> FlushHandler {
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
        endpoint: &str,
        status: u16,
        message: &str,
    ) -> AuditAggregateKey {
        AuditAggregateKey {
            instance: instance.to_string(),
            token_name: token_name.to_string(),
            endpoint: endpoint.to_string(),
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
    fn flushed_events() -> Arc<Mutex<Vec<AuditEvent>>> {
        Arc::new(Mutex::new(Vec::new()))
    }

    #[rstest]
    fn make_event_uses_first_timestamp_and_aggregates_duration() {
        let event = make_event(
            make_test_key("instance-a", "token-1", "GET /api/v1/info", 200, ""),
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
        assert_eq!(event.token_name, "token-1");
        assert_eq!(event.endpoint, "GET /api/v1/info");
        assert_eq!(event.status, 200);
        assert_eq!(event.call_count, 3);
        assert!((event.duration - 4.5).abs() < 1e-9);
    }

    #[rstest]
    #[tokio::test]
    async fn next_deadline_returns_none_when_empty(state: Arc<AsyncRwLock<AuditState>>) {
        assert!(AuditAggregator::next_deadline(&state).await.is_none());
    }

    #[rstest]
    #[tokio::test]
    async fn next_deadline_returns_earliest_deadline(state: Arc<AsyncRwLock<AuditState>>) {
        let earlier = Instant::now() + Duration::from_secs(1);
        let later = Instant::now() + Duration::from_secs(5);
        let much_later = Instant::now() + Duration::from_secs(20);
        let mut guard = state.write().await.unwrap();
        guard.aggregates.insert(
            make_test_key("instance-a", "token-1", "GET /api/v1/info", 200, ""),
            make_test_aggregate(1, 1, 1.0, later, much_later),
        );
        guard.aggregates.insert(
            make_test_key("instance-a", "token-2", "GET /api/v1/info", 200, ""),
            make_test_aggregate(2, 1, 2.0, much_later, earlier),
        );
        drop(guard);

        assert_eq!(AuditAggregator::next_deadline(&state).await, Some(earlier));
    }

    #[rstest]
    #[tokio::test]
    async fn aggregate_event_updates_existing_entry(state: Arc<AsyncRwLock<AuditState>>) {
        AuditAggregator::aggregate_event(&state, make_test_event(20))
            .await
            .unwrap();
        AuditAggregator::aggregate_event(&state, make_test_event(10))
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
        AuditAggregator::aggregate_event(&state, make_test_event(1))
            .await
            .unwrap();
        let (initial_flush_at, initial_force_flush_at) = {
            let guard = state.read().await.unwrap();
            let aggregate = guard.aggregates.values().next().unwrap();
            (aggregate.flush_at, aggregate.force_flush_at)
        };

        tokio::time::sleep(Duration::from_millis(10)).await;

        AuditAggregator::aggregate_event(&state, make_test_event(2))
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
        flushed_events: Arc<Mutex<Vec<AuditEvent>>>,
    ) {
        let handler = make_test_handler(Arc::clone(&flushed_events));
        let now = Instant::now();

        let mut guard = state.write().await.unwrap();
        guard.aggregates.insert(
            make_test_key("instance-a", "expired", "GET /expired", 200, ""),
            make_test_aggregate(
                1,
                1,
                1.0,
                now - Duration::from_millis(1),
                now + Duration::from_secs(10),
            ),
        );
        guard.aggregates.insert(
            make_test_key("instance-a", "pending", "GET /pending", 200, ""),
            make_test_aggregate(
                2,
                1,
                1.0,
                now + Duration::from_secs(10),
                now + Duration::from_secs(10),
            ),
        );
        drop(guard);

        AuditAggregator::flush_expired(&state, &handler)
            .await
            .unwrap();

        let events = flushed_events.lock().await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].token_name, "expired");
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
        flushed_events: Arc<Mutex<Vec<AuditEvent>>>,
    ) {
        let handler = make_test_handler(Arc::clone(&flushed_events));
        let now = Instant::now();
        let mut guard = state.write().await.unwrap();
        guard.aggregates.insert(
            make_test_key("instance-a", "forced", "GET /forced", 200, ""),
            make_test_aggregate(
                3,
                2,
                2.0,
                now + Duration::from_secs(10),
                now - Duration::from_millis(1),
            ),
        );
        drop(guard);

        AuditAggregator::flush_expired(&state, &handler)
            .await
            .unwrap();

        let events = flushed_events.lock().await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].token_name, "forced");
        assert!(state.read().await.unwrap().aggregates.is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn flush_all_drains_all_entries(
        state: Arc<AsyncRwLock<AuditState>>,
        flushed_events: Arc<Mutex<Vec<AuditEvent>>>,
    ) {
        let handler = make_test_handler(Arc::clone(&flushed_events));

        let mut guard = state.write().await.unwrap();
        for (idx, token_name) in ["token-1", "token-2"].into_iter().enumerate() {
            guard.aggregates.insert(
                make_test_key("instance-a", token_name, "GET /api/v1/info", 200, ""),
                make_test_aggregate(idx as u64 + 1, 1, 1.0, Instant::now(), Instant::now()),
            );
        }
        drop(guard);

        AuditAggregator::flush_all(&state, &handler).await.unwrap();

        assert_eq!(flushed_events.lock().await.len(), 2);
        assert!(state.read().await.unwrap().aggregates.is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn log_event_returns_internal_error_when_worker_channel_is_closed() {
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let aggregator = AuditAggregator {
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
        flushed_events: Arc<Mutex<Vec<AuditEvent>>>,
    ) {
        let handler = make_test_handler(Arc::clone(&flushed_events));
        let state = Arc::new(AsyncRwLock::new(AuditState::default()));
        let (tx, rx) = mpsc::channel(4);

        tx.send(make_test_event(1)).await.unwrap();
        drop(tx);

        AuditAggregator::run_worker(rx, Arc::clone(&state), handler).await;

        let events = flushed_events.lock().await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].timestamp, 1);
        assert!(state.read().await.unwrap().aggregates.is_empty());
    }
}
