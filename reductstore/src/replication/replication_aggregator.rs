// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

//! Aggregates replication diagnostics into periodic system events.
//!
//! One active "bucket" (running tally) is kept per replication task, keyed by
//! the status code. The bucket is flushed and a new one opened when ANY of:
//!   1. the status (the key) changes (e.g. 200 -> 404),
//!   2. no records were replicated for [`AGGREGATION_WINDOW_SECS`] (idle),
//!   3. the bucket accumulated [`FORCE_FLUSH_TIMEOUT_SECS`] of data (cap).
//!
//! The window/force-flush timing (conditions 2 and 3) reuses the model of
//! [`crate::api::audit::aggregator::ApiAuditEventAggregator`]; the status-change
//! trigger (condition 1) is added on top.

use crate::api::audit::aggregator::{AGGREGATION_WINDOW_SECS, FORCE_FLUSH_TIMEOUT_SECS};
use crate::core::sync::AsyncRwLock;
use crate::lifecycle::SystemEventSink;
use crate::replication::replication_event_payload::ReplicationSystemEventPayload;
use crate::syslog::{SystemEvent, SystemEventHandler};
use log::error;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};

const REPLICATION_CHANNEL_SIZE: usize = 1024;
const REPLICATION_EVENT_TYPE: &str = "replication_sync";

/// A single observation reported by a replication pass for one status code.
#[derive(Debug, Clone)]
pub(crate) struct ReplicationObservation {
    pub instance: String,
    pub replication_name: String,
    pub timestamp: u64,
    pub status: u16,
    pub pending_records: u64,
    pub records: u64,
    pub data_size: u64,
    pub duration: f64,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ReplicationAggregateKey {
    pub instance: String,
    pub replication_name: String,
}

#[derive(Debug, Clone)]
pub(crate) struct ReplicationAggregate {
    pub status: u16,
    pub first_timestamp: u64,
    pub last_timestamp: u64,
    pub pending_records: u64,
    pub written_records: u64,
    pub failed_records: u64,
    pub replicated_data_size: u64,
    pub duration: f64,
    pub message: String,
    pub flush_at: Instant,
    pub force_flush_at: Instant,
}

#[derive(Default)]
pub(crate) struct ReplicationAggregatorState {
    pub aggregates: HashMap<ReplicationAggregateKey, ReplicationAggregate>,
}

fn is_success(status: u16) -> bool {
    (200..300).contains(&status)
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

fn new_aggregate(obs: &ReplicationObservation, now: Instant) -> ReplicationAggregate {
    let success = is_success(obs.status);
    ReplicationAggregate {
        status: obs.status,
        first_timestamp: obs.timestamp,
        last_timestamp: obs.timestamp,
        pending_records: obs.pending_records,
        written_records: if success { obs.records } else { 0 },
        failed_records: if success { 0 } else { obs.records },
        replicated_data_size: obs.data_size,
        duration: obs.duration,
        message: obs.message.clone(),
        flush_at: now + Duration::from_secs(AGGREGATION_WINDOW_SECS),
        force_flush_at: now + Duration::from_secs(FORCE_FLUSH_TIMEOUT_SECS),
    }
}

pub(crate) fn make_event(
    key: ReplicationAggregateKey,
    aggregate: ReplicationAggregate,
) -> SystemEvent {
    let payload = ReplicationSystemEventPayload {
        status: aggregate.status,
        pending_records: aggregate.pending_records,
        written_records: aggregate.written_records,
        failed_records: aggregate.failed_records,
        replicated_data_size: aggregate.replicated_data_size,
        duration: aggregate.duration,
    };

    SystemEvent {
        event_type: REPLICATION_EVENT_TYPE.to_string(),
        timestamp: aggregate.first_timestamp,
        instance: key.instance,
        entry_name: key.replication_name,
        status: aggregate.status,
        message: aggregate.message,
        payload: payload.to_value(),
    }
}

pub(crate) struct ReplicationEventAggregator {
    instance_name: String,
    replication_name: String,
    tx: mpsc::Sender<ReplicationObservation>,
    #[cfg(test)]
    #[allow(dead_code)]
    pub state: Arc<AsyncRwLock<ReplicationAggregatorState>>,
}

impl ReplicationEventAggregator {
    pub(crate) fn new(sink: SystemEventSink, replication_name: String) -> Self {
        let instance_name = sink.instance_name.clone();
        let logger = Arc::clone(&sink.system_logger);
        let handler: SystemEventHandler = Arc::new(move |event| {
            let logger = Arc::clone(&logger);
            Box::pin(async move {
                let mut guard = logger.write().await?;
                guard.log_event(event).await
            })
        });

        let state = Arc::new(AsyncRwLock::new(ReplicationAggregatorState::default()));
        let (tx, rx) = mpsc::channel(REPLICATION_CHANNEL_SIZE);
        tokio::spawn(Self::run_worker(rx, Arc::clone(&state), handler));

        Self {
            instance_name,
            replication_name,
            tx,
            #[cfg(test)]
            state,
        }
    }

    /// Record a replication pass.
    ///
    /// Telemetry must never break replication, so enqueue failures are
    /// swallowed and logged rather than propagated. The pass `duration` is
    /// attributed to the lowest-status observation only, so it is never
    /// double-counted when a single pass produces several status codes.
    pub(crate) async fn record_pass(
        &self,
        pending_records: u64,
        duration: f64,
        counter: &[(Result<(), ReductError>, u64, u64)],
    ) {
        let mut per_status: BTreeMap<u16, (u64, u64, String)> = BTreeMap::new();
        for (result, records, data_size) in counter {
            let (status, message) = match result {
                Ok(_) => (200u16, String::new()),
                Err(err) => (err.status as u16, err.message.clone()),
            };
            let entry = per_status.entry(status).or_default();
            entry.0 += *records;
            entry.1 += *data_size;
            if !message.is_empty() {
                entry.2 = message;
            }
        }

        if per_status.is_empty() {
            return;
        }

        let timestamp = now_micros();
        let mut first = true;
        for (status, (records, data_size, message)) in per_status {
            let observation = ReplicationObservation {
                instance: self.instance_name.clone(),
                replication_name: self.replication_name.clone(),
                timestamp,
                status,
                pending_records,
                records,
                data_size,
                duration: if first { duration } else { 0.0 },
                message,
            };
            first = false;
            if let Err(err) = self.enqueue(observation).await {
                error!(
                    "Failed to enqueue replication diagnostics for '{}': {}",
                    self.replication_name, err
                );
            }
        }
    }

    async fn enqueue(&self, observation: ReplicationObservation) -> Result<(), ReductError> {
        self.tx
            .send(observation)
            .await
            .map_err(|_| internal_server_error!("Replication diagnostics worker is not available"))
    }

    async fn run_worker(
        mut rx: mpsc::Receiver<ReplicationObservation>,
        state: Arc<AsyncRwLock<ReplicationAggregatorState>>,
        handler: SystemEventHandler,
    ) {
        loop {
            if let Some(deadline) = Self::next_deadline(&state).await {
                tokio::select! {
                    maybe_observation = rx.recv() => {
                        match maybe_observation {
                            Some(observation) => {
                                Self::handle_observation(&state, &handler, observation).await;
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
                    Some(observation) => {
                        Self::handle_observation(&state, &handler, observation).await;
                    }
                    None => break,
                }
            }
        }
    }

    async fn handle_observation(
        state: &Arc<AsyncRwLock<ReplicationAggregatorState>>,
        handler: &SystemEventHandler,
        observation: ReplicationObservation,
    ) {
        match Self::aggregate_event(state, observation).await {
            Ok(events) => Self::emit_all(handler, events).await,
            Err(err) => error!("Failed to aggregate replication diagnostics: {}", err),
        }
    }

    /// Apply an observation to the active bucket, returning any event displaced
    /// by a status change (condition 1).
    async fn aggregate_event(
        state: &Arc<AsyncRwLock<ReplicationAggregatorState>>,
        observation: ReplicationObservation,
    ) -> Result<Vec<SystemEvent>, ReductError> {
        let now = Instant::now();
        let key = ReplicationAggregateKey {
            instance: observation.instance.clone(),
            replication_name: observation.replication_name.clone(),
        };

        let mut state = state.write().await?;
        let mut displaced = Vec::new();
        match state.aggregates.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                if entry.get().status != observation.status {
                    let previous = entry.insert(new_aggregate(&observation, now));
                    displaced.push(make_event(key, previous));
                } else {
                    let aggregate = entry.get_mut();
                    if is_success(observation.status) {
                        aggregate.written_records += observation.records;
                        aggregate.replicated_data_size += observation.data_size;
                    } else {
                        aggregate.failed_records += observation.records;
                    }
                    aggregate.pending_records = observation.pending_records;
                    aggregate.duration += observation.duration;
                    aggregate.last_timestamp = observation.timestamp;
                    if observation.timestamp < aggregate.first_timestamp {
                        aggregate.first_timestamp = observation.timestamp;
                    }
                    if !observation.message.is_empty() {
                        aggregate.message = observation.message;
                    }
                    aggregate.flush_at = now + Duration::from_secs(AGGREGATION_WINDOW_SECS);
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(new_aggregate(&observation, now));
            }
        }

        Ok(displaced)
    }

    async fn next_deadline(
        state: &Arc<AsyncRwLock<ReplicationAggregatorState>>,
    ) -> Option<Instant> {
        let state = state.read().await.ok()?;
        state
            .aggregates
            .values()
            .map(|aggregate| aggregate.flush_at.min(aggregate.force_flush_at))
            .min()
    }

    async fn flush_expired(
        state: &Arc<AsyncRwLock<ReplicationAggregatorState>>,
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

        Self::emit_all(handler, events).await;
        Ok(())
    }

    async fn flush_all(
        state: &Arc<AsyncRwLock<ReplicationAggregatorState>>,
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

        Self::emit_all(handler, events).await;
        Ok(())
    }

    async fn emit_all(handler: &SystemEventHandler, events: Vec<SystemEvent>) {
        for event in events {
            if let Err(err) = handler(event).await {
                error!("Failed to persist replication system event: {}", err);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reduct_base::{not_found, timeout};
    use rstest::{fixture, rstest};
    use std::sync::Mutex;

    fn observation(status: u16, records: u64, data_size: u64) -> ReplicationObservation {
        ReplicationObservation {
            instance: "instance-a".to_string(),
            replication_name: "repl-1".to_string(),
            timestamp: 10,
            status,
            pending_records: 3,
            records,
            data_size,
            duration: 0.5,
            message: if is_success(status) {
                String::new()
            } else {
                "boom".to_string()
            },
        }
    }

    fn make_test_handler(events: Arc<Mutex<Vec<SystemEvent>>>) -> SystemEventHandler {
        Arc::new(move |event| {
            let events = Arc::clone(&events);
            Box::pin(async move {
                events.lock().unwrap().push(event);
                Ok(())
            })
        })
    }

    #[fixture]
    fn state() -> Arc<AsyncRwLock<ReplicationAggregatorState>> {
        Arc::new(AsyncRwLock::new(ReplicationAggregatorState::default()))
    }

    #[fixture]
    fn flushed_events() -> Arc<Mutex<Vec<SystemEvent>>> {
        Arc::new(Mutex::new(Vec::new()))
    }

    #[rstest]
    fn make_event_carries_unified_payload() {
        let event = make_event(
            ReplicationAggregateKey {
                instance: "instance-a".to_string(),
                replication_name: "repl-1".to_string(),
            },
            new_aggregate(&observation(200, 5, 1234), Instant::now()),
        );

        assert_eq!(event.event_type, "replication_sync");
        assert_eq!(event.instance, "instance-a");
        assert_eq!(event.entry_name, "repl-1");
        assert_eq!(event.status, 200);
        assert_eq!(event.timestamp, 10);
        assert_eq!(event.payload["status"], 200);
        assert_eq!(event.payload["pending_records"], 3);
        assert_eq!(event.payload["written_records"], 5);
        assert_eq!(event.payload["failed_records"], 0);
        assert_eq!(event.payload["replicated_data_size"], 1234);
    }

    /// Schema invariant: a success and a failure event carry the identical
    /// field set (status, pending, written, failed, size, duration).
    #[rstest]
    fn success_and_failure_events_share_schema() {
        let success = make_event(
            ReplicationAggregateKey {
                instance: "instance-a".to_string(),
                replication_name: "repl-1".to_string(),
            },
            new_aggregate(&observation(200, 5, 100), Instant::now()),
        );
        let failure = make_event(
            ReplicationAggregateKey {
                instance: "instance-a".to_string(),
                replication_name: "repl-1".to_string(),
            },
            new_aggregate(&observation(404, 7, 0), Instant::now()),
        );

        let keys_of = |event: &SystemEvent| {
            let mut keys = event
                .payload
                .as_object()
                .unwrap()
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            keys.sort();
            keys
        };
        assert_eq!(keys_of(&success), keys_of(&failure));

        // success: written=N, failed=0; failure: failed=X, written=0
        assert_eq!(success.payload["written_records"], 5);
        assert_eq!(success.payload["failed_records"], 0);
        assert_eq!(failure.payload["written_records"], 0);
        assert_eq!(failure.payload["failed_records"], 7);
    }

    /// Status-change flush (condition 1): a 200 observation then a 404 one
    /// flushes exactly one 200 event with the accumulated written_records and
    /// opens a fresh 404 bucket.
    #[rstest]
    #[tokio::test]
    async fn status_change_flushes_previous_bucket(
        state: Arc<AsyncRwLock<ReplicationAggregatorState>>,
    ) {
        ReplicationEventAggregator::aggregate_event(&state, observation(200, 2, 20))
            .await
            .unwrap();
        let displaced =
            ReplicationEventAggregator::aggregate_event(&state, observation(200, 3, 30))
                .await
                .unwrap();
        assert!(displaced.is_empty(), "same status keeps accumulating");

        let displaced = ReplicationEventAggregator::aggregate_event(&state, observation(404, 4, 0))
            .await
            .unwrap();

        assert_eq!(displaced.len(), 1, "status change flushes the 200 bucket");
        let event = &displaced[0];
        assert_eq!(event.status, 200);
        assert_eq!(event.payload["written_records"], 5);
        assert_eq!(event.payload["failed_records"], 0);
        assert_eq!(event.payload["replicated_data_size"], 50);

        let guard = state.read().await.unwrap();
        assert_eq!(guard.aggregates.len(), 1);
        let aggregate = guard.aggregates.values().next().unwrap();
        assert_eq!(aggregate.status, 404);
        assert_eq!(aggregate.failed_records, 4);
        assert_eq!(aggregate.written_records, 0);
    }

    /// End-to-end 200 -> 404 example: the 404 event carries failed_records=X.
    #[rstest]
    #[tokio::test]
    async fn flush_all_emits_open_bucket(
        state: Arc<AsyncRwLock<ReplicationAggregatorState>>,
        flushed_events: Arc<Mutex<Vec<SystemEvent>>>,
    ) {
        let handler = make_test_handler(Arc::clone(&flushed_events));

        ReplicationEventAggregator::aggregate_event(&state, observation(404, 6, 0))
            .await
            .unwrap();
        ReplicationEventAggregator::flush_all(&state, &handler)
            .await
            .unwrap();

        let events = flushed_events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].status, 404);
        assert_eq!(events[0].payload["failed_records"], 6);
        assert_eq!(events[0].message, "boom");
        assert!(state.read().await.unwrap().aggregates.is_empty());
    }

    /// Counts: written/failed map correctly and pending is pulled from the
    /// latest observation; record_pass groups a mixed pass by status.
    #[rstest]
    #[tokio::test]
    async fn record_pass_groups_counter_by_status(flushed_events: Arc<Mutex<Vec<SystemEvent>>>) {
        let sink = SystemEventSink {
            system_logger: Arc::new(AsyncRwLock::new(Box::new(CapturingLogger {
                events: Arc::clone(&flushed_events),
                fail: false,
            })
                as Box<dyn crate::syslog::LogSystemEvent + Send + Sync>)),
            instance_name: "instance-a".to_string(),
        };
        let aggregator = ReplicationEventAggregator::new(sink, "repl-1".to_string());

        let counter = vec![
            (Ok(()), 2u64, 40u64),
            (Err(not_found!("missing")), 1u64, 0u64),
        ];
        aggregator.record_pass(7, 0.25, &counter).await;

        // allow the worker to drain the channel
        tokio::time::sleep(Duration::from_millis(50)).await;

        let guard = aggregator.state.read().await.unwrap();
        // 200 grouped, then 404 grouped -> status change flushed the 200 bucket
        assert_eq!(guard.aggregates.len(), 1);
        let aggregate = guard.aggregates.values().next().unwrap();
        assert_eq!(aggregate.status, 404);
        assert_eq!(aggregate.failed_records, 1);
        assert_eq!(aggregate.pending_records, 7);
        drop(guard);

        let events = flushed_events.lock().unwrap();
        assert_eq!(
            events.len(),
            1,
            "the 200 bucket was flushed on status change"
        );
        assert_eq!(events[0].status, 200);
        assert_eq!(events[0].payload["written_records"], 2);
        assert_eq!(events[0].payload["replicated_data_size"], 40);
    }

    /// Safety: a logger whose log_event returns Err must not break the
    /// aggregator; the error is swallowed and logged, never propagated.
    #[rstest]
    #[tokio::test]
    async fn emission_errors_are_swallowed(
        state: Arc<AsyncRwLock<ReplicationAggregatorState>>,
        flushed_events: Arc<Mutex<Vec<SystemEvent>>>,
    ) {
        let handler: SystemEventHandler = Arc::new(move |_event| {
            Box::pin(async move { Err(internal_server_error!("disk is on fire")) })
        });

        ReplicationEventAggregator::aggregate_event(&state, observation(200, 1, 10))
            .await
            .unwrap();

        // must not panic / propagate despite the failing handler
        ReplicationEventAggregator::flush_all(&state, &handler)
            .await
            .unwrap();

        assert!(flushed_events.lock().unwrap().is_empty());
        assert!(state.read().await.unwrap().aggregates.is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn next_deadline_returns_none_when_empty(
        state: Arc<AsyncRwLock<ReplicationAggregatorState>>,
    ) {
        assert!(ReplicationEventAggregator::next_deadline(&state)
            .await
            .is_none());
    }

    fn insert_aggregate(
        state: &ReplicationAggregatorState,
        flush_at: Instant,
        force_flush_at: Instant,
    ) -> ReplicationAggregatorState {
        let mut state = ReplicationAggregatorState {
            aggregates: state.aggregates.clone(),
        };
        state.aggregates.insert(
            ReplicationAggregateKey {
                instance: "instance-a".to_string(),
                replication_name: "repl-1".to_string(),
            },
            ReplicationAggregate {
                status: 200,
                first_timestamp: 1,
                last_timestamp: 1,
                pending_records: 0,
                written_records: 1,
                failed_records: 0,
                replicated_data_size: 10,
                duration: 0.1,
                message: String::new(),
                flush_at,
                force_flush_at,
            },
        );
        state
    }

    /// Idle flush (condition 2): a bucket whose sliding window expired is
    /// flushed even though the cap has not been reached.
    #[rstest]
    #[tokio::test]
    async fn flush_expired_flushes_idle_bucket(flushed_events: Arc<Mutex<Vec<SystemEvent>>>) {
        let now = Instant::now();
        let state = Arc::new(AsyncRwLock::new(insert_aggregate(
            &ReplicationAggregatorState::default(),
            now - Duration::from_millis(1),
            now + Duration::from_secs(60),
        )));
        let handler = make_test_handler(Arc::clone(&flushed_events));

        ReplicationEventAggregator::flush_expired(&state, &handler)
            .await
            .unwrap();

        assert_eq!(flushed_events.lock().unwrap().len(), 1);
        assert!(state.read().await.unwrap().aggregates.is_empty());
    }

    /// Cap flush (condition 3): a bucket whose force-flush deadline expired is
    /// flushed even though it is still receiving activity (window not idle).
    #[rstest]
    #[tokio::test]
    async fn flush_expired_flushes_capped_bucket(flushed_events: Arc<Mutex<Vec<SystemEvent>>>) {
        let now = Instant::now();
        let state = Arc::new(AsyncRwLock::new(insert_aggregate(
            &ReplicationAggregatorState::default(),
            now + Duration::from_secs(5),
            now - Duration::from_millis(1),
        )));
        let handler = make_test_handler(Arc::clone(&flushed_events));

        ReplicationEventAggregator::flush_expired(&state, &handler)
            .await
            .unwrap();

        assert_eq!(flushed_events.lock().unwrap().len(), 1);
        assert!(state.read().await.unwrap().aggregates.is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn flush_expired_keeps_active_bucket(flushed_events: Arc<Mutex<Vec<SystemEvent>>>) {
        let now = Instant::now();
        let state = Arc::new(AsyncRwLock::new(insert_aggregate(
            &ReplicationAggregatorState::default(),
            now + Duration::from_secs(5),
            now + Duration::from_secs(60),
        )));
        let handler = make_test_handler(Arc::clone(&flushed_events));

        ReplicationEventAggregator::flush_expired(&state, &handler)
            .await
            .unwrap();

        assert!(flushed_events.lock().unwrap().is_empty());
        assert_eq!(state.read().await.unwrap().aggregates.len(), 1);
    }

    /// Idle flush through the real worker (condition 2): after the aggregation
    /// window elapses with no further activity, the bucket is flushed.
    #[cfg(target_os = "linux")] // we need precise timing
    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn worker_flushes_after_idle_window(flushed_events: Arc<Mutex<Vec<SystemEvent>>>) {
        let sink = SystemEventSink {
            system_logger: Arc::new(AsyncRwLock::new(Box::new(CapturingLogger {
                events: Arc::clone(&flushed_events),
                fail: false,
            })
                as Box<dyn crate::syslog::LogSystemEvent + Send + Sync>)),
            instance_name: "instance-a".to_string(),
        };
        let aggregator = ReplicationEventAggregator::new(sink, "repl-1".to_string());

        aggregator
            .record_pass(0, 0.1, &[(Ok(()), 1u64, 10u64)])
            .await;

        // AGGREGATION_WINDOW_SECS is 1 in test builds; wait past it
        tokio::time::sleep(
            Duration::from_secs(AGGREGATION_WINDOW_SECS) + Duration::from_millis(500),
        )
        .await;

        let events = flushed_events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].status, 200);
        assert_eq!(events[0].payload["written_records"], 1);
    }

    /// Entry path (#7): events land at replications/<instance>/<replication-name>.
    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn writes_replication_event_to_system_bucket() {
        use crate::cfg::Cfg;
        use crate::storage::engine::StorageEngine;
        use crate::syslog::{build_replication_system_logger, SYSTEM_BUCKET_NAME};
        use reduct_base::io::ReadRecord;

        let tmp_dir = tempfile::tempdir().unwrap();
        let mut cfg = Cfg {
            data_path: tmp_dir.keep(),
            ..Cfg::default()
        };
        cfg.system_events_conf.enabled = true;
        cfg.instance_name = "instance-1".to_string();

        let storage = Arc::new(
            StorageEngine::builder()
                .with_data_path(cfg.data_path.clone())
                .with_cfg(cfg.clone())
                .build()
                .await,
        );

        let logger = build_replication_system_logger(&cfg, Arc::clone(&storage)).await;
        let sink = SystemEventSink {
            system_logger: Arc::new(AsyncRwLock::new(logger)),
            instance_name: "instance-1".to_string(),
        };
        let aggregator = ReplicationEventAggregator::new(sink, "repl-1".to_string());

        aggregator
            .record_pass(2, 0.5, &[(Ok(()), 3u64, 120u64)])
            .await;

        // drop the aggregator to close the channel and flush the open bucket
        drop(aggregator);

        // The worker creates the bucket, flushes and persists asynchronously, so
        // poll for the entry to appear rather than relying on a single fixed sleep
        // (disk I/O can be slow on some platforms, e.g. Windows CI).
        let entry_path = "replications/instance-1/repl-1";
        let mut found = None;
        for _ in 0..100 {
            if let Ok(weak_bucket) = storage.get_bucket(SYSTEM_BUCKET_NAME).await {
                let bucket = weak_bucket.upgrade_and_unwrap();
                if let Some(entry) = Arc::clone(&bucket)
                    .info()
                    .await
                    .unwrap()
                    .entries
                    .into_iter()
                    .find(|entry| entry.name == entry_path)
                {
                    found = Some((bucket, entry.latest_record));
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let (bucket, latest_record) = found.expect("replication diagnostics entry must exist");
        let mut reader = bucket.begin_read(entry_path, latest_record).await.unwrap();
        let record = reader.read_chunk().unwrap().unwrap();
        let event: serde_json::Value = serde_json::from_slice(&record).unwrap();

        assert_eq!(event["instance"], "instance-1");
        assert_eq!(event["status"], 200);
        assert_eq!(event["written_records"], 3);
        assert_eq!(event["replicated_data_size"], 120);
        assert_eq!(event["pending_records"], 2);
    }

    #[derive(Clone)]
    struct CapturingLogger {
        events: Arc<Mutex<Vec<SystemEvent>>>,
        fail: bool,
    }

    #[async_trait::async_trait]
    impl crate::syslog::LogSystemEvent for CapturingLogger {
        async fn log_event(&mut self, event: SystemEvent) -> Result<(), ReductError> {
            if self.fail {
                return Err(timeout!("nope"));
            }
            self.events.lock().unwrap().push(event);
            Ok(())
        }
    }
}
