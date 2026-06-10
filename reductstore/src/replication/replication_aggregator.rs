// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

//! Aggregates replication diagnostics into periodic `$system` events.
//!
//! One active "bucket" (running tally) is kept per replication task, keyed by
//! the status code. The bucket is flushed and a new one opened when ANY of:
//!   1. the status (the key) changes (e.g. 200 -> 404),
//!   2. no records were replicated for [`AGGREGATION_WINDOW_SECS`] (idle),
//!   3. the bucket accumulated [`FORCE_FLUSH_TIMEOUT_SECS`] of data (cap).
//!
//! Replication only runs on the primary node, so — unlike the API audit
//! aggregator, which forwards events off read-only replicas through a channel
//! and a background worker — this aggregator is owned and driven directly by
//! the single-threaded replication worker loop. It emits straight through the
//! [`SystemEventSink`], the same way the lifecycle module does.

use crate::api::audit::aggregator::{AGGREGATION_WINDOW_SECS, FORCE_FLUSH_TIMEOUT_SECS};
use crate::lifecycle::SystemEventSink;
use crate::replication::replication_event_payload::ReplicationSystemEventPayload;
use crate::syslog::SystemEvent;
use log::error;
use reduct_base::error::ReductError;
use std::collections::BTreeMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const REPLICATION_EVENT_TYPE: &str = "replication_sync";

/// The active aggregation bucket for a replication task, keyed by status code.
#[derive(Debug, Clone)]
struct ReplicationAggregate {
    status: u16,
    first_timestamp: u64,
    pending_records: u64,
    written_records: u64,
    failed_records: u64,
    replicated_data_size: u64,
    duration: f64,
    message: String,
    /// Sliding idle deadline (condition 2).
    flush_at: Instant,
    /// Hard time-cap deadline (condition 3).
    force_flush_at: Instant,
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

fn make_event(
    instance: &str,
    replication_name: &str,
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
        instance: instance.to_string(),
        entry_name: replication_name.to_string(),
        status: aggregate.status,
        message: aggregate.message,
        payload: payload.to_value(),
    }
}

/// Per-task replication diagnostics aggregator.
pub(super) struct ReplicationEventAggregator {
    sink: SystemEventSink,
    replication_name: String,
    active: Option<ReplicationAggregate>,
}

impl ReplicationEventAggregator {
    pub(super) fn new(sink: SystemEventSink, replication_name: String) -> Self {
        Self {
            sink,
            replication_name,
            active: None,
        }
    }

    /// Record one replication pass as a list of `(result, records, data_size)`.
    ///
    /// Telemetry must never break replication, so emission failures are
    /// swallowed and logged. The pass `duration` is attributed once, to the
    /// lowest-status observation, so it is never double-counted when a single
    /// pass produces several status codes.
    pub(super) async fn record_pass(
        &mut self,
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
            let pass_duration = if first { duration } else { 0.0 };
            first = false;

            // Condition 1: a status change flushes the current bucket.
            if self
                .active
                .as_ref()
                .is_some_and(|aggregate| aggregate.status != status)
            {
                self.flush().await;
            }

            let now = Instant::now();
            match &mut self.active {
                Some(aggregate) => {
                    if is_success(status) {
                        aggregate.written_records += records;
                        aggregate.replicated_data_size += data_size;
                    } else {
                        aggregate.failed_records += records;
                    }
                    aggregate.pending_records = pending_records;
                    aggregate.duration += pass_duration;
                    if !message.is_empty() {
                        aggregate.message = message;
                    }
                    aggregate.flush_at = now + Duration::from_secs(AGGREGATION_WINDOW_SECS);
                }
                None => {
                    let success = is_success(status);
                    self.active = Some(ReplicationAggregate {
                        status,
                        first_timestamp: timestamp,
                        pending_records,
                        written_records: if success { records } else { 0 },
                        failed_records: if success { 0 } else { records },
                        replicated_data_size: data_size,
                        duration: pass_duration,
                        message,
                        flush_at: now + Duration::from_secs(AGGREGATION_WINDOW_SECS),
                        force_flush_at: now + Duration::from_secs(FORCE_FLUSH_TIMEOUT_SECS),
                    });
                }
            }
        }

        // Condition 3: the bucket may have hit its time cap.
        self.flush_if_due().await;
    }

    /// Flush the open bucket if its idle window elapsed (condition 2) or it hit
    /// the time cap (condition 3). Called every worker iteration so an idle
    /// bucket is flushed even when no new records arrive.
    pub(super) async fn flush_if_due(&mut self) {
        let now = Instant::now();
        if self
            .active
            .as_ref()
            .is_some_and(|aggregate| now >= aggregate.flush_at || now >= aggregate.force_flush_at)
        {
            self.flush().await;
        }
    }

    /// Emit the open bucket (if any) as a `$system` event and clear it.
    pub(super) async fn flush(&mut self) {
        let Some(aggregate) = self.active.take() else {
            return;
        };
        let event = make_event(&self.sink.instance_name, &self.replication_name, aggregate);

        match self.sink.system_logger.write().await {
            Ok(mut logger) => {
                if let Err(err) = logger.log_event(event).await {
                    error!(
                        "Failed to persist replication diagnostics for '{}': {}",
                        self.replication_name, err
                    );
                }
            }
            Err(err) => error!(
                "Failed to lock system logger for replication '{}': {}",
                self.replication_name, err
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::sync::AsyncRwLock;
    use crate::syslog::LogSystemEvent;
    use reduct_base::{not_found, timeout};
    use rstest::{fixture, rstest};
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct CapturingLogger {
        events: Arc<Mutex<Vec<SystemEvent>>>,
        fail: bool,
    }

    #[async_trait::async_trait]
    impl LogSystemEvent for CapturingLogger {
        async fn log_event(&mut self, event: SystemEvent) -> Result<(), ReductError> {
            if self.fail {
                return Err(timeout!("sink is down"));
            }
            self.events.lock().unwrap().push(event);
            Ok(())
        }
    }

    fn aggregator(events: Arc<Mutex<Vec<SystemEvent>>>, fail: bool) -> ReplicationEventAggregator {
        let sink = SystemEventSink {
            system_logger: Arc::new(AsyncRwLock::new(
                Box::new(CapturingLogger { events, fail }) as Box<dyn LogSystemEvent + Send + Sync>
            )),
            instance_name: "instance-1".to_string(),
        };
        ReplicationEventAggregator::new(sink, "repl-1".to_string())
    }

    #[fixture]
    fn events() -> Arc<Mutex<Vec<SystemEvent>>> {
        Arc::new(Mutex::new(Vec::new()))
    }

    #[rstest]
    #[tokio::test]
    async fn records_and_flushes_success(events: Arc<Mutex<Vec<SystemEvent>>>) {
        let mut agg = aggregator(Arc::clone(&events), false);
        agg.record_pass(3, 0.5, &[(Ok(()), 5, 1234)]).await;
        agg.flush().await;

        let captured = events.lock().unwrap();
        assert_eq!(captured.len(), 1);
        let event = &captured[0];
        assert_eq!(event.event_type, "replication_sync");
        assert_eq!(event.instance, "instance-1");
        assert_eq!(event.entry_name, "repl-1");
        assert_eq!(event.status, 200);
        assert_eq!(event.payload["pending_records"], 3);
        assert_eq!(event.payload["written_records"], 5);
        assert_eq!(event.payload["failed_records"], 0);
        assert_eq!(event.payload["replicated_data_size"], 1234);
    }

    /// Status-change flush (condition 1): accumulate at 200, then a 404 record
    /// flushes one 200 event with the accumulated written_records and opens a
    /// fresh 404 bucket whose flush carries failed_records=X.
    #[rstest]
    #[tokio::test]
    async fn status_change_flushes_previous_bucket(events: Arc<Mutex<Vec<SystemEvent>>>) {
        let mut agg = aggregator(Arc::clone(&events), false);
        agg.record_pass(0, 0.1, &[(Ok(()), 2, 20)]).await;
        agg.record_pass(0, 0.1, &[(Ok(()), 3, 30)]).await;
        assert!(events.lock().unwrap().is_empty(), "same status accumulates");

        agg.record_pass(7, 0.1, &[(Err(not_found!("missing")), 4, 0)])
            .await;

        {
            let captured = events.lock().unwrap();
            assert_eq!(captured.len(), 1, "status change flushed the 200 bucket");
            assert_eq!(captured[0].status, 200);
            assert_eq!(captured[0].payload["written_records"], 5);
            assert_eq!(captured[0].payload["replicated_data_size"], 50);
        }

        agg.flush().await;
        let captured = events.lock().unwrap();
        assert_eq!(captured.len(), 2);
        assert_eq!(captured[1].status, 404);
        assert_eq!(captured[1].payload["failed_records"], 4);
        assert_eq!(captured[1].payload["written_records"], 0);
        assert_eq!(captured[1].message, "missing");
    }

    /// Schema invariant (#4): success and failure events carry the identical
    /// field set.
    #[rstest]
    #[tokio::test]
    async fn success_and_failure_events_share_schema(events: Arc<Mutex<Vec<SystemEvent>>>) {
        let mut agg = aggregator(Arc::clone(&events), false);
        agg.record_pass(0, 0.1, &[(Ok(()), 5, 100)]).await;
        agg.flush().await;
        agg.record_pass(0, 0.1, &[(Err(not_found!("x")), 7, 0)])
            .await;
        agg.flush().await;

        let captured = events.lock().unwrap();
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
        assert_eq!(keys_of(&captured[0]), keys_of(&captured[1]));
        assert_eq!(captured[0].payload["written_records"], 5);
        assert_eq!(captured[0].payload["failed_records"], 0);
        assert_eq!(captured[1].payload["written_records"], 0);
        assert_eq!(captured[1].payload["failed_records"], 7);
    }

    /// Idle flush (condition 2): once the sliding window elapses, flush_if_due
    /// emits the bucket without any new activity.
    #[rstest]
    #[tokio::test]
    async fn idle_window_flushes(events: Arc<Mutex<Vec<SystemEvent>>>) {
        let mut agg = aggregator(Arc::clone(&events), false);
        agg.record_pass(0, 0.1, &[(Ok(()), 1, 10)]).await;
        // age the idle deadline into the past
        agg.active.as_mut().unwrap().flush_at = Instant::now() - Duration::from_millis(1);

        agg.flush_if_due().await;
        assert_eq!(events.lock().unwrap().len(), 1);
        assert!(agg.active.is_none());
    }

    /// Cap flush (condition 3): once the force-flush deadline passes, the bucket
    /// is flushed even though the idle window has not elapsed.
    #[rstest]
    #[tokio::test]
    async fn time_cap_flushes(events: Arc<Mutex<Vec<SystemEvent>>>) {
        let mut agg = aggregator(Arc::clone(&events), false);
        agg.record_pass(0, 0.1, &[(Ok(()), 1, 10)]).await;
        let now = Instant::now();
        let aggregate = agg.active.as_mut().unwrap();
        aggregate.flush_at = now + Duration::from_secs(5); // not idle
        aggregate.force_flush_at = now - Duration::from_millis(1); // capped

        agg.flush_if_due().await;
        assert_eq!(events.lock().unwrap().len(), 1);
        assert!(agg.active.is_none());
    }

    #[rstest]
    #[tokio::test]
    async fn flush_if_due_keeps_fresh_bucket(events: Arc<Mutex<Vec<SystemEvent>>>) {
        let mut agg = aggregator(Arc::clone(&events), false);
        agg.record_pass(0, 0.1, &[(Ok(()), 1, 10)]).await;

        agg.flush_if_due().await;
        assert!(events.lock().unwrap().is_empty());
        assert!(agg.active.is_some());
    }

    /// Safety (#6): a logger that always errors must not break the aggregator;
    /// the error is swallowed, the bucket is still cleared.
    #[rstest]
    #[tokio::test]
    async fn emission_errors_are_swallowed(events: Arc<Mutex<Vec<SystemEvent>>>) {
        let mut agg = aggregator(Arc::clone(&events), true);
        agg.record_pass(0, 0.1, &[(Ok(()), 1, 10)]).await;
        agg.flush().await; // must not panic / propagate

        assert!(events.lock().unwrap().is_empty());
        assert!(agg.active.is_none());
    }

    #[rstest]
    #[tokio::test]
    async fn empty_pass_is_ignored(events: Arc<Mutex<Vec<SystemEvent>>>) {
        let mut agg = aggregator(Arc::clone(&events), false);
        agg.record_pass(0, 0.1, &[]).await;
        assert!(agg.active.is_none());
        agg.flush().await;
        assert!(events.lock().unwrap().is_empty());
    }

    /// Entry path (#7): events land at replications/<instance>/<name>. With the
    /// inline aggregator, flush() persists synchronously, so the entry is
    /// readable immediately after — no polling needed.
    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn writes_event_to_replications_entry_path() {
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
        let mut agg = ReplicationEventAggregator::new(sink, "repl-1".to_string());

        agg.record_pass(2, 0.5, &[(Ok(()), 3, 120)]).await;
        agg.flush().await;

        let bucket = storage
            .get_bucket(SYSTEM_BUCKET_NAME)
            .await
            .unwrap()
            .upgrade_and_unwrap();
        let entry_path = "replications/instance-1/repl-1";
        let latest_record = Arc::clone(&bucket)
            .info()
            .await
            .unwrap()
            .entries
            .into_iter()
            .find(|entry| entry.name == entry_path)
            .expect("replication diagnostics entry must exist")
            .latest_record;
        let mut reader = bucket.begin_read(entry_path, latest_record).await.unwrap();
        let record = reader.read_chunk().unwrap().unwrap();
        let event: serde_json::Value = serde_json::from_slice(&record).unwrap();

        assert_eq!(event["instance"], "instance-1");
        assert_eq!(event["status"], 200);
        assert_eq!(event["written_records"], 3);
        assert_eq!(event["replicated_data_size"], 120);
        assert_eq!(event["pending_records"], 2);
    }
}
