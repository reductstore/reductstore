// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

//! Periodic task that emits usage statistics as `$system` events.

use crate::lifecycle::SystemEventSink;
use crate::storage::engine::StorageEngine;
use crate::syslog::SystemEvent;
use crate::usage::usage_event_payload::UsageSystemEventPayload;
use crate::usage::{DrainedUsageCounters, UsageCounters, UsageSnapshot};
use log::error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;

const USAGE_EVENT_TYPE: &str = "usage_stats";
/// Hard-coded flush interval; the `duration` payload field carries the
/// measured elapsed time, so timer drift does not skew the statistics.
const USAGE_FLUSH_INTERVAL: Duration = Duration::from_secs(60);
/// Entry name for instance-wide totals. Per-bucket events are a future
/// extension that would emit additional events with other entry names.
const USAGE_TOTAL_ENTRY_NAME: &str = "total";

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

fn make_event(
    instance: &str,
    entry_name: &str,
    duration: f64,
    counters: DrainedUsageCounters,
    snapshot: UsageSnapshot,
) -> SystemEvent {
    let payload = UsageSystemEventPayload {
        duration,
        write_bytes: counters.write_bytes,
        read_bytes: counters.read_bytes,
        records_written: counters.records_written,
        records_read: counters.records_read,
        storage_bytes: snapshot.storage_bytes,
        bucket_count: snapshot.bucket_count,
        entry_count: snapshot.entry_count,
        block_count: snapshot.block_count,
    };

    SystemEvent {
        event_type: USAGE_EVENT_TYPE.to_string(),
        timestamp: now_micros(),
        instance: instance.to_string(),
        entry_name: entry_name.to_string(),
        status: 200,
        message: String::new(),
        payload: payload.to_value(),
    }
}

/// Background task that flushes usage statistics every
/// [`USAGE_FLUSH_INTERVAL`] and once more on shutdown.
pub(crate) struct UsageStatsTask {
    stop_flag: Arc<AtomicBool>,
    worker_handle: Option<JoinHandle<()>>,
}

impl UsageStatsTask {
    pub(crate) fn spawn(
        sink: SystemEventSink,
        storage: Arc<StorageEngine>,
        counters: Arc<UsageCounters>,
    ) -> Self {
        let stop_flag = Arc::new(AtomicBool::new(false));
        let thr_stop_flag = Arc::clone(&stop_flag);

        let handle = tokio::spawn(async move {
            let mut last_flush = Instant::now();
            loop {
                Self::sleep_with_stop(&thr_stop_flag, USAGE_FLUSH_INTERVAL).await;
                Self::flush(&sink, &storage, &counters, &mut last_flush).await;
                if thr_stop_flag.load(Ordering::Relaxed) {
                    break;
                }
            }
        });

        Self {
            stop_flag,
            worker_handle: Some(handle),
        }
    }

    /// Stop the worker; the partial interval is flushed before it exits.
    pub(crate) async fn stop(&mut self) {
        self.stop_flag.store(true, Ordering::Relaxed);
        if let Some(handle) = self.worker_handle.take() {
            if let Err(err) = handle.await {
                error!("Usage statistics task failed to join: {:?}", err);
            }
        }
    }

    /// Emit one usage event. Telemetry must never break anything, so all
    /// errors are swallowed and logged. The counters are drained with a swap,
    /// so traffic counted during the flush rolls into the next interval.
    async fn flush(
        sink: &SystemEventSink,
        storage: &Arc<StorageEngine>,
        counters: &Arc<UsageCounters>,
        last_flush: &mut Instant,
    ) {
        let duration = last_flush.elapsed().as_secs_f64();
        *last_flush = Instant::now();
        let drained = counters.drain();

        let snapshot = match storage.usage_snapshot().await {
            Ok(snapshot) => snapshot,
            Err(err) => {
                error!("Failed to collect usage snapshot: {}", err);
                return;
            }
        };

        let event = make_event(
            &sink.instance_name,
            USAGE_TOTAL_ENTRY_NAME,
            duration,
            drained,
            snapshot,
        );
        match sink.system_logger.write().await {
            Ok(mut logger) => {
                if let Err(err) = logger.log_event(event).await {
                    error!("Failed to persist usage statistics: {}", err);
                }
            }
            Err(err) => error!("Failed to lock system logger for usage statistics: {}", err),
        }
    }

    async fn sleep_with_stop(stop_flag: &Arc<AtomicBool>, duration: Duration) {
        const SLICE: Duration = Duration::from_millis(50);
        let mut remaining = duration;
        while remaining > Duration::ZERO && !stop_flag.load(Ordering::Relaxed) {
            let step = remaining.min(SLICE);
            tokio::time::sleep(step).await;
            remaining = remaining.saturating_sub(step);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::Cfg;
    use crate::core::sync::AsyncRwLock;
    use crate::syslog::LogSystemEvent;
    use bytes::Bytes;
    use reduct_base::error::ReductError;
    use reduct_base::msg::bucket_api::BucketSettings;
    use reduct_base::{timeout, Labels};
    use rstest::{fixture, rstest};
    use std::sync::Mutex;

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

    fn capturing_sink(events: Arc<Mutex<Vec<SystemEvent>>>, fail: bool) -> SystemEventSink {
        SystemEventSink {
            system_logger: Arc::new(AsyncRwLock::new(
                Box::new(CapturingLogger { events, fail }) as Box<dyn LogSystemEvent + Send + Sync>
            )),
            instance_name: "instance-1".to_string(),
        }
    }

    #[fixture]
    fn events() -> Arc<Mutex<Vec<SystemEvent>>> {
        Arc::new(Mutex::new(Vec::new()))
    }

    #[fixture]
    async fn storage() -> Arc<StorageEngine> {
        let cfg = Cfg {
            data_path: tempfile::tempdir().unwrap().keep(),
            ..Cfg::default()
        };
        Arc::new(
            StorageEngine::builder()
                .with_data_path(cfg.data_path.clone())
                .with_cfg(cfg)
                .build()
                .await,
        )
    }

    /// Write two 10-byte records and read one back through the engine choke
    /// points, so the shared counters in the engine's `Cfg` are populated.
    async fn generate_traffic(storage: &Arc<StorageEngine>) {
        storage
            .create_bucket("bucket-1", BucketSettings::default())
            .await
            .unwrap();
        for time in [1, 2] {
            let mut writer = storage
                .begin_write(
                    "bucket-1",
                    "entry-1",
                    time,
                    10,
                    "text/plain".to_string(),
                    Labels::new(),
                )
                .await
                .unwrap();
            writer
                .send(Ok(Some(Bytes::from("0123456789"))))
                .await
                .unwrap();
            writer.send(Ok(None)).await.unwrap();
        }

        let _reader = storage
            .get_bucket("bucket-1")
            .await
            .unwrap()
            .upgrade_and_unwrap()
            .begin_read("entry-1", 1)
            .await
            .unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn flush_emits_event_with_all_nine_payload_fields(
        events: Arc<Mutex<Vec<SystemEvent>>>,
        #[future] storage: Arc<StorageEngine>,
    ) {
        let storage = storage.await;
        generate_traffic(&storage).await;

        let sink = capturing_sink(Arc::clone(&events), false);
        let counters = Arc::clone(&storage.cfg().usage_counters);
        let mut last_flush = Instant::now();
        UsageStatsTask::flush(&sink, &storage, &counters, &mut last_flush).await;

        let captured = events.lock().unwrap();
        assert_eq!(captured.len(), 1);
        let event = &captured[0];
        assert_eq!(event.event_type, "usage_stats");
        assert_eq!(event.instance, "instance-1");
        assert_eq!(event.entry_name, "total");
        assert_eq!(event.status, 200);

        let payload = event.payload.as_object().unwrap();
        let mut keys = payload.keys().cloned().collect::<Vec<_>>();
        keys.sort();
        assert_eq!(
            keys,
            vec![
                "block_count",
                "bucket_count",
                "duration",
                "entry_count",
                "read_bytes",
                "records_read",
                "records_written",
                "storage_bytes",
                "write_bytes",
            ]
        );
        assert!(event.payload["duration"].as_f64().unwrap() >= 0.0);
        assert_eq!(event.payload["write_bytes"], 20);
        assert_eq!(event.payload["read_bytes"], 10);
        assert_eq!(event.payload["records_written"], 2);
        assert_eq!(event.payload["records_read"], 1);
        assert!(event.payload["storage_bytes"].as_u64().unwrap() > 0);
        assert_eq!(event.payload["bucket_count"], 1);
        assert_eq!(event.payload["entry_count"], 1);
        assert_eq!(event.payload["block_count"], 1);
    }

    #[rstest]
    #[tokio::test]
    async fn flush_drains_counters_between_intervals(
        events: Arc<Mutex<Vec<SystemEvent>>>,
        #[future] storage: Arc<StorageEngine>,
    ) {
        let storage = storage.await;
        generate_traffic(&storage).await;

        let sink = capturing_sink(Arc::clone(&events), false);
        let counters = Arc::clone(&storage.cfg().usage_counters);
        let mut last_flush = Instant::now();
        UsageStatsTask::flush(&sink, &storage, &counters, &mut last_flush).await;
        UsageStatsTask::flush(&sink, &storage, &counters, &mut last_flush).await;

        let captured = events.lock().unwrap();
        assert_eq!(captured.len(), 2);
        let second = &captured[1];
        assert_eq!(second.payload["write_bytes"], 0);
        assert_eq!(second.payload["records_written"], 0);
        assert_eq!(
            second.payload["bucket_count"], 1,
            "snapshot fields must still be populated"
        );
    }

    /// Emission failures are swallowed; the next flush still works.
    #[rstest]
    #[tokio::test]
    async fn emission_failure_does_not_break_flushing(
        events: Arc<Mutex<Vec<SystemEvent>>>,
        #[future] storage: Arc<StorageEngine>,
    ) {
        let storage = storage.await;
        let counters = Arc::clone(&storage.cfg().usage_counters);
        let mut last_flush = Instant::now();

        let failing_sink = capturing_sink(Arc::clone(&events), true);
        UsageStatsTask::flush(&failing_sink, &storage, &counters, &mut last_flush).await;
        assert!(events.lock().unwrap().is_empty());

        let working_sink = capturing_sink(Arc::clone(&events), false);
        UsageStatsTask::flush(&working_sink, &storage, &counters, &mut last_flush).await;
        assert_eq!(events.lock().unwrap().len(), 1);
    }

    /// Shutdown flush: stopping the task interrupts the 60s sleep and emits
    /// the partial interval exactly once.
    #[rstest]
    #[tokio::test]
    async fn stop_flushes_partial_interval(
        events: Arc<Mutex<Vec<SystemEvent>>>,
        #[future] storage: Arc<StorageEngine>,
    ) {
        let storage = storage.await;
        let counters = Arc::clone(&storage.cfg().usage_counters);

        let mut task = UsageStatsTask::spawn(
            capturing_sink(Arc::clone(&events), false),
            Arc::clone(&storage),
            counters,
        );
        task.stop().await;

        let captured = events.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].event_type, "usage_stats");
        assert_eq!(captured[0].payload["bucket_count"], 0);
    }

    /// Entry path: events land at `$system/usage/<instance>/total` as flat
    /// JSON with the 9 payload fields.
    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn writes_event_to_usage_entry_path() {
        use crate::syslog::{build_usage_system_logger, SYSTEM_BUCKET_NAME};
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
        generate_traffic(&storage).await;

        let logger = build_usage_system_logger(&cfg, Arc::clone(&storage)).await;
        let sink = SystemEventSink {
            system_logger: Arc::new(AsyncRwLock::new(logger)),
            instance_name: "instance-1".to_string(),
        };
        let counters = Arc::clone(&cfg.usage_counters);
        let mut last_flush = Instant::now();
        UsageStatsTask::flush(&sink, &storage, &counters, &mut last_flush).await;

        let bucket = storage
            .get_bucket(SYSTEM_BUCKET_NAME)
            .await
            .unwrap()
            .upgrade_and_unwrap();
        let entry_path = "usage/instance-1/total";
        let latest_record = Arc::clone(&bucket)
            .info()
            .await
            .unwrap()
            .entries
            .into_iter()
            .find(|entry| entry.name == entry_path)
            .expect("usage entry must exist")
            .latest_record;
        let mut reader = bucket.begin_read(entry_path, latest_record).await.unwrap();
        let record = reader.read_chunk().unwrap().unwrap();
        let event: serde_json::Value = serde_json::from_slice(&record).unwrap();

        assert_eq!(event["instance"], "instance-1");
        assert_eq!(event["status"], 200);
        assert_eq!(event["write_bytes"], 20);
        assert_eq!(event["read_bytes"], 10);
        assert_eq!(event["records_written"], 2);
        assert_eq!(event["records_read"], 1);
        assert!(event["storage_bytes"].as_u64().unwrap() > 0);
        assert_eq!(event["bucket_count"], 1);
        assert_eq!(event["entry_count"], 1);
        assert_eq!(event["block_count"], 1);
        assert!(event["duration"].as_f64().unwrap() >= 0.0);
    }
}
