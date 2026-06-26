// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

//! Captures the instance's own log messages into `$system/logs/<instance>/messages`.
//!
//! A [`reduct_base`] log sink (registered globally) snapshots every log record
//! and, when it is at or above the configured persist level, forwards it over a
//! bounded channel to a consumer task that writes it as a `log_message` system
//! event. The hook is non-blocking (`try_send`, drop-on-overflow) so logging
//! never blocks the server.

use crate::lifecycle::SystemEventSink;
use crate::syslog::log_event_payload::LogSystemEventPayload;
use crate::syslog::SystemEvent;
use log::{error, warn, Level};
use reduct_base::logger::{clear_log_sink, set_log_sink, LogSnapshot};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

const LOG_EVENT_TYPE: &str = "log_message";
const LOG_ENTRY_NAME: &str = "messages";
const LOG_CHANNEL_SIZE: usize = 1024;

tokio::task_local! {
    /// Set to `true` while the consumer is writing a captured record to storage.
    /// The sink checks it and skips re-capturing records emitted *during* that
    /// write, breaking the otherwise-infinite recursion (a storage write logs).
    /// It gates ONLY re-capture; console output already happened unconditionally
    /// upstream in `reduct_base`.
    static IN_CAPTURE_WRITE: bool;
}

fn level_code(level: Level) -> u16 {
    match level {
        Level::Error => 1,
        Level::Warn => 2,
        Level::Info => 3,
        Level::Debug => 4,
        Level::Trace => 5,
    }
}

fn make_event(instance: &str, snapshot: LogSnapshot) -> SystemEvent {
    let LogSnapshot {
        timestamp,
        level,
        target,
        file,
        line,
        message,
    } = snapshot;
    let payload = LogSystemEventPayload {
        level: level.to_string(),
        target,
        file,
        line,
    };
    SystemEvent {
        event_type: LOG_EVENT_TYPE.to_string(),
        timestamp,
        instance: instance.to_string(),
        entry_name: LOG_ENTRY_NAME.to_string(),
        status: level_code(level),
        message,
        payload: payload.to_value(),
    }
}

/// The non-blocking enqueue path used by the registered sink. Skips records
/// emitted during a capture write (reentrancy guard) and records below the
/// persist level, then `try_send`s; on a full channel it counts a drop and
/// returns immediately rather than blocking.
fn enqueue(
    tx: &mpsc::Sender<LogSnapshot>,
    persist_level: Level,
    dropped: &AtomicU64,
    snapshot: LogSnapshot,
) {
    // Reentrancy guard: never capture a log emitted while we are writing one.
    if IN_CAPTURE_WRITE.try_with(|active| *active).unwrap_or(false) {
        return;
    }
    // Persist-level filter: a less severe record (numerically greater `Level`)
    // is dropped before it reaches the channel.
    if snapshot.level > persist_level {
        return;
    }
    match tx.try_send(snapshot) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(_)) => {
            dropped.fetch_add(1, Ordering::Relaxed);
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {}
    }
}

/// Owns the log-capture consumer task and the global sink registration.
///
/// Mirrors [`UsageEventAggregator`](crate::storage::usage::UsageEventAggregator):
/// a bounded channel feeds a worker that drains it. [`stop`](Self::stop)
/// unregisters the sink and joins the worker.
pub(crate) struct LogCapture {
    // Dropping (or `take`-ing in `stop`) closes the channel, signalling the
    // worker to drain the backlog and exit.
    shutdown_tx: Option<mpsc::Sender<()>>,
    worker_handle: Option<JoinHandle<()>>,
    dropped: Arc<AtomicU64>,
}

impl LogCapture {
    pub(crate) fn new(sink: SystemEventSink, persist_level: Level) -> Self {
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
        let (data_tx, data_rx) = mpsc::channel(LOG_CHANNEL_SIZE);
        let dropped = Arc::new(AtomicU64::new(0));

        // `data_tx` is the only sender; it is moved into the global sink.
        let sink_dropped = Arc::clone(&dropped);
        set_log_sink(Arc::new(move |snapshot: LogSnapshot| {
            enqueue(&data_tx, persist_level, &sink_dropped, snapshot);
        }));

        let worker_handle = tokio::spawn(Self::run_worker(shutdown_rx, data_rx, sink));
        Self {
            shutdown_tx: Some(shutdown_tx),
            worker_handle: Some(worker_handle),
            dropped,
        }
    }

    /// Unregister the sink and stop the worker, draining any buffered records.
    /// Telemetry must never break shutdown, so a failed join is logged only.
    pub(crate) async fn stop(&mut self) {
        clear_log_sink();
        // Drop the sender so the worker observes shutdown and drains.
        self.shutdown_tx.take();
        if let Some(handle) = self.worker_handle.take() {
            if let Err(err) = handle.await {
                error!("Log capture task failed to join: {:?}", err);
            }
        }

        let dropped = self.dropped.load(Ordering::Relaxed);
        if dropped > 0 {
            warn!(
                "Log capture dropped {} message(s) because the buffer was full",
                dropped
            );
        }
    }

    async fn run_worker(
        mut shutdown_rx: mpsc::Receiver<()>,
        mut data_rx: mpsc::Receiver<LogSnapshot>,
        sink: SystemEventSink,
    ) {
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    // Shutdown signalled: drain whatever is already buffered,
                    // then stop.
                    while let Ok(snapshot) = data_rx.try_recv() {
                        Self::persist(&sink, snapshot).await;
                    }
                    break;
                }
                maybe_snapshot = data_rx.recv() => {
                    match maybe_snapshot {
                        Some(snapshot) => Self::persist(&sink, snapshot).await,
                        None => break,
                    }
                }
            }
        }
    }

    /// Write one captured record. The storage write is wrapped in the
    /// reentrancy guard so any log it emits is not re-captured; errors are
    /// swallowed and logged (telemetry must never break the server).
    async fn persist(sink: &SystemEventSink, snapshot: LogSnapshot) {
        let event = make_event(&sink.instance_name, snapshot);
        IN_CAPTURE_WRITE
            .scope(true, async {
                let mut logger = match sink.system_logger.write().await {
                    Ok(logger) => logger,
                    Err(err) => {
                        error!("Failed to lock system logger for log capture: {}", err);
                        return;
                    }
                };
                if let Err(err) = logger.log_event(event).await {
                    error!("Failed to persist log message: {}", err);
                }
            })
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::sync::AsyncRwLock;
    use crate::syslog::LogSystemEvent;
    use async_trait::async_trait;
    use reduct_base::error::ReductError;
    use std::sync::Mutex;
    use std::time::Duration;

    fn snapshot(level: Level, message: &str) -> LogSnapshot {
        LogSnapshot {
            timestamp: 1_000,
            level,
            target: "reductstore::test".to_string(),
            file: Some("syslog/log_capture.rs".to_string()),
            line: Some(42),
            message: message.to_string(),
        }
    }

    fn sink_with(logger: Box<dyn LogSystemEvent + Send + Sync>) -> SystemEventSink {
        SystemEventSink {
            system_logger: Arc::new(AsyncRwLock::new(logger)),
            instance_name: "instance-1".to_string(),
        }
    }

    /// A logger whose write path itself re-enters the enqueue path — exactly the
    /// recursion the guard must stop (a real storage write logs while writing).
    struct ReentrantLogger {
        events: Arc<Mutex<Vec<SystemEvent>>>,
        on_write: Arc<dyn Fn() + Send + Sync>,
    }

    #[async_trait]
    impl LogSystemEvent for ReentrantLogger {
        async fn log_event(&mut self, event: SystemEvent) -> Result<(), ReductError> {
            (self.on_write)();
            self.events.lock().unwrap().push(event);
            Ok(())
        }
    }

    #[test]
    fn make_event_maps_snapshot_fields() {
        let event = make_event("instance-1", snapshot(Level::Warn, "disk almost full"));
        assert_eq!(event.event_type, "log_message");
        assert_eq!(event.entry_name, "messages");
        assert_eq!(event.instance, "instance-1");
        assert_eq!(event.timestamp, 1_000);
        assert_eq!(event.status, 2); // WARN
        assert_eq!(event.message, "disk almost full");
        assert_eq!(event.payload["level"], "WARN");
        assert_eq!(event.payload["target"], "reductstore::test");
        assert_eq!(event.payload["file"], "syslog/log_capture.rs");
        assert_eq!(event.payload["line"], 42);
    }

    #[tokio::test]
    async fn enqueue_passes_records_at_or_above_persist_level() {
        let (tx, mut rx) = mpsc::channel(8);
        let dropped = AtomicU64::new(0);
        enqueue(&tx, Level::Warn, &dropped, snapshot(Level::Error, "boom"));
        enqueue(&tx, Level::Warn, &dropped, snapshot(Level::Warn, "warn"));
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_ok());
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn enqueue_drops_records_below_persist_level() {
        let (tx, mut rx) = mpsc::channel(8);
        let dropped = AtomicU64::new(0);
        enqueue(&tx, Level::Warn, &dropped, snapshot(Level::Info, "chatty"));
        assert!(rx.try_recv().is_err());
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn enqueue_drops_on_full_channel_and_counts() {
        let (tx, _rx) = mpsc::channel(1);
        let dropped = AtomicU64::new(0);
        // First call fills the single slot; the rest overflow and are counted.
        for _ in 0..5 {
            enqueue(&tx, Level::Trace, &dropped, snapshot(Level::Info, "x"));
        }
        assert_eq!(dropped.load(Ordering::Relaxed), 4);
    }

    /// The guard gates ONLY the enqueue (re-capture), nothing else.
    #[tokio::test]
    async fn guard_suppresses_reenqueue_during_capture_write() {
        let (tx, mut rx) = mpsc::channel(8);
        let dropped = AtomicU64::new(0);
        // Inside the capture-write scope, an enqueue is skipped...
        IN_CAPTURE_WRITE
            .scope(true, async {
                enqueue(
                    &tx,
                    Level::Trace,
                    &dropped,
                    snapshot(Level::Error, "during"),
                );
            })
            .await;
        assert!(
            rx.try_recv().is_err(),
            "enqueue during a capture write must be suppressed"
        );
        // ...but outside it, enqueue works normally.
        enqueue(&tx, Level::Trace, &dropped, snapshot(Level::Error, "after"));
        assert!(
            rx.try_recv().is_ok(),
            "enqueue outside a capture write must pass"
        );
    }

    /// KEYSTONE: the consumer must not loop when each write emits a log. The
    /// guard suppresses the re-capture, so exactly the seeded records are
    /// written and the worker terminates.
    #[tokio::test(flavor = "multi_thread")]
    async fn consumer_does_not_loop_when_write_emits_logs() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let (data_tx, data_rx) = mpsc::channel::<LogSnapshot>(1024);
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
        let dropped = Arc::new(AtomicU64::new(0));

        // On every write the logger re-enters the enqueue path (as a storage
        // write would by logging). Without the guard this loops forever.
        let reentry_tx = data_tx.clone();
        let reentry_dropped = Arc::clone(&dropped);
        let logger = ReentrantLogger {
            events: Arc::clone(&events),
            on_write: Arc::new(move || {
                enqueue(
                    &reentry_tx,
                    Level::Trace,
                    &reentry_dropped,
                    snapshot(Level::Error, "emitted-during-write"),
                );
            }),
        };
        let sink = sink_with(Box::new(logger));
        let worker = tokio::spawn(LogCapture::run_worker(shutdown_rx, data_rx, sink));

        // Seed three records from outside any write scope.
        for i in 0..3 {
            enqueue(
                &data_tx,
                Level::Trace,
                &dropped,
                snapshot(Level::Error, &format!("seed-{i}")),
            );
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
        drop(shutdown_tx);
        tokio::time::timeout(Duration::from_secs(3), worker)
            .await
            .expect("consumer must not loop/hang")
            .unwrap();

        assert_eq!(
            events.lock().unwrap().len(),
            3,
            "exactly the seeds are written — no re-capture amplification"
        );
    }

    /// A captured record lands at `$system/logs/<instance>/messages` with the
    /// expected flat schema and `status` (level code) label.
    #[tokio::test(flavor = "multi_thread")]
    async fn persist_writes_to_logs_entry_path() {
        use crate::cfg::Cfg;
        use crate::storage::engine::StorageEngine;
        use crate::syslog::{build_logs_system_logger, SYSTEM_BUCKET_NAME};
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

        let logger = build_logs_system_logger(&cfg, Arc::clone(&storage)).await;
        let sink = sink_with(logger);

        LogCapture::persist(&sink, snapshot(Level::Warn, "disk almost full")).await;

        let bucket = storage
            .get_bucket(SYSTEM_BUCKET_NAME)
            .await
            .unwrap()
            .upgrade_and_unwrap();
        let mut reader = bucket
            .begin_read("logs/instance-1/messages", 1_000)
            .await
            .unwrap();
        let record = reader.read_chunk().unwrap().unwrap();
        let event: serde_json::Value = serde_json::from_slice(&record).unwrap();

        assert_eq!(event["instance"], "instance-1");
        assert_eq!(event["status"], 2);
        assert_eq!(event["message"], "disk almost full");
        assert_eq!(event["level"], "WARN");
        assert_eq!(event["target"], "reductstore::test");
        assert_eq!(event["line"], 42);
    }
}
