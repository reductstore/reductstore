// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

pub(crate) mod aggregate;
pub(crate) mod payload;

mod capture;
mod event;
mod forward_writer;
mod local_writer;
mod path;
mod sink;
mod system_event_logger;

use crate::cfg::Cfg;
use crate::core::sync::AsyncRwLock;
use crate::storage::engine::StorageEngine;
use crate::storage::usage::UsageCounters;
use async_trait::async_trait;
use reduct_base::error::ReductError;
use reduct_base::msg::bucket_api::{BucketSettings, QuotaType};
use std::sync::Arc;

pub(crate) use aggregate::usage::UsageEventAggregator;
pub(crate) use event::{SystemEvent, SystemEventKind};
pub(crate) use sink::{BoxedSystemLogger, LogSystemEvent, SystemEventSink};
use system_event_logger::SystemEventLoggerBuilder;

pub(crate) const AUDIT_BUCKET_NAME: &str = "$audit";
pub(crate) const SYSTEM_BUCKET_NAME: &str = "$system";
pub(crate) const SYSTEM_AUDIT_ENTRY_PREFIX: &str = "audit";
pub(crate) const SYSTEM_LIFECYCLE_ENTRY_PREFIX: &str = "lifecycle";
pub(crate) const SYSTEM_REPLICATION_ENTRY_PREFIX: &str = "replications";
pub(crate) const SYSTEM_USAGE_ENTRY_PREFIX: &str = "usage";
pub(crate) const SYSTEM_LOGS_ENTRY_PREFIX: &str = "logs";

pub(crate) use capture::logs::LogCapture;

pub(crate) async fn build_audit_logger(
    cfg: &Cfg,
    storage: Arc<StorageEngine>,
) -> BoxedSystemLogger {
    if !cfg.system_events_conf.enabled {
        Box::new(DisabledAuditLogger)
    } else {
        let inner = SystemEventLoggerBuilder::new(SYSTEM_BUCKET_NAME, system_bucket_settings(cfg))
            .build(cfg, storage)
            .expect("audit system logger must build");
        // The aggregation machinery (batching worker + flush handler) lives in
        // `aggregate::audit`; here we only build the inner writer and wrap it.
        aggregate::audit::aggregated_audit_logger(inner)
    }
}

pub(crate) async fn build_system_logger(
    cfg: &Cfg,
    storage: Arc<StorageEngine>,
) -> BoxedSystemLogger {
    if !cfg.system_events_conf.enabled {
        return Box::new(DisabledSystemLogger);
    }

    SystemEventLoggerBuilder::new(SYSTEM_BUCKET_NAME, system_bucket_settings(cfg))
        .build(cfg, storage)
        .expect("system logger must build")
}

pub(crate) async fn build_replication_system_logger(
    cfg: &Cfg,
    storage: Arc<StorageEngine>,
) -> BoxedSystemLogger {
    if !cfg.system_events_conf.enabled {
        return Box::new(DisabledSystemLogger);
    }

    SystemEventLoggerBuilder::new(SYSTEM_BUCKET_NAME, system_bucket_settings(cfg))
        .build(cfg, storage)
        .expect("replication system logger must build")
}

pub(crate) async fn build_usage_system_logger(
    cfg: &Cfg,
    storage: Arc<StorageEngine>,
) -> BoxedSystemLogger {
    if !cfg.system_events_conf.enabled {
        return Box::new(DisabledSystemLogger);
    }

    SystemEventLoggerBuilder::new(SYSTEM_BUCKET_NAME, system_bucket_settings(cfg))
        .build(cfg, storage)
        .expect("usage system logger must build")
}

/// Build the usage statistics aggregator: it owns the periodic task that drains
/// the shared traffic `counters` (incremented by the storage engine) and writes
/// usage events under `usage/<instance>/total`. Returns `None` when system
/// events are disabled. The inner `$system` writer is built like the other
/// loggers; the aggregator wraps it with the timer and snapshot logic.
pub(crate) async fn build_usage_logger(
    cfg: &Cfg,
    storage: Arc<StorageEngine>,
    counters: Arc<UsageCounters>,
) -> Option<UsageEventAggregator> {
    if !cfg.system_events_conf.enabled {
        return None;
    }

    let inner = build_usage_system_logger(cfg, Arc::clone(&storage)).await;
    let sink = SystemEventSink {
        system_logger: Arc::new(AsyncRwLock::new(inner)),
        instance_name: cfg.instance_name.clone(),
    };
    Some(UsageEventAggregator::new(sink, storage, counters))
}

pub(crate) async fn build_logs_system_logger(
    cfg: &Cfg,
    storage: Arc<StorageEngine>,
) -> BoxedSystemLogger {
    if !cfg.system_events_conf.enabled {
        return Box::new(DisabledSystemLogger);
    }

    SystemEventLoggerBuilder::new(SYSTEM_BUCKET_NAME, system_bucket_settings(cfg))
        .build(cfg, storage)
        .expect("logs system logger must build")
}

/// Build the log-capture component: it registers a global log sink and owns the
/// consumer task that writes captured records under `logs/<instance>/messages`.
/// Returns `None` when system events are disabled, no persist level is
/// configured, or the instance is a replica (logs are node-local to avoid the
/// replica forward-failure loop).
pub(crate) async fn build_log_capture(
    cfg: &Cfg,
    storage: Arc<StorageEngine>,
) -> Option<LogCapture> {
    if !cfg.system_events_conf.enabled {
        return None;
    }
    let persist_level = cfg.system_events_conf.log_level?;
    if cfg.role == crate::cfg::InstanceRole::Replica {
        return None;
    }

    let inner = build_logs_system_logger(cfg, Arc::clone(&storage)).await;
    let sink = SystemEventSink {
        system_logger: Arc::new(AsyncRwLock::new(inner)),
        instance_name: cfg.instance_name.clone(),
    };
    Some(LogCapture::new(sink, persist_level, &cfg.log_level))
}

fn system_bucket_settings(cfg: &Cfg) -> BucketSettings {
    if let Some(quota_size) = cfg.system_events_conf.quota_size {
        BucketSettings {
            quota_type: Some(QuotaType::FIFO),
            quota_size: Some(quota_size),
            ..BucketSettings::default()
        }
    } else {
        BucketSettings::default()
    }
}

struct DisabledAuditLogger;

#[async_trait]
impl LogSystemEvent for DisabledAuditLogger {
    async fn log_event(&mut self, _event: SystemEvent) -> Result<(), ReductError> {
        Ok(())
    }
}

struct DisabledSystemLogger;

#[async_trait]
impl LogSystemEvent for DisabledSystemLogger {
    async fn log_event(&mut self, _event: SystemEvent) -> Result<(), ReductError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::Cfg;
    use crate::syslog::aggregate::AGGREGATION_WINDOW_SECS;
    use reduct_base::io::ReadRecord;
    use rstest::{fixture, rstest};
    use serde_json::Value;
    use tempfile::tempdir;
    use tokio::time::{sleep, Duration};

    #[fixture]
    async fn storage_and_cfg() -> (Arc<StorageEngine>, Cfg) {
        let tmp_dir = tempdir().unwrap();
        let mut cfg = Cfg {
            data_path: tmp_dir.keep(),
            ..Cfg::default()
        };
        cfg.system_events_conf.enabled = true;
        let storage = Arc::new(
            StorageEngine::builder()
                .with_data_path(cfg.data_path.clone())
                .with_cfg(cfg.clone())
                .build()
                .await,
        );
        (storage, cfg)
    }

    fn make_event() -> SystemEvent {
        SystemEvent {
            kind: SystemEventKind::Audit,
            event_type: "api_call".to_string(),
            timestamp: 1,
            instance: "test-instance".to_string(),
            entry_name: "token-1".to_string(),
            status: 200,
            message: "".to_string(),
            payload: serde_json::json!({
                "token_name": "token-1",
                "method": "GET",
                "path": "/api/v1/info",
                "client_ip": null,
                "call_count": 1,
                "duration": 0.1
            }),
        }
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn build_uses_local_repository_for_non_replica(
        #[future] storage_and_cfg: (Arc<StorageEngine>, Cfg),
    ) {
        let (storage, cfg) = storage_and_cfg.await;
        let mut repo = build_audit_logger(&cfg, Arc::clone(&storage)).await;

        repo.log_event(make_event()).await.unwrap();
        sleep(Duration::from_secs(AGGREGATION_WINDOW_SECS * 2)).await;

        let bucket = storage
            .get_bucket(SYSTEM_BUCKET_NAME)
            .await
            .unwrap()
            .upgrade_and_unwrap();
        let mut reader = bucket
            .begin_read("audit/test-instance/token-1", 1)
            .await
            .unwrap();
        let record = reader.read_chunk().unwrap().unwrap();
        let event: Value = serde_json::from_slice(&record).unwrap();
        assert_eq!(event["token_name"], "token-1");
        assert_eq!(event["instance"], "test-instance");
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn build_uses_read_only_repository_for_replica(
        #[future] storage_and_cfg: (Arc<StorageEngine>, Cfg),
    ) {
        let (storage, mut cfg) = storage_and_cfg.await;
        cfg.role = crate::cfg::InstanceRole::Replica;
        let mut repo = build_audit_logger(&cfg, Arc::clone(&storage)).await;

        repo.log_event(make_event()).await.unwrap();
        sleep(Duration::from_secs(AGGREGATION_WINDOW_SECS * 2)).await;

        assert!(storage.get_bucket(SYSTEM_BUCKET_NAME).await.is_err());
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn build_disables_audit_when_not_enabled(
        #[future] storage_and_cfg: (Arc<StorageEngine>, Cfg),
    ) {
        let (storage, mut cfg) = storage_and_cfg.await;
        cfg.system_events_conf.enabled = false;
        let mut repo = build_audit_logger(&cfg, Arc::clone(&storage)).await;

        repo.log_event(make_event()).await.unwrap();
        sleep(Duration::from_millis(AGGREGATION_WINDOW_SECS * 1000 + 300)).await;

        assert!(storage.get_bucket(SYSTEM_BUCKET_NAME).await.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn writes_lifecycle_event_to_system_bucket() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let mut cfg = Cfg {
            data_path: tmp_dir.keep(),
            ..Cfg::default()
        };
        cfg.system_events_conf.enabled = true;

        let storage = Arc::new(
            StorageEngine::builder()
                .with_data_path(cfg.data_path.clone())
                .with_cfg(cfg.clone())
                .build()
                .await,
        );

        let mut logger = build_system_logger(&cfg, Arc::clone(&storage)).await;

        logger
            .log_event(SystemEvent {
                kind: SystemEventKind::Lifecycle,
                event_type: "lifecycle_run".to_string(),
                timestamp: 100,
                instance: "instance-1".to_string(),
                entry_name: "policy-1".to_string(),
                status: 200,
                message: "".to_string(),
                payload: serde_json::json!({"policy_name": "policy-1"}),
            })
            .await
            .unwrap();

        let bucket = storage
            .get_bucket(SYSTEM_BUCKET_NAME)
            .await
            .unwrap()
            .upgrade_and_unwrap();

        let mut reader = bucket
            .begin_read("lifecycle/instance-1/policy-1", 100)
            .await
            .unwrap();
        let record = reader.read_chunk().unwrap().unwrap();
        let event: Value = serde_json::from_slice(&record).unwrap();

        assert_eq!(event["policy_name"], "policy-1");
        assert_eq!(event["status"], 200);
        assert_eq!(event["instance"], "instance-1");
    }

    // --- Phase 0 characterization tests -------------------------------------
    // These pin the EXTERNAL `$system` record format (the flat-JSON key-set,
    // values and labels) for the families whose existing coverage was thin
    // (audit, lifecycle). They must keep passing — unchanged — across the
    // refactor: that is the proof the record format did not change.

    async fn enabled_storage() -> (Arc<StorageEngine>, Cfg) {
        let tmp_dir = tempdir().unwrap();
        let mut cfg = Cfg {
            data_path: tmp_dir.keep(),
            ..Cfg::default()
        };
        cfg.system_events_conf.enabled = true;
        let storage = Arc::new(
            StorageEngine::builder()
                .with_data_path(cfg.data_path.clone())
                .with_cfg(cfg.clone())
                .build()
                .await,
        );
        (storage, cfg)
    }

    fn lifecycle_system_event(payload: Value, status: u16, message: &str) -> SystemEvent {
        SystemEvent {
            kind: SystemEventKind::Lifecycle,
            event_type: "lifecycle_run".to_string(),
            timestamp: 100,
            instance: "instance-1".to_string(),
            entry_name: "policy-1".to_string(),
            status,
            message: message.to_string(),
            payload,
        }
    }

    /// KEYSTONE INVARIANT: `to_flat_json` is the external record serializer. It
    /// must emit exactly `{timestamp, instance, status, message}` plus the
    /// payload keys, and MUST NOT leak any routing field (`type`/`event_type`,
    /// `entry_name`, or — after the refactor — `kind`). Adding a routing field
    /// to `SystemEvent` must not change the persisted record.
    #[test]
    fn to_flat_json_record_keyset_excludes_routing_fields() {
        let bytes = make_event().to_flat_json().unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        let object = value.as_object().unwrap();

        let mut keys = object.keys().cloned().collect::<Vec<_>>();
        keys.sort();
        assert_eq!(
            keys,
            vec![
                "call_count",
                "client_ip",
                "duration",
                "instance",
                "message",
                "method",
                "path",
                "status",
                "timestamp",
                "token_name",
            ]
        );
        assert!(!object.contains_key("type"));
        assert!(!object.contains_key("event_type"));
        assert!(!object.contains_key("entry_name"));
        assert!(!object.contains_key("kind"));
        assert_eq!(value["timestamp"], 1);
        assert_eq!(value["instance"], "test-instance");
        assert_eq!(value["status"], 200);
        assert_eq!(value["message"], "");
    }

    /// CHARACTERIZATION: the exact persisted audit record — full key-set,
    /// representative values and the `status` label (and the absence of a
    /// `level` label, which is logs-only).
    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn audit_persisted_record_keyset_and_labels(
        #[future] storage_and_cfg: (Arc<StorageEngine>, Cfg),
    ) {
        let (storage, cfg) = storage_and_cfg.await;
        let mut repo = build_audit_logger(&cfg, Arc::clone(&storage)).await;
        repo.log_event(make_event()).await.unwrap();
        sleep(Duration::from_secs(AGGREGATION_WINDOW_SECS * 2)).await;

        let bucket = storage
            .get_bucket(SYSTEM_BUCKET_NAME)
            .await
            .unwrap()
            .upgrade_and_unwrap();
        let mut reader = bucket
            .begin_read("audit/test-instance/token-1", 1)
            .await
            .unwrap();

        // The only label on an audit record is the HTTP status.
        let labels = reader.meta().labels().clone();
        assert_eq!(labels.get("status").map(String::as_str), Some("200"));
        assert!(!labels.contains_key("level"));

        let record = reader.read_chunk().unwrap().unwrap();
        let event: Value = serde_json::from_slice(&record).unwrap();
        let object = event.as_object().unwrap();
        let mut keys = object.keys().cloned().collect::<Vec<_>>();
        keys.sort();
        assert_eq!(
            keys,
            vec![
                "call_count",
                "client_ip",
                "duration",
                "instance",
                "message",
                "method",
                "path",
                "status",
                "timestamp",
                "token_name",
            ]
        );
        assert_eq!(event["timestamp"], 1);
        assert_eq!(event["instance"], "test-instance");
        assert_eq!(event["status"], 200);
        assert_eq!(event["message"], "");
        assert_eq!(event["token_name"], "token-1");
        assert_eq!(event["method"], "GET");
        assert_eq!(event["path"], "/api/v1/info");
        assert_eq!(event["call_count"], 1);
        assert!(event["client_ip"].is_null());
        assert!((event["duration"].as_f64().unwrap() - 0.1).abs() < 1e-9);
    }

    /// CHARACTERIZATION: the success-path lifecycle record. `last_processed_ts`
    /// is present (Some) and the error fields are absent (None).
    #[tokio::test(flavor = "multi_thread")]
    async fn lifecycle_success_persisted_record_keyset() {
        use crate::syslog::payload::lifecycle::LifecycleSystemEventPayload;

        let (storage, cfg) = enabled_storage().await;
        let mut logger = build_system_logger(&cfg, Arc::clone(&storage)).await;

        let payload = LifecycleSystemEventPayload::success(
            "policy-1",
            "compress",
            "bucket-1",
            1.5,
            42,
            Some(7),
            Some(123_456),
            true,
        )
        .to_value();
        logger
            .log_event(lifecycle_system_event(payload, 200, ""))
            .await
            .unwrap();

        let bucket = storage
            .get_bucket(SYSTEM_BUCKET_NAME)
            .await
            .unwrap()
            .upgrade_and_unwrap();
        let mut reader = bucket
            .begin_read("lifecycle/instance-1/policy-1", 100)
            .await
            .unwrap();
        let record = reader.read_chunk().unwrap().unwrap();
        let event: Value = serde_json::from_slice(&record).unwrap();
        let object = event.as_object().unwrap();
        let mut keys = object.keys().cloned().collect::<Vec<_>>();
        keys.sort();
        assert_eq!(
            keys,
            vec![
                "action_type",
                "bucket",
                "caught_up",
                "duration",
                "instance",
                "last_processed_ts",
                "message",
                "policy_name",
                "processed_blocks",
                "processed_records",
                "status",
                "timestamp",
            ]
        );
        assert_eq!(event["policy_name"], "policy-1");
        assert_eq!(event["action_type"], "compress");
        assert_eq!(event["bucket"], "bucket-1");
        assert_eq!(event["processed_records"], 42);
        assert_eq!(event["processed_blocks"], 7);
        assert_eq!(event["last_processed_ts"], 123_456);
        assert_eq!(event["caught_up"], true);
        assert_eq!(event["status"], 200);
        assert!(!object.contains_key("error_code"));
        assert!(!object.contains_key("error_message"));
    }

    /// CHARACTERIZATION: the error-path lifecycle record. Failure metadata is
    /// carried by the top-level `status`/`message` (per PR-1491); the payload
    /// no longer carries `error_code`/`error_message`, and `last_processed_ts`
    /// is absent.
    #[tokio::test(flavor = "multi_thread")]
    async fn lifecycle_error_persisted_record_keyset() {
        use crate::syslog::payload::lifecycle::LifecycleSystemEventPayload;

        let (storage, cfg) = enabled_storage().await;
        let mut logger = build_system_logger(&cfg, Arc::clone(&storage)).await;

        let payload =
            LifecycleSystemEventPayload::error("policy-1", "delete", "bucket-1", 0.25).to_value();
        logger
            .log_event(lifecycle_system_event(payload, 404, "boom"))
            .await
            .unwrap();

        let bucket = storage
            .get_bucket(SYSTEM_BUCKET_NAME)
            .await
            .unwrap()
            .upgrade_and_unwrap();
        let mut reader = bucket
            .begin_read("lifecycle/instance-1/policy-1", 100)
            .await
            .unwrap();
        let record = reader.read_chunk().unwrap().unwrap();
        let event: Value = serde_json::from_slice(&record).unwrap();
        let object = event.as_object().unwrap();
        let mut keys = object.keys().cloned().collect::<Vec<_>>();
        keys.sort();
        assert_eq!(
            keys,
            vec![
                "action_type",
                "bucket",
                "caught_up",
                "duration",
                "instance",
                "message",
                "policy_name",
                "processed_blocks",
                "processed_records",
                "status",
                "timestamp",
            ]
        );
        assert_eq!(event["policy_name"], "policy-1");
        assert_eq!(event["action_type"], "delete");
        assert_eq!(event["bucket"], "bucket-1");
        assert_eq!(event["processed_records"], 0);
        assert_eq!(event["processed_blocks"], 0);
        assert_eq!(event["caught_up"], false);
        // Failure metadata is now the top-level status/message.
        assert_eq!(event["status"], 404);
        assert_eq!(event["message"], "boom");
        assert!(!object.contains_key("error_code"));
        assert!(!object.contains_key("error_message"));
        assert!(!object.contains_key("last_processed_ts"));
    }
}
