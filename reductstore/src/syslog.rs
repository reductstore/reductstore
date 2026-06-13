// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

mod forward_system_logger;
mod local_system_logger;

use crate::cfg::Cfg;
use crate::core::sync::AsyncRwLock;
use crate::lifecycle::SystemEventSink;
use crate::storage::engine::StorageEngine;
use crate::storage::usage::{UsageCounters, UsageEventAggregator};
use async_trait::async_trait;
use forward_system_logger::ForwardSystemLogger;
use local_system_logger::LocalSystemLogger;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use reduct_base::msg::bucket_api::{BucketSettings, QuotaType};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;

pub(crate) const AUDIT_BUCKET_NAME: &str = "$audit";
pub(crate) const SYSTEM_BUCKET_NAME: &str = "$system";
pub(crate) const SYSTEM_AUDIT_ENTRY_PREFIX: &str = "audit";
pub(crate) const SYSTEM_LIFECYCLE_ENTRY_PREFIX: &str = "lifecycle";
pub(crate) const SYSTEM_REPLICATION_ENTRY_PREFIX: &str = "replications";
pub(crate) const SYSTEM_USAGE_ENTRY_PREFIX: &str = "usage";

pub(crate) type SystemEventFlushFuture =
    Pin<Box<dyn Future<Output = Result<(), ReductError>> + Send>>;
pub(crate) type SystemEventHandler =
    Arc<dyn Fn(SystemEvent) -> SystemEventFlushFuture + Send + Sync>;

#[async_trait]
pub(crate) trait SystemEventAggregator: Send + Sync {
    async fn log_event(&self, event: SystemEvent) -> Result<(), ReductError>;
}

pub(crate) type BoxedSystemEventAggregator = Box<dyn SystemEventAggregator + Send + Sync>;

pub(crate) fn build_audit_event_aggregator(
    handler: SystemEventHandler,
) -> BoxedSystemEventAggregator {
    Box::new(crate::api::audit::aggregator::ApiAuditEventAggregator::new(
        handler,
    ))
}

#[async_trait]
pub(crate) trait LogSystemEvent {
    async fn log_event(&mut self, event: SystemEvent) -> Result<(), ReductError>;
}

pub(crate) struct SystemLoggerBuilder {
    bucket_name: &'static str,
    bucket_settings: BucketSettings,
    entry_prefix: Option<&'static str>,
}

impl SystemLoggerBuilder {
    pub(crate) fn new(bucket_name: &'static str, bucket_settings: BucketSettings) -> Self {
        Self {
            bucket_name,
            bucket_settings,
            entry_prefix: None,
        }
    }

    pub(crate) fn with_entry_prefix(mut self, entry_prefix: &'static str) -> Self {
        self.entry_prefix = Some(entry_prefix);
        self
    }

    pub(crate) fn build(
        self,
        cfg: &Cfg,
        storage: Arc<StorageEngine>,
    ) -> Result<Box<dyn LogSystemEvent + Send + Sync>, ReductError> {
        if cfg.role == crate::cfg::InstanceRole::Replica {
            Ok(Box::new(ForwardSystemLogger::new(
                self.bucket_name,
                self.entry_prefix,
                cfg,
            )?))
        } else {
            Ok(Box::new(LocalSystemLogger::new(
                self.bucket_name,
                self.bucket_settings,
                self.entry_prefix,
                storage,
            )))
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SystemEvent {
    #[serde(default = "default_audit_type", rename = "type")]
    pub event_type: String,
    pub timestamp: u64,
    #[serde(default = "default_audit_instance")]
    pub instance: String,
    pub entry_name: String,
    pub status: u16,
    #[serde(default = "default_audit_message")]
    pub message: String,
    pub payload: Value,
}

impl SystemEvent {
    pub(crate) fn to_flat_json(&self) -> Result<Vec<u8>, ReductError> {
        let mut map = serde_json::Map::new();
        map.insert("timestamp".to_string(), serde_json::json!(self.timestamp));
        map.insert("instance".to_string(), serde_json::json!(self.instance));
        map.insert("status".to_string(), serde_json::json!(self.status));
        map.insert("message".to_string(), serde_json::json!(self.message));
        if let Value::Object(payload_map) = &self.payload {
            for (k, v) in payload_map {
                map.insert(k.clone(), v.clone());
            }
        }

        serde_json::to_vec(&map)
            .map_err(|err| internal_server_error!("Failed to serialize audit event: {}", err))
    }
}

fn default_audit_type() -> String {
    "api_call".to_string()
}

fn default_audit_instance() -> String {
    "unknown".to_string()
}

fn default_audit_message() -> String {
    "".to_string()
}

pub(crate) async fn build_audit_logger(
    cfg: &Cfg,
    storage: Arc<StorageEngine>,
) -> BoxedSystemLogger {
    if !cfg.system_events_conf.enabled {
        Box::new(DisabledAuditLogger)
    } else {
        let bucket_settings = system_bucket_settings(cfg);
        let system_logger = SystemLoggerBuilder::new(SYSTEM_BUCKET_NAME, bucket_settings)
            .with_entry_prefix(SYSTEM_AUDIT_ENTRY_PREFIX)
            .build(cfg, storage)
            .expect("audit system logger must build");

        let system_logger = Arc::new(Mutex::new(system_logger));
        let handler: SystemEventHandler = Arc::new(move |event| {
            let system_logger = Arc::clone(&system_logger);
            Box::pin(async move { system_logger.lock().await.log_event(event).await })
        });

        Box::new(AggregatedAuditLogger {
            aggregator: build_audit_event_aggregator(handler),
        })
    }
}

pub(crate) async fn build_system_logger(
    cfg: &Cfg,
    storage: Arc<StorageEngine>,
) -> BoxedSystemLogger {
    if !cfg.system_events_conf.enabled {
        return Box::new(DisabledSystemLogger);
    }

    SystemLoggerBuilder::new(SYSTEM_BUCKET_NAME, system_bucket_settings(cfg))
        .with_entry_prefix(SYSTEM_LIFECYCLE_ENTRY_PREFIX)
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

    SystemLoggerBuilder::new(SYSTEM_BUCKET_NAME, system_bucket_settings(cfg))
        .with_entry_prefix(SYSTEM_REPLICATION_ENTRY_PREFIX)
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

    SystemLoggerBuilder::new(SYSTEM_BUCKET_NAME, system_bucket_settings(cfg))
        .with_entry_prefix(SYSTEM_USAGE_ENTRY_PREFIX)
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

struct AggregatedAuditLogger {
    aggregator: BoxedSystemEventAggregator,
}

#[async_trait]
impl LogSystemEvent for AggregatedAuditLogger {
    async fn log_event(&mut self, event: SystemEvent) -> Result<(), ReductError> {
        self.aggregator.log_event(event).await
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

pub(crate) type BoxedSystemLogger = Box<dyn LogSystemEvent + Send + Sync>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::audit::aggregator::AGGREGATION_WINDOW_SECS;
    use crate::cfg::Cfg;
    use reduct_base::io::ReadRecord;
    use rstest::{fixture, rstest};
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

    #[test]
    fn default_audit_type_is_api_call() {
        assert_eq!(default_audit_type(), "api_call");
    }

    #[test]
    fn default_audit_instance_is_unknown() {
        assert_eq!(default_audit_instance(), "unknown");
    }

    #[test]
    fn default_audit_message_is_empty() {
        assert_eq!(default_audit_message(), "");
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
}
