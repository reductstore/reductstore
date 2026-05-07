// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

mod full_access;
mod read_only;

use crate::audit::full_access::FullAccessAuditLogger;
use crate::audit::read_only::ReadOnlyAuditLogger;
use crate::cfg::{Cfg, InstanceRole};
use crate::storage::engine::StorageEngine;
use async_trait::async_trait;
use reduct_base::error::ReductError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

pub(crate) const AUDIT_BUCKET_NAME: &str = "$audit";

pub(crate) type AuditFlushFuture = Pin<Box<dyn Future<Output = Result<(), ReductError>> + Send>>;
pub(crate) type AuditAggregationHandler = Arc<dyn Fn(AuditEvent) -> AuditFlushFuture + Send + Sync>;

#[async_trait]
pub(crate) trait AuditEventAggregator: Send + Sync {
    async fn log_event(&self, event: AuditEvent) -> Result<(), ReductError>;
}

pub(crate) type BoxedAuditEventAggregator = Box<dyn AuditEventAggregator + Send + Sync>;

pub(crate) fn build_audit_event_aggregator(
    handler: AuditAggregationHandler,
) -> BoxedAuditEventAggregator {
    Box::new(crate::api::audit::aggregator::ApiAuditEventAggregator::new(
        handler,
    ))
}

#[async_trait]
pub(crate) trait LogAuditEvent {
    async fn log_event(&mut self, event: AuditEvent) -> Result<(), ReductError>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct AuditEvent {
    #[serde(default = "default_audit_type", rename = "type")]
    pub event_type: String,
    pub timestamp: u64,
    #[serde(default = "default_audit_instance")]
    pub instance: String,
    #[serde(default = "default_audit_token_name")]
    pub token_name: String,
    #[serde(default = "default_audit_method")]
    pub method: String,
    #[serde(default = "default_audit_path")]
    pub path: String,
    pub status: u16,
    #[serde(default = "default_audit_message")]
    pub message: String,
    #[serde(default)]
    pub client_ip: Option<String>,
    #[serde(default = "default_audit_call_count")]
    pub call_count: u64,
    #[serde(default)]
    pub duration: f64,
    #[serde(default)]
    pub payload: Option<Value>,
}

fn default_audit_type() -> String {
    "api_call".to_string()
}

fn default_audit_instance() -> String {
    "unknown".to_string()
}

fn default_audit_method() -> String {
    "UNKNOWN".to_string()
}

fn default_audit_path() -> String {
    "".to_string()
}

fn default_audit_message() -> String {
    "".to_string()
}

fn default_audit_token_name() -> String {
    "unknown".to_string()
}

fn default_audit_call_count() -> u64 {
    1
}

pub(crate) struct AuditLoggerBuilder {
    cfg: Cfg,
}

impl AuditLoggerBuilder {
    pub fn new(cfg: Cfg) -> Self {
        Self { cfg }
    }

    pub async fn build(self, storage: Arc<StorageEngine>) -> BoxedAuditLogger {
        if !self.cfg.audit_conf.enabled {
            Box::new(DisabledAuditLogger)
        } else if self.cfg.role == InstanceRole::Replica {
            Box::new(ReadOnlyAuditLogger::new(self.cfg, storage).await)
        } else {
            Box::new(FullAccessAuditLogger::new(self.cfg, storage).await)
        }
    }
}

struct DisabledAuditLogger;

#[async_trait]
impl LogAuditEvent for DisabledAuditLogger {
    async fn log_event(&mut self, _event: AuditEvent) -> Result<(), ReductError> {
        Ok(())
    }
}

pub(crate) type BoxedAuditLogger = Box<dyn LogAuditEvent + Send + Sync>;

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
        cfg.audit_conf.enabled = true;
        let storage = Arc::new(
            StorageEngine::builder()
                .with_data_path(cfg.data_path.clone())
                .with_cfg(cfg.clone())
                .build()
                .await,
        );
        (storage, cfg)
    }

    fn make_event() -> AuditEvent {
        AuditEvent {
            event_type: "api_call".to_string(),
            timestamp: 1,
            instance: "test-instance".to_string(),
            token_name: "token-1".to_string(),
            method: "GET".to_string(),
            path: "/api/v1/info".to_string(),
            status: 200,
            message: "".to_string(),
            client_ip: None,
            call_count: 1,
            duration: 0.1,
            payload: None,
        }
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn builder_uses_local_repository_for_non_replica(
        #[future] storage_and_cfg: (Arc<StorageEngine>, Cfg),
    ) {
        let (storage, cfg) = storage_and_cfg.await;
        let mut repo = AuditLoggerBuilder::new(cfg)
            .build(Arc::clone(&storage))
            .await;

        repo.log_event(make_event()).await.unwrap();
        sleep(Duration::from_secs(AGGREGATION_WINDOW_SECS * 2)).await;

        let bucket = storage
            .get_bucket(AUDIT_BUCKET_NAME)
            .await
            .unwrap()
            .upgrade_and_unwrap();
        let mut reader = bucket.begin_read("test-instance/token-1", 1).await.unwrap();
        let record = reader.read_chunk().unwrap().unwrap();
        let event: AuditEvent = serde_json::from_slice(&record).unwrap();
        assert_eq!(event.token_name, "token-1");
        assert_eq!(event.instance, "test-instance");
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn builder_uses_read_only_repository_for_replica(
        #[future] storage_and_cfg: (Arc<StorageEngine>, Cfg),
    ) {
        let (storage, mut cfg) = storage_and_cfg.await;
        cfg.role = InstanceRole::Replica;
        let mut repo = AuditLoggerBuilder::new(cfg)
            .build(Arc::clone(&storage))
            .await;

        repo.log_event(make_event()).await.unwrap();
        sleep(Duration::from_secs(AGGREGATION_WINDOW_SECS * 2)).await;

        assert!(storage.get_bucket(AUDIT_BUCKET_NAME).await.is_err());
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn builder_disables_audit_when_not_enabled(
        #[future] storage_and_cfg: (Arc<StorageEngine>, Cfg),
    ) {
        let (storage, mut cfg) = storage_and_cfg.await;
        cfg.audit_conf.enabled = false;
        let mut repo = AuditLoggerBuilder::new(cfg)
            .build(Arc::clone(&storage))
            .await;

        repo.log_event(make_event()).await.unwrap();
        sleep(Duration::from_millis(AGGREGATION_WINDOW_SECS * 1000 + 300)).await;

        assert!(storage.get_bucket(AUDIT_BUCKET_NAME).await.is_err());
    }

    #[test]
    fn deserializes_legacy_event_with_missing_fields() {
        let event: AuditEvent = serde_json::from_str(
            r#"{
                "timestamp": 1,
                "token_name": "token-1",
                "endpoint": "GET /api/v1/info",
                "status": 200,
                "call_count": 1,
                "duration": 0.1
            }"#,
        )
        .unwrap();

        assert_eq!(event.event_type, "api_call");
        assert_eq!(event.instance, "unknown");
        assert_eq!(event.method, "UNKNOWN");
        assert_eq!(event.path, "");
        assert_eq!(event.message, "");
    }
}
