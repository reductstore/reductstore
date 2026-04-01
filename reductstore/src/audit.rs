// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

mod aggregator;
mod read_only;
mod repo;

use crate::audit::read_only::ReadOnlyAuditRepository;
use crate::audit::repo::AuditRepository;
use crate::cfg::{Cfg, InstanceRole};
use crate::storage::engine::StorageEngine;
use async_trait::async_trait;
use reduct_base::error::ReductError;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub(crate) const AUDIT_BUCKET_NAME: &str = "$audit";

#[async_trait]
pub(crate) trait ManageAudit {
    async fn log_event(&mut self, event: AuditEvent) -> Result<(), ReductError>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct AuditEvent {
    pub timestamp: u64,
    pub token_name: String,
    pub endpoint: String,
    pub status: u16,
    pub call_count: u64,
    pub duration: u64,
}

pub(crate) struct AuditRepositoryBuilder {
    cfg: Cfg,
}

impl AuditRepositoryBuilder {
    pub fn new(cfg: Cfg) -> Self {
        Self { cfg }
    }

    pub async fn build(self, storage: Arc<StorageEngine>) -> BoxedAuditRepository {
        if !self.cfg.audit_conf.enabled {
            Box::new(DisabledAuditRepository)
        } else if self.cfg.role == InstanceRole::Replica {
            Box::new(ReadOnlyAuditRepository::new(self.cfg, storage).await)
        } else {
            Box::new(AuditRepository::new(self.cfg, storage).await)
        }
    }
}

struct DisabledAuditRepository;

#[async_trait]
impl ManageAudit for DisabledAuditRepository {
    async fn log_event(&mut self, _event: AuditEvent) -> Result<(), ReductError> {
        Ok(())
    }
}

pub(crate) type BoxedAuditRepository = Box<dyn ManageAudit + Send + Sync>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::aggregator::AGGREGATION_WINDOW_SECS;
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
            timestamp: 1,
            token_name: "token-1".to_string(),
            endpoint: "GET /api/v1/info".to_string(),
            status: 200,
            call_count: 1,
            duration: 100,
        }
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn builder_uses_local_repository_for_non_replica(
        #[future] storage_and_cfg: (Arc<StorageEngine>, Cfg),
    ) {
        let (storage, cfg) = storage_and_cfg.await;
        let mut repo = AuditRepositoryBuilder::new(cfg)
            .build(Arc::clone(&storage))
            .await;

        repo.log_event(make_event()).await.unwrap();
        sleep(Duration::from_millis(AGGREGATION_WINDOW_SECS * 1000 + 300)).await;

        let bucket = storage
            .get_bucket(AUDIT_BUCKET_NAME)
            .await
            .unwrap()
            .upgrade_and_unwrap();
        let mut reader = bucket.begin_read("token-1", 1).await.unwrap();
        let record = reader.read_chunk().unwrap().unwrap();
        let event: AuditEvent = serde_json::from_slice(&record).unwrap();
        assert_eq!(event.token_name, "token-1");
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn builder_uses_read_only_repository_for_replica(
        #[future] storage_and_cfg: (Arc<StorageEngine>, Cfg),
    ) {
        let (storage, mut cfg) = storage_and_cfg.await;
        cfg.role = InstanceRole::Replica;
        let mut repo = AuditRepositoryBuilder::new(cfg)
            .build(Arc::clone(&storage))
            .await;

        repo.log_event(make_event()).await.unwrap();
        sleep(Duration::from_millis(AGGREGATION_WINDOW_SECS * 1000 + 300)).await;

        assert!(storage.get_bucket(AUDIT_BUCKET_NAME).await.is_err());
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn builder_disables_audit_when_not_enabled(
        #[future] storage_and_cfg: (Arc<StorageEngine>, Cfg),
    ) {
        let (storage, mut cfg) = storage_and_cfg.await;
        cfg.audit_conf.enabled = false;
        let mut repo = AuditRepositoryBuilder::new(cfg)
            .build(Arc::clone(&storage))
            .await;

        repo.log_event(make_event()).await.unwrap();
        sleep(Duration::from_millis(AGGREGATION_WINDOW_SECS * 1000 + 300)).await;

        assert!(storage.get_bucket(AUDIT_BUCKET_NAME).await.is_err());
    }
}
