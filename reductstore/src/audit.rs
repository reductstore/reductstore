// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

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
    async fn query_token_events(
        &mut self,
        token_name: &str,
        filter: AuditQuery,
    ) -> Result<Vec<AuditEvent>, ReductError>;
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

#[derive(Clone, Debug, Default)]
pub(crate) struct AuditQuery {
    pub start: Option<u64>,
    pub end: Option<u64>,
    pub endpoint: Option<String>,
}

pub(crate) struct AuditRepositoryBuilder {
    cfg: Cfg,
}

impl AuditRepositoryBuilder {
    pub fn new(cfg: Cfg) -> Self {
        Self { cfg }
    }

    pub async fn build(self, storage: Arc<StorageEngine>) -> BoxedAuditRepository {
        if self.cfg.role == InstanceRole::Replica {
            Box::new(ReadOnlyAuditRepository::new(self.cfg, storage).await)
        } else {
            Box::new(AuditRepository::new(self.cfg, storage).await)
        }
    }
}

pub(crate) type BoxedAuditRepository = Box<dyn ManageAudit + Send + Sync>;
