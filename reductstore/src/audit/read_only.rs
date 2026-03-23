// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::audit::{AuditEvent, AuditQuery, ManageAudit};
use crate::cfg::Cfg;
use crate::storage::engine::StorageEngine;
use async_trait::async_trait;
use reduct_base::error::ReductError;
use reduct_base::forbidden;
use std::sync::Arc;

pub(crate) struct ReadOnlyAuditRepository {
    cfg: Cfg,
    storage: Arc<StorageEngine>,
}

impl ReadOnlyAuditRepository {
    pub async fn new(cfg: Cfg, storage: Arc<StorageEngine>) -> Self {
        Self { cfg, storage }
    }
}

#[async_trait]
impl ManageAudit for ReadOnlyAuditRepository {
    async fn log_event(&mut self, _event: AuditEvent) -> Result<(), ReductError> {
        Err(forbidden!("Cannot write audit data in read-only mode"))
    }

    async fn query_token_events(
        &mut self,
        _token_name: &str,
        _filter: AuditQuery,
    ) -> Result<Vec<AuditEvent>, ReductError> {
        // TODO: fetch audit records from the primary service.
        Ok(vec![])
    }
}
