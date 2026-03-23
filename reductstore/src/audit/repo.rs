// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::audit::{AuditEvent, AuditQuery, ManageAudit, AUDIT_BUCKET_NAME};
use crate::cfg::Cfg;
use crate::storage::engine::StorageEngine;
use async_trait::async_trait;
use reduct_base::error::ReductError;
use reduct_base::Labels;
use std::sync::Arc;

// The local audit repository writes audit events into the internal $audit bucket.
pub(crate) struct AuditRepository {
    #[allow(dead_code)]
    cfg: Cfg,
    #[allow(dead_code)]
    storage: Arc<StorageEngine>,
}

impl AuditRepository {
    pub async fn new(cfg: Cfg, storage: Arc<StorageEngine>) -> Self {
        Self { cfg, storage }
    }
}

#[async_trait]
impl ManageAudit for AuditRepository {
    async fn log_event(&mut self, event: AuditEvent) -> Result<(), ReductError> {
        let _labels = Labels::from([
            ("token_name".to_string(), event.token_name.clone()),
            ("endpoint".to_string(), event.endpoint.clone()),
        ]);
        let _bucket_name = AUDIT_BUCKET_NAME;
        let _payload = serde_json::to_vec(&event).map_err(|err| {
            ReductError::internal_server_error(&format!("Failed to serialize audit event: {}", err))
        })?;

        // TODO: write event to the $audit bucket
        Ok(())
    }

    async fn query_token_events(
        &mut self,
        _token_name: &str,
        _filter: AuditQuery,
    ) -> Result<Vec<AuditEvent>, ReductError> {
        // TODO: query audit records from the $audit bucket.
        Ok(vec![])
    }
}
