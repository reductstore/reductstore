// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::audit::{AuditEvent, AuditQuery, ManageAudit, AUDIT_BUCKET_NAME};
use crate::cfg::Cfg;
use crate::storage::bucket::Bucket;
use crate::storage::engine::StorageEngine;
use async_trait::async_trait;
use bytes::Bytes;
use reduct_base::error::ReductError;
use reduct_base::Labels;
use std::sync::Arc;

// The local audit repository writes audit events into the internal $audit bucket.
pub(crate) struct AuditRepository {
    cfg: Cfg,
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
        let bucket = match self.storage.get_bucket(AUDIT_BUCKET_NAME).await {
            Ok(bucket) => bucket.upgrade()?,
            Err(_) => {
                self
                    // If the $audit bucket does not exist, we create it
                    .storage
                    .create_bucket(AUDIT_BUCKET_NAME, Bucket::defaults())
                    .await?
                    .upgrade()?
            }
        };
        let labels = Labels::from([("endpoint".to_string(), event.endpoint.clone())]);
        let payload = serde_json::to_vec(&event).map_err(|err| {
            ReductError::internal_server_error(&format!("Failed to serialize audit event: {}", err))
        })?;
        let mut writer = bucket
            .begin_write(
                &event.token_name,
                event.timestamp,
                payload.len() as u64,
                "application/json".to_string(),
                labels,
            )
            .await?;
        writer.send(Ok(Some(Bytes::from(payload)))).await?;
        writer.send(Ok(None)).await?;
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
