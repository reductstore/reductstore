// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::storage::engine::StorageEngine;
use crate::syslog::path::{entry_path, record_labels};
use crate::syslog::{LogSystemEvent, SystemEvent};
use async_trait::async_trait;
use bytes::Bytes;
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::msg::bucket_api::BucketSettings;
use std::sync::Arc;

pub(super) struct LocalSystemLogger {
    bucket_name: &'static str,
    bucket_settings: BucketSettings,
    storage: Arc<StorageEngine>,
}

impl LocalSystemLogger {
    pub(super) fn new(
        bucket_name: &'static str,
        bucket_settings: BucketSettings,
        storage: Arc<StorageEngine>,
    ) -> Self {
        Self {
            bucket_name,
            bucket_settings,
            storage,
        }
    }

    async fn log_local(&self, event: SystemEvent) -> Result<(), ReductError> {
        let entry_name = entry_path(&event);
        let labels = record_labels(&event);
        let payload = event.to_flat_json()?;
        let mut writer = match self
            .storage
            .begin_write(
                self.bucket_name,
                &entry_name,
                event.timestamp,
                payload.len() as u64,
                "application/json".to_string(),
                labels.clone(),
            )
            .await
        {
            Ok(writer) => writer,
            Err(err) if err.status == ErrorCode::NotFound => {
                let bucket = self
                    .storage
                    .create_system_bucket(self.bucket_name, self.bucket_settings.clone())
                    .await?;
                // The server owns this bucket, so protect it from being removed,
                // renamed or reconfigured through the API.
                if let Ok(bucket) = bucket.upgrade() {
                    bucket.set_provisioned(true);
                }
                self.storage
                    .begin_write(
                        self.bucket_name,
                        &entry_name,
                        event.timestamp,
                        payload.len() as u64,
                        "application/json".to_string(),
                        labels,
                    )
                    .await?
            }
            Err(err) => return Err(err),
        };
        writer.send(Ok(Some(Bytes::from(payload)))).await?;
        writer.send(Ok(None)).await?;
        Ok(())
    }
}

#[async_trait]
impl LogSystemEvent for LocalSystemLogger {
    async fn log_event(&mut self, event: SystemEvent) -> Result<(), ReductError> {
        self.log_local(event).await
    }
}
