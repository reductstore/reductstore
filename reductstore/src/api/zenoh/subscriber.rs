// Copyright 2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

use super::sanitize_entry_name;
use crate::api::zenoh::attachments;
use crate::cfg::zenoh::ZenohApiConfig;
use crate::core::components::Components;
use crate::replication::{Transaction, TransactionNotification};
use bytes::Bytes;
use log::{debug, info, warn};
use reduct_base::error::ReductError;
use reduct_base::io::RecordMeta;
use reduct_base::Labels;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Subscriber pipeline for ingesting Zenoh samples into ReductStore.
///
/// In single-bucket mode, all data is written to a fixed bucket configured via
/// `RS_ZENOH_BUCKET`. The full Zenoh key expression becomes the entry name.
pub(crate) struct SubscriberPipeline {
    components: Arc<Components>,
    /// The fixed bucket name for all ingested data.
    bucket: String,
}

impl SubscriberPipeline {
    pub(crate) fn new(config: ZenohApiConfig, components: Arc<Components>) -> Self {
        SubscriberPipeline {
            components,
            bucket: config.bucket.clone(),
        }
    }

    /// Handles a single Zenoh sample by writing it into storage and notifying replications.
    ///
    /// The full key expression is used as the entry name within the configured bucket.
    /// Slashes in the key expression are replaced with underscores since ReductStore
    /// entry names only allow alphanumeric characters, hyphens, and underscores.
    pub(crate) async fn handle_sample(
        &self,
        key_expr: &str,
        payload: Bytes,
        attachment: Option<Vec<u8>>,
        timestamp: Option<u64>,
    ) -> Result<(), IngestError> {
        // In single-bucket mode: entry = full Zenoh key (with slashes replaced)
        let entry_name = sanitize_entry_name(key_expr.trim_matches('/'));

        let labels = match attachment {
            Some(raw_labels) => match attachments::deserialize_labels(&raw_labels) {
                Ok(labels) => labels,
                Err(err) => {
                    warn!(
                        "Failed to decode labels for {}:{} ({}): {}",
                        self.bucket, entry_name, key_expr, err
                    );
                    Labels::new()
                }
            },
            None => Labels::new(),
        };

        let ts = timestamp.unwrap_or_else(|| current_time_us());
        let content_size = payload.len() as u64;

        debug!(
            "Ingesting Zenoh sample bucket={} entry={} timestamp={} bytes={}",
            self.bucket, entry_name, ts, content_size
        );

        let bucket = self
            .components
            .storage
            .get_bucket(&self.bucket)
            .await?
            .upgrade()?;

        let mut writer = bucket
            .begin_write(
                &entry_name,
                ts,
                content_size,
                "application/octet-stream".to_string(),
                labels.clone(),
            )
            .await?;

        writer.send(Ok(Some(payload))).await?;
        writer.send(Ok(None)).await?;

        // Notify replication system about the write
        self.notify_replication(&self.bucket, &entry_name, ts, labels)
            .await?;

        Ok(())
    }

    /// Notifies the replication system about a write event.
    async fn notify_replication(
        &self,
        bucket: &str,
        entry: &str,
        timestamp: u64,
        labels: Labels,
    ) -> Result<(), ReductError> {
        self.components
            .replication_repo
            .write()
            .await?
            .notify(TransactionNotification {
                bucket: bucket.to_string(),
                entry: entry.to_string(),
                meta: RecordMeta::builder()
                    .timestamp(timestamp)
                    .labels(labels)
                    .build(),
                event: Transaction::WriteRecord(timestamp),
            })
            .await?;
        Ok(())
    }

    pub(crate) async fn bootstrap(&self) -> Result<(), String> {
        let server_info = self
            .components
            .storage
            .info()
            .await
            .map_err(|err| err.to_string())?;

        info!(
            "Zenoh subscriber ready (storage version {}): bucket='{}'",
            server_info.version, self.bucket
        );
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) enum IngestError {
    Storage(ReductError),
}

impl Display for IngestError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            IngestError::Storage(err) => write!(f, "Storage error: {}", err),
        }
    }
}

impl Error for IngestError {}

impl From<ReductError> for IngestError {
    fn from(value: ReductError) -> Self {
        IngestError::Storage(value)
    }
}

fn current_time_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_micros() as u64)
        .unwrap_or_default()
}
