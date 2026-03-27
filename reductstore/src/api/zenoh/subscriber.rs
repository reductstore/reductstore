// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::zenoh::attachments;
use crate::api::Components;
use crate::cfg::zenoh::ZenohApiConfig;
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
/// All data is written to a fixed bucket configured via `RS_ZENOH_BUCKET`.
/// The full Zenoh key expression becomes the entry name.
pub(crate) struct SubscriberPipeline {
    components: Arc<Components>,
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
    pub(crate) async fn handle_sample(
        &self,
        key_expr: &str,
        payload: Bytes,
        attachment: Option<Vec<u8>>,
        timestamp: Option<u64>,
        content_type: String,
        source_labels: Labels,
    ) -> Result<(), IngestError> {
        let entry_name = key_expr.trim_matches('/');

        let mut labels = match attachment {
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

        for (key, value) in source_labels {
            labels.insert(key, value);
        }

        let ts = timestamp.unwrap_or_else(|| current_time_us());
        let content_size = payload.len() as u64;

        self.components.limits.check_api_request().await?;
        self.components.limits.check_ingress(content_size).await?;

        debug!(
            "Ingesting Zenoh sample bucket={} entry={} timestamp={} bytes={} content_type={}",
            self.bucket, entry_name, ts, content_size, content_type
        );

        let bucket = self
            .components
            .storage
            .get_bucket(&self.bucket)
            .await?
            .upgrade()?;

        let mut writer = bucket
            .begin_write(&entry_name, ts, content_size, content_type, labels.clone())
            .await?;

        writer.send(Ok(Some(payload))).await?;
        writer.send(Ok(None)).await?;

        self.notify_replication(&self.bucket, &entry_name, ts, labels)
            .await?;

        Ok(())
    }

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
