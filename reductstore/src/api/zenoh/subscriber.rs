// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::limits::LimitScope;
use crate::api::zenoh::attachments;
use crate::api::zenoh::routing::BucketRouting;
use crate::api::Components;
use crate::cfg::zenoh::ZenohApiConfig;
use crate::replication::{Transaction, TransactionNotification};
use bytes::Bytes;
use log::{debug, info, warn};
use reduct_base::error::ReductError;
use reduct_base::io::RecordMeta;
use reduct_base::msg::bucket_api::BucketSettings;
use reduct_base::Labels;
use std::collections::HashSet;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

/// Subscriber pipeline for ingesting Zenoh samples into ReductStore.
pub(crate) struct SubscriberPipeline {
    components: Arc<Components>,
    routing: BucketRouting,
    known_buckets: Mutex<HashSet<String>>,
}

impl SubscriberPipeline {
    pub(crate) fn new(config: ZenohApiConfig, components: Arc<Components>) -> Self {
        SubscriberPipeline {
            components,
            routing: BucketRouting::from_config(&config),
            known_buckets: Mutex::new(HashSet::new()),
        }
    }

    async fn ensure_bucket(&self, bucket: &str) -> Result<(), ReductError> {
        let mut known = self.known_buckets.lock().await;
        if known.contains(bucket) {
            return Ok(());
        }
        if self.components.storage.get_bucket(bucket).await.is_err() {
            match self
                .components
                .storage
                .create_bucket(bucket, BucketSettings::default())
                .await
            {
                Ok(_) => info!("Zenoh subscriber created bucket '{}'", bucket),
                // lost a create race with another writer
                Err(_) if self.components.storage.get_bucket(bucket).await.is_ok() => {}
                Err(err) => return Err(err),
            }
        }
        known.insert(bucket.to_string());
        Ok(())
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
        let (bucket, entry_name) = self.routing.resolve(key_expr);

        let mut labels = match attachment {
            Some(raw_labels) => match attachments::deserialize_labels(&raw_labels) {
                Ok(labels) => labels,
                Err(err) => {
                    warn!(
                        "Failed to decode labels for {}:{} ({}): {}",
                        bucket, entry_name, key_expr, err
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

        self.components
            .limits
            .check_api_request_for(LimitScope::GlobalFallback)
            .await?;
        self.components
            .limits
            .check_ingress_for(LimitScope::GlobalFallback, content_size)
            .await?;

        if self.routing.is_dynamic() {
            self.ensure_bucket(bucket).await?;
        }

        debug!(
            "Ingesting Zenoh sample bucket={} entry={} timestamp={} bytes={} content_type={}",
            bucket, entry_name, ts, content_size, content_type
        );

        let mut writer = self
            .components
            .storage
            .begin_write(
                bucket,
                entry_name,
                ts,
                content_size,
                content_type,
                labels.clone(),
            )
            .await?;

        writer.send(Ok(Some(payload))).await?;
        writer.send(Ok(None)).await?;

        self.notify_replication(bucket, entry_name, ts, labels)
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
            "Zenoh subscriber ready (storage version {}): {}",
            server_info.version,
            self.routing.describe()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::components::StateKeeper;
    use crate::api::http::tests::{api_limited_keeper, ingress_limited_keeper, keeper};
    use crate::cfg::zenoh::ZenohBucketRouting;
    use reduct_base::error::ErrorCode;
    use rstest::rstest;
    use std::sync::Arc;

    fn config() -> ZenohApiConfig {
        ZenohApiConfig {
            bucket: "bucket-1".to_string(),
            ..Default::default()
        }
    }

    #[rstest]
    #[tokio::test]
    async fn key_prefix_routing_creates_bucket_on_demand(#[future] keeper: Arc<StateKeeper>) {
        let components = keeper.await.get_anonymous().await.unwrap();
        let pipeline = SubscriberPipeline::new(
            ZenohApiConfig {
                bucket_routing: ZenohBucketRouting::KeyPrefix,
                ..config()
            },
            Arc::clone(&components),
        );

        assert!(components.storage.get_bucket("run_abc123").await.is_err());

        for ts in [100, 101] {
            pipeline
                .handle_sample(
                    "/run_abc123/motion/welder/commanded",
                    Bytes::from("payload"),
                    None,
                    Some(ts),
                    "application/cbor".to_string(),
                    Labels::new(),
                )
                .await
                .unwrap();
        }

        let bucket = components
            .storage
            .get_bucket("run_abc123")
            .await
            .unwrap()
            .upgrade()
            .unwrap();
        let entry = bucket
            .get_entry("motion/welder/commanded")
            .await
            .unwrap()
            .upgrade()
            .unwrap();
        assert_eq!(entry.info().await.unwrap().record_count, 2);
    }

    #[rstest]
    #[tokio::test]
    async fn key_prefix_routing_single_chunk_falls_back(#[future] keeper: Arc<StateKeeper>) {
        let components = keeper.await.get_anonymous().await.unwrap();
        let pipeline = SubscriberPipeline::new(
            ZenohApiConfig {
                bucket_routing: ZenohBucketRouting::KeyPrefix,
                ..config()
            },
            Arc::clone(&components),
        );

        pipeline
            .handle_sample(
                "orphan",
                Bytes::from("x"),
                None,
                Some(100),
                "text/plain".to_string(),
                Labels::new(),
            )
            .await
            .unwrap();

        let bucket = components
            .storage
            .get_bucket("bucket-1")
            .await
            .unwrap()
            .upgrade()
            .unwrap();
        assert!(bucket.get_entry("orphan").await.is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn static_routing_keeps_full_key_as_entry(#[future] keeper: Arc<StateKeeper>) {
        let components = keeper.await.get_anonymous().await.unwrap();
        let pipeline = SubscriberPipeline::new(config(), Arc::clone(&components));

        pipeline
            .handle_sample(
                "/factory/line1/status",
                Bytes::from("x"),
                None,
                Some(100),
                "text/plain".to_string(),
                Labels::new(),
            )
            .await
            .unwrap();

        let bucket = components
            .storage
            .get_bucket("bucket-1")
            .await
            .unwrap()
            .upgrade()
            .unwrap();
        assert!(bucket.get_entry("factory/line1/status").await.is_ok());
        assert!(components.storage.get_bucket("factory").await.is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn handle_sample_rejects_ingress_over_limit(
        #[future] ingress_limited_keeper: Arc<StateKeeper>,
    ) {
        let components = ingress_limited_keeper.await.get_anonymous().await.unwrap();
        let pipeline = SubscriberPipeline::new(config(), components);

        let err = pipeline
            .handle_sample(
                "/entry-zenoh",
                Bytes::from("ab"),
                None,
                Some(100),
                "text/plain".to_string(),
                Labels::new(),
            )
            .await
            .err()
            .unwrap();

        let IngestError::Storage(err) = err;
        assert_eq!(err.status, ErrorCode::TooManyRequests);
        assert!(err.message.contains("ingress bytes"));
    }

    #[rstest]
    #[tokio::test]
    async fn handle_sample_rejects_api_request_over_limit(
        #[future] api_limited_keeper: Arc<StateKeeper>,
    ) {
        let components = api_limited_keeper.await.get_anonymous().await.unwrap();
        let pipeline = SubscriberPipeline::new(config(), components);

        assert!(pipeline
            .handle_sample(
                "/entry-zenoh-api-limit",
                Bytes::from("a"),
                None,
                Some(101),
                "text/plain".to_string(),
                Labels::new(),
            )
            .await
            .is_ok());

        let err = pipeline
            .handle_sample(
                "/entry-zenoh-api-limit",
                Bytes::from("a"),
                None,
                Some(102),
                "text/plain".to_string(),
                Labels::new(),
            )
            .await
            .err()
            .unwrap();

        let IngestError::Storage(err) = err;
        assert_eq!(err.status, ErrorCode::TooManyRequests);
        assert!(err.message.contains("api requests"));
    }
}
