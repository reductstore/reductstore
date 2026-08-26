// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::limits::LimitScope;
use crate::api::zenoh::attachments;
use crate::api::zenoh::routing::{BucketRouter, RoutingError};
use crate::api::Components;
use crate::replication::{Transaction, TransactionNotification};
use bytes::Bytes;
use log::{debug, info, warn};
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::io::RecordMeta;
use reduct_base::msg::bucket_api::BucketSettings;
use reduct_base::Labels;
use std::collections::HashSet;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

const DROP_REPORT_INTERVAL_MS: u64 = 10_000;

/// How far a record timestamp may be advanced past the publisher's stamp to clear
/// microsecond collisions before the sample is rejected.
const MAX_TIMESTAMP_SHIFT_US: u64 = 1_000;

/// Subscriber pipeline for ingesting Zenoh samples of one configured block into ReductStore.
pub(crate) struct SubscriberPipeline {
    components: Arc<Components>,
    router: Arc<BucketRouter>,
    ready_buckets: Mutex<HashSet<String>>,
    drops: DropCounter,
}

impl SubscriberPipeline {
    pub(crate) fn new(router: Arc<BucketRouter>, components: Arc<Components>) -> Self {
        SubscriberPipeline {
            components,
            router,
            ready_buckets: Mutex::new(HashSet::new()),
            drops: DropCounter::new(),
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
        let content_size = payload.len() as u64;

        // charged before routing so unroutable traffic still consumes the rate limit budget
        self.components
            .limits
            .check_api_request_for(LimitScope::GlobalFallback)
            .await?;
        self.components
            .limits
            .check_ingress_for(LimitScope::GlobalFallback, content_size)
            .await?;

        let route = self.route(key_expr)?;
        let (bucket, entry_name) = (route.bucket, route.entry);

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

        let mut ts = timestamp.unwrap_or_else(current_time_us);

        self.ensure_bucket(bucket, route.may_create).await?;

        debug!(
            "Ingesting Zenoh sample bucket={} entry={} timestamp={} bytes={} content_type={}",
            bucket, entry_name, ts, content_size, content_type
        );

        let mut recreated = false;
        let mut writer = loop {
            match self
                .components
                .storage
                .begin_write(
                    bucket,
                    entry_name,
                    ts,
                    content_size,
                    content_type.clone(),
                    labels.clone(),
                )
                .await
            {
                Ok(writer) => break writer,
                // Publishers stamping faster than the microsecond resolution of a
                // record timestamp land on an occupied slot; the next free one keeps
                // the sample and its arrival order.
                Err(err)
                    if err.status() == ErrorCode::Conflict
                        && ts.wrapping_sub(timestamp.unwrap_or(ts)) < MAX_TIMESTAMP_SHIFT_US =>
                {
                    ts += 1;
                }
                // The bucket was removed out of band after it was memoized as ready;
                // recreate it and keep the sample.
                Err(err) if err.status() == ErrorCode::NotFound && !recreated => {
                    recreated = true;
                    self.forget_bucket(bucket).await;
                    self.ensure_bucket(bucket, route.may_create).await?;
                }
                Err(err) => return Err(err.into()),
            }
        };

        writer.send(Ok(Some(payload))).await?;
        writer.send(Ok(None)).await?;

        self.notify_replication(bucket, entry_name, ts, labels)
            .await?;

        Ok(())
    }

    pub(crate) fn describe(&self) -> String {
        self.router.describe()
    }

    fn route<'a>(
        &'a self,
        key_expr: &'a str,
    ) -> Result<crate::api::zenoh::routing::Route<'a>, IngestError> {
        self.router.resolve(key_expr).map_err(|err| {
            self.report_drop(key_expr, &err);
            IngestError::Routing(err)
        })
    }

    async fn ensure_bucket(&self, bucket: &str, may_create: bool) -> Result<(), IngestError> {
        if self.ready_buckets.lock().await.contains(bucket) {
            return Ok(());
        }

        if self.components.storage.get_bucket(bucket).await.is_err() {
            if !may_create {
                let err = RoutingError::BucketMissing {
                    bucket: bucket.to_string(),
                };
                // not memoized: the bucket may be created out of band later
                self.report_drop(bucket, &err);
                return Err(IngestError::Routing(err));
            }

            match self
                .components
                .storage
                .create_bucket(bucket, BucketSettings::default())
                .await
            {
                Ok(_) => info!(
                    "Zenoh subscriber block '{}' created bucket '{}'",
                    self.router.block_id(),
                    bucket
                ),
                // lost a create race with another writer
                Err(_) if self.components.storage.get_bucket(bucket).await.is_ok() => {}
                Err(err) => return Err(IngestError::Storage(err)),
            }
        }

        self.ready_buckets.lock().await.insert(bucket.to_string());
        Ok(())
    }

    async fn forget_bucket(&self, bucket: &str) {
        self.ready_buckets.lock().await.remove(bucket);
    }

    fn report_drop(&self, subject: &str, err: &RoutingError) {
        debug!(
            "Zenoh subscriber block '{}' dropped '{}': {}",
            self.router.block_id(),
            subject,
            err
        );

        if let Some(total) = self.drops.record() {
            warn!(
                "Zenoh subscriber block '{}' has dropped {} sample(s), most recently '{}': {}",
                self.router.block_id(),
                total,
                subject,
                err
            );
        }
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
            .read()
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
}

struct DropCounter {
    start: Instant,
    dropped: AtomicU64,
    last_report_ms: AtomicU64,
}

impl DropCounter {
    fn new() -> Self {
        DropCounter {
            start: Instant::now(),
            dropped: AtomicU64::new(0),
            last_report_ms: AtomicU64::new(0),
        }
    }

    fn record(&self) -> Option<u64> {
        let total = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
        if total == 1 {
            self.last_report_ms
                .store(self.start.elapsed().as_millis() as u64, Ordering::Relaxed);
            return Some(total);
        }

        let now_ms = self.start.elapsed().as_millis() as u64;
        let last_ms = self.last_report_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(last_ms) < DROP_REPORT_INTERVAL_MS {
            return None;
        }

        self.last_report_ms
            .compare_exchange(last_ms, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .ok()
            .map(|_| total)
    }
}

#[derive(Debug)]
pub(crate) enum IngestError {
    Storage(ReductError),
    Routing(RoutingError),
}

impl Display for IngestError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            IngestError::Storage(err) => write!(f, "Storage error: {}", err),
            IngestError::Routing(err) => write!(f, "Routing error: {}", err),
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
    use crate::cfg::zenoh::{ZenohBlock, ZenohBucketRouting};
    use reduct_base::error::ErrorCode;
    use rstest::rstest;
    use std::sync::Arc;

    fn router(
        routing: ZenohBucketRouting,
        bucket: Option<&str>,
        allowlist: &[&str],
        allow_bucket_creation: bool,
    ) -> Arc<BucketRouter> {
        Arc::new(BucketRouter::from_block(&ZenohBlock {
            id: "0".to_string(),
            keyexprs: vec!["**".to_string()],
            routing,
            bucket: bucket.map(|name| name.to_string()),
            bucket_allowlist: allowlist.iter().map(|p| p.to_string()).collect(),
            allow_bucket_creation,
        }))
    }

    fn static_router() -> Arc<BucketRouter> {
        router(ZenohBucketRouting::Static, Some("bucket-1"), &[], false)
    }

    async fn write(pipeline: &SubscriberPipeline, key: &str, ts: u64) -> Result<(), IngestError> {
        pipeline
            .handle_sample(
                key,
                Bytes::from("payload"),
                None,
                Some(ts),
                "text/plain".to_string(),
                Labels::new(),
            )
            .await
    }

    fn expect_storage_error(err: IngestError) -> ReductError {
        match err {
            IngestError::Storage(err) => err,
            other => panic!("expected a storage error, got {:?}", other),
        }
    }

    fn expect_routing_error(err: IngestError) -> RoutingError {
        match err {
            IngestError::Routing(err) => err,
            other => panic!("expected a routing error, got {:?}", other),
        }
    }

    #[rstest]
    #[tokio::test]
    async fn recreates_bucket_removed_out_of_band(#[future] keeper: Arc<StateKeeper>) {
        let components = keeper.await.get_anonymous().await.unwrap();
        let pipeline = SubscriberPipeline::new(
            router(ZenohBucketRouting::KeyPrefix, None, &["run_*"], true),
            Arc::clone(&components),
        );

        write(&pipeline, "/run_gone/motion", 100).await.unwrap();
        components.storage.remove_bucket("run_gone").await.unwrap();

        let mut attempts = 0;
        while write(&pipeline, "/run_gone/motion", 200).await.is_err() {
            attempts += 1;
            assert!(attempts < 100, "ingest never recovered from bucket removal");
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        let bucket = components
            .storage
            .get_bucket("run_gone")
            .await
            .unwrap()
            .upgrade()
            .unwrap();
        assert_eq!(
            bucket
                .get_entry("motion")
                .await
                .unwrap()
                .upgrade()
                .unwrap()
                .info()
                .await
                .unwrap()
                .record_count,
            1
        );
    }

    #[rstest]
    #[tokio::test]
    async fn shifts_timestamp_when_slot_is_taken(#[future] keeper: Arc<StateKeeper>) {
        let components = keeper.await.get_anonymous().await.unwrap();
        let pipeline = SubscriberPipeline::new(static_router(), Arc::clone(&components));

        for _ in 0..3 {
            write(&pipeline, "motion/welder", 500).await.unwrap();
        }

        let entry = components
            .storage
            .get_bucket("bucket-1")
            .await
            .unwrap()
            .upgrade()
            .unwrap()
            .get_entry("motion/welder")
            .await
            .unwrap()
            .upgrade()
            .unwrap();
        assert_eq!(entry.info().await.unwrap().record_count, 3);
        for ts in [500, 501, 502] {
            assert!(entry.begin_read(ts).await.is_ok(), "record {} missing", ts);
        }
    }

    #[rstest]
    #[tokio::test]
    async fn rejects_timestamp_shifted_beyond_the_limit(#[future] keeper: Arc<StateKeeper>) {
        let components = keeper.await.get_anonymous().await.unwrap();
        let pipeline = SubscriberPipeline::new(static_router(), Arc::clone(&components));

        for offset in 0..=MAX_TIMESTAMP_SHIFT_US {
            write(&pipeline, "motion/welder", 1_000 + offset)
                .await
                .unwrap();
        }

        let err = expect_storage_error(write(&pipeline, "motion/welder", 1_000).await.unwrap_err());
        assert_eq!(err.status(), ErrorCode::Conflict);
    }

    #[rstest]
    #[tokio::test]
    async fn key_prefix_creates_bucket_when_allowed(#[future] keeper: Arc<StateKeeper>) {
        let components = keeper.await.get_anonymous().await.unwrap();
        let pipeline = SubscriberPipeline::new(
            router(ZenohBucketRouting::KeyPrefix, None, &["run_*"], true),
            Arc::clone(&components),
        );

        assert!(components.storage.get_bucket("run_abc123").await.is_err());

        for ts in [100, 101] {
            write(&pipeline, "/run_abc123/motion/welder/commanded", ts)
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
    async fn key_prefix_rejects_creation_when_disabled(#[future] keeper: Arc<StateKeeper>) {
        let components = keeper.await.get_anonymous().await.unwrap();
        let pipeline = SubscriberPipeline::new(
            router(ZenohBucketRouting::KeyPrefix, None, &["*"], false),
            Arc::clone(&components),
        );

        let err = write(&pipeline, "/new_bucket/entry", 100)
            .await
            .unwrap_err();
        assert_eq!(
            expect_routing_error(err),
            RoutingError::BucketMissing {
                bucket: "new_bucket".to_string()
            }
        );
        assert!(components.storage.get_bucket("new_bucket").await.is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn bucket_missing_is_not_memoized(#[future] keeper: Arc<StateKeeper>) {
        let components = keeper.await.get_anonymous().await.unwrap();
        let pipeline = SubscriberPipeline::new(
            router(ZenohBucketRouting::KeyPrefix, None, &["*"], false),
            Arc::clone(&components),
        );

        assert!(write(&pipeline, "/later_bucket/entry", 100).await.is_err());

        components
            .storage
            .create_bucket("later_bucket", BucketSettings::default())
            .await
            .unwrap();

        write(&pipeline, "/later_bucket/entry", 101).await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn key_prefix_writes_to_existing_bucket_without_creation(
        #[future] keeper: Arc<StateKeeper>,
    ) {
        let components = keeper.await.get_anonymous().await.unwrap();
        let pipeline = SubscriberPipeline::new(
            router(ZenohBucketRouting::KeyPrefix, None, &["bucket-*"], false),
            Arc::clone(&components),
        );

        write(&pipeline, "/bucket-2/entry-x", 100).await.unwrap();

        let bucket = components
            .storage
            .get_bucket("bucket-2")
            .await
            .unwrap()
            .upgrade()
            .unwrap();
        assert!(bucket.get_entry("entry-x").await.is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn key_prefix_uses_fallback_for_single_chunk(#[future] keeper: Arc<StateKeeper>) {
        let components = keeper.await.get_anonymous().await.unwrap();
        let pipeline = SubscriberPipeline::new(
            router(
                ZenohBucketRouting::KeyPrefix,
                Some("bucket-1"),
                &["bucket-*"],
                true,
            ),
            Arc::clone(&components),
        );

        write(&pipeline, "orphan", 100).await.unwrap();

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
    async fn key_prefix_without_fallback_drops_single_chunk(#[future] keeper: Arc<StateKeeper>) {
        let components = keeper.await.get_anonymous().await.unwrap();
        let pipeline = SubscriberPipeline::new(
            router(ZenohBucketRouting::KeyPrefix, None, &["*"], true),
            Arc::clone(&components),
        );

        let err = write(&pipeline, "orphan", 100).await.unwrap_err();
        assert_eq!(
            expect_routing_error(err),
            RoutingError::NoFallbackBucket {
                key: "orphan".to_string()
            }
        );
    }

    #[rstest]
    #[tokio::test]
    async fn allowlist_rejects_write_to_other_bucket(#[future] keeper: Arc<StateKeeper>) {
        let components = keeper.await.get_anonymous().await.unwrap();
        let pipeline = SubscriberPipeline::new(
            router(ZenohBucketRouting::KeyPrefix, None, &["bucket-1"], true),
            Arc::clone(&components),
        );

        let err = write(&pipeline, "/bucket-2/entry-y", 100)
            .await
            .unwrap_err();
        assert_eq!(
            expect_routing_error(err),
            RoutingError::NotAllowed {
                bucket: "bucket-2".to_string()
            }
        );

        let bucket = components
            .storage
            .get_bucket("bucket-2")
            .await
            .unwrap()
            .upgrade()
            .unwrap();
        assert!(bucket.get_entry("entry-y").await.is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn rejects_write_to_system_bucket(#[future] keeper: Arc<StateKeeper>) {
        let components = keeper.await.get_anonymous().await.unwrap();
        let _ = components
            .storage
            .create_system_bucket("$system", BucketSettings::default())
            .await;
        assert!(components.storage.get_bucket("$system").await.is_ok());

        let pipeline = SubscriberPipeline::new(
            router(ZenohBucketRouting::KeyPrefix, None, &["*"], true),
            Arc::clone(&components),
        );

        let err = write(&pipeline, "$system/evil/entry", 100)
            .await
            .unwrap_err();
        assert!(matches!(
            expect_routing_error(err),
            RoutingError::InvalidBucketName { .. }
        ));

        let bucket = components
            .storage
            .get_bucket("$system")
            .await
            .unwrap()
            .upgrade()
            .unwrap();
        assert!(bucket.get_entry("evil/entry").await.is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn ready_bucket_cache_is_per_block(#[future] keeper: Arc<StateKeeper>) {
        let components = keeper.await.get_anonymous().await.unwrap();

        let permissive = SubscriberPipeline::new(
            router(ZenohBucketRouting::KeyPrefix, None, &["bucket-*"], false),
            Arc::clone(&components),
        );
        write(&permissive, "/bucket-2/shared", 100).await.unwrap();

        let restricted = SubscriberPipeline::new(
            router(ZenohBucketRouting::KeyPrefix, None, &["bucket-1"], false),
            Arc::clone(&components),
        );
        let err = write(&restricted, "/bucket-2/shared", 101)
            .await
            .unwrap_err();
        assert_eq!(
            expect_routing_error(err),
            RoutingError::NotAllowed {
                bucket: "bucket-2".to_string()
            }
        );
    }

    #[rstest]
    #[tokio::test]
    async fn static_routing_keeps_full_key_as_entry(#[future] keeper: Arc<StateKeeper>) {
        let components = keeper.await.get_anonymous().await.unwrap();
        let pipeline = SubscriberPipeline::new(static_router(), Arc::clone(&components));

        write(&pipeline, "/factory/line1/status", 100)
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
        let pipeline = SubscriberPipeline::new(static_router(), components);

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

        let err = expect_storage_error(err);
        assert_eq!(err.status, ErrorCode::TooManyRequests);
        assert!(err.message.contains("ingress bytes"));
    }

    #[rstest]
    #[tokio::test]
    async fn handle_sample_rejects_api_request_over_limit(
        #[future] api_limited_keeper: Arc<StateKeeper>,
    ) {
        let components = api_limited_keeper.await.get_anonymous().await.unwrap();
        let pipeline = SubscriberPipeline::new(static_router(), components);

        assert!(write(&pipeline, "/entry-zenoh-api-limit", 101)
            .await
            .is_ok());

        let err = write(&pipeline, "/entry-zenoh-api-limit", 102)
            .await
            .unwrap_err();
        let err = expect_storage_error(err);
        assert_eq!(err.status, ErrorCode::TooManyRequests);
        assert!(err.message.contains("api requests"));
    }

    #[rstest]
    fn drop_counter_throttles_reports() {
        let counter = DropCounter::new();
        assert_eq!(counter.record(), Some(1));
        assert_eq!(counter.record(), None);
        assert_eq!(counter.record(), None);
    }
}
