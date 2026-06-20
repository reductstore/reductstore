// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::core::duration::parse_duration_to_micros;
use crate::lifecycle::action::{LifecycleAction, LifecycleContext, LifecycleRunResult};
use async_trait::async_trait;
use reduct_base::error::ReductError;
use reduct_base::msg::lifecycle_api::{LifecycleMode, LifecycleSettings, LifecycleType};
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) struct CompressLifecycleAction;

#[async_trait]
impl LifecycleAction for CompressLifecycleAction {
    fn lifecycle_type(&self) -> LifecycleType {
        LifecycleType::Compress
    }

    async fn run(
        &self,
        _name: &str,
        settings: &LifecycleSettings,
        context: LifecycleContext,
    ) -> Result<LifecycleRunResult, ReductError> {
        let older_than_us = parse_duration_to_micros(&settings.older_than)?;
        let now_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        let cutoff = now_us.saturating_sub(older_than_us.max(0) as u64);
        let start = Some(0u64);
        let stop = Some(cutoff.saturating_add(1));
        let entries = if settings.entries.is_empty() {
            None
        } else {
            Some(settings.entries.clone())
        };

        let bucket = context
            .storage
            .get_bucket(&settings.bucket)
            .await?
            .upgrade()?;

        let stats = if settings.mode == LifecycleMode::DryRun {
            bucket
                .count_compressible_blocks(entries, start, stop)
                .await?
        } else {
            bucket.compress_blocks(entries, start, stop).await?
        };

        Ok(LifecycleRunResult {
            affected_records: stats.records,
            affected_blocks: Some(stats.blocks),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::Cfg;
    use crate::lifecycle::lifecycle_task::tests::{settings, storage};
    use crate::storage::bucket::tests::{write, write_meta};
    use crate::storage::bucket::Bucket;
    use crate::storage::engine::StorageEngine;
    use rstest::{fixture, rstest};
    use serial_test::serial;
    use std::sync::Arc;

    #[fixture]
    async fn test_context() -> (Arc<StorageEngine>, Arc<Bucket>) {
        let storage = storage().await;
        let bucket = storage
            .get_bucket("bucket-1")
            .await
            .unwrap()
            .upgrade()
            .unwrap();
        (storage, bucket)
    }

    async fn restore_storage(storage: &Arc<StorageEngine>) -> (Arc<StorageEngine>, Arc<Bucket>) {
        let cfg = Cfg {
            data_path: storage.data_path().clone(),
            ..Cfg::default()
        };
        let storage = Arc::new(
            StorageEngine::builder()
                .with_data_path(cfg.data_path.clone())
                .with_cfg(cfg)
                .build()
                .await,
        );
        let bucket = storage
            .get_bucket("bucket-1")
            .await
            .unwrap()
            .upgrade()
            .unwrap();
        (storage, bucket)
    }

    #[fixture]
    fn action() -> CompressLifecycleAction {
        CompressLifecycleAction
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn compress_lifecycle_compresses_old_blocks(
        #[future] test_context: (Arc<StorageEngine>, Arc<Bucket>),
        action: CompressLifecycleAction,
        mut settings: LifecycleSettings,
    ) {
        let (test_storage, test_bucket) = test_context.await;
        write(&test_bucket, "entry-1", 1, b"r1").await.unwrap();
        write(&test_bucket, "entry-1", 2, b"r2").await.unwrap();
        test_bucket.sync_fs().await.unwrap();
        let (test_storage, test_bucket) = restore_storage(&test_storage).await;
        settings.lifecycle_type = LifecycleType::Compress;
        settings.older_than = "0s".to_string();

        let result = action
            .run("test", &settings, LifecycleContext::new(test_storage))
            .await
            .unwrap();

        assert_eq!(result.affected_records, 2);
        assert_eq!(result.affected_blocks, Some(1));
        assert_eq!(
            test_bucket
                .clone()
                .count_compressible_blocks(Some(vec!["entry-1".into()]), None, None)
                .await
                .unwrap(),
            crate::storage::entry::CompressionStats::default()
        );
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn compress_lifecycle_dry_run_counts_without_compressing(
        #[future] test_context: (Arc<StorageEngine>, Arc<Bucket>),
        action: CompressLifecycleAction,
        mut settings: LifecycleSettings,
    ) {
        let (test_storage, test_bucket) = test_context.await;
        write(&test_bucket, "entry-1", 1, b"r1").await.unwrap();
        write(&test_bucket, "entry-1", 2, b"r2").await.unwrap();
        test_bucket.sync_fs().await.unwrap();
        let (test_storage, test_bucket) = restore_storage(&test_storage).await;
        settings.lifecycle_type = LifecycleType::Compress;
        settings.mode = LifecycleMode::DryRun;
        settings.older_than = "0s".to_string();

        let result = action
            .run("test", &settings, LifecycleContext::new(test_storage))
            .await
            .unwrap();

        assert_eq!(result.affected_records, 2);
        assert_eq!(result.affected_blocks, Some(1));
        assert_eq!(
            test_bucket
                .clone()
                .count_compressible_blocks(Some(vec!["entry-1".into()]), None, None)
                .await
                .unwrap(),
            crate::storage::entry::CompressionStats {
                blocks: 1,
                records: 2
            }
        );
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn compress_lifecycle_skips_already_compressed(
        #[future] test_context: (Arc<StorageEngine>, Arc<Bucket>),
        action: CompressLifecycleAction,
        mut settings: LifecycleSettings,
    ) {
        let (test_storage, test_bucket) = test_context.await;
        write(&test_bucket, "entry-1", 1, b"r1").await.unwrap();
        write(&test_bucket, "entry-1", 2, b"r2").await.unwrap();
        test_bucket.sync_fs().await.unwrap();
        let (test_storage, _) = restore_storage(&test_storage).await;
        settings.lifecycle_type = LifecycleType::Compress;
        settings.older_than = "0s".to_string();

        let context = LifecycleContext::new(test_storage);
        assert_eq!(
            action
                .run("test", &settings, context.clone())
                .await
                .unwrap()
                .affected_records,
            2
        );
        assert_eq!(
            action
                .run("test", &settings, context)
                .await
                .unwrap()
                .affected_records,
            0
        );
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn compress_lifecycle_with_empty_entries_compresses_all(
        #[future] test_context: (Arc<StorageEngine>, Arc<Bucket>),
        action: CompressLifecycleAction,
        mut settings: LifecycleSettings,
    ) {
        let (test_storage, test_bucket) = test_context.await;
        write(&test_bucket, "entry-1", 1, b"r1").await.unwrap();
        write(&test_bucket, "entry-1", 2, b"r2").await.unwrap();
        test_bucket.sync_fs().await.unwrap();
        let (test_storage, _) = restore_storage(&test_storage).await;
        settings.lifecycle_type = LifecycleType::Compress;
        settings.older_than = "0s".to_string();
        settings.entries = vec![];

        let result = action
            .run("test", &settings, LifecycleContext::new(test_storage))
            .await
            .unwrap();

        assert_eq!(result.affected_records, 2);
        assert_eq!(result.affected_blocks, Some(1));
    }

    #[rstest]
    fn compress_action_lifecycle_type(action: CompressLifecycleAction) {
        assert_eq!(action.lifecycle_type(), LifecycleType::Compress);
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn compress_lifecycle_skips_meta_entries(
        #[future] test_context: (Arc<StorageEngine>, Arc<Bucket>),
        action: CompressLifecycleAction,
        mut settings: LifecycleSettings,
    ) {
        let (test_storage, test_bucket) = test_context.await;
        write_meta(&test_bucket, "entry-1/$meta", 1, b"meta")
            .await
            .unwrap();
        test_bucket.sync_fs().await.unwrap();
        let (test_storage, test_bucket) = restore_storage(&test_storage).await;
        settings.lifecycle_type = LifecycleType::Compress;
        settings.older_than = "0s".to_string();
        settings.entries = vec!["entry-1/$meta".to_string()];

        let result = action
            .run("test", &settings, LifecycleContext::new(test_storage))
            .await
            .unwrap();

        assert_eq!(result.affected_records, 0);
        assert_eq!(result.affected_blocks, Some(0));
        assert!(test_bucket.begin_read("entry-1/$meta", 1).await.is_ok());
    }
}
