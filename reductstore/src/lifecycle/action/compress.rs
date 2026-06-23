// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::core::duration::parse_duration_to_micros;
use crate::lifecycle::action::progress;
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
        name: &str,
        settings: &LifecycleSettings,
        context: LifecycleContext,
    ) -> Result<LifecycleRunResult, ReductError> {
        let older_than_us = parse_duration_to_micros(&settings.older_than)?;
        let now_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        let cutoff = now_us.saturating_sub(older_than_us.max(0) as u64);
        let cutoff_stop = cutoff.saturating_add(1);
        let window = progress::processing_window(settings, &context, name, cutoff_stop).await?;
        if window.caught_up {
            return Ok(LifecycleRunResult {
                affected_records: 0,
                affected_blocks: Some(0),
                last_processed_ts: window.last_processed_ts,
            });
        }
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
                .estimate_compressible_data(entries, window.start, window.stop)
                .await?
        } else {
            bucket
                .compress_blocks(entries, window.start, window.stop)
                .await?
        };

        Ok(LifecycleRunResult {
            affected_records: stats.records,
            affected_blocks: Some(stats.blocks),
            last_processed_ts: window.last_processed_ts,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::Cfg;
    use crate::lifecycle::action::progress;
    use crate::lifecycle::lifecycle_task::tests::{settings, settings_fixture, storage};
    use crate::storage::bucket::tests::{write, write_meta};
    use crate::storage::bucket::Bucket;
    use crate::storage::engine::StorageEngine;
    use reduct_base::msg::bucket_api::BucketSettings;
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

    async fn test_context_with_one_record_blocks() -> (Arc<StorageEngine>, Arc<Bucket>) {
        let storage = storage().await;
        let bucket = storage
            .get_bucket("bucket-1")
            .await
            .unwrap()
            .upgrade()
            .unwrap();
        bucket
            .set_settings(BucketSettings {
                max_block_records: Some(1),
                ..BucketSettings::default()
            })
            .await
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
            .run(
                "test",
                &settings,
                LifecycleContext::new(test_storage, false, "unknown".to_string()),
            )
            .await
            .unwrap();

        assert_eq!(result.affected_records, 2);
        assert_eq!(result.affected_blocks, Some(1));
        assert_eq!(
            test_bucket
                .clone()
                .estimate_compressible_data(Some(vec!["entry-1".into()]), None, None)
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
            .run(
                "test",
                &settings,
                LifecycleContext::new(test_storage, false, "unknown".to_string()),
            )
            .await
            .unwrap();

        assert_eq!(result.affected_records, 2);
        assert_eq!(result.affected_blocks, Some(1));
        assert_eq!(
            test_bucket
                .clone()
                .estimate_compressible_data(Some(vec!["entry-1".into()]), None, None)
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

        let context = LifecycleContext::new(test_storage, false, "unknown".to_string());
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
            .run(
                "test",
                &settings,
                LifecycleContext::new(test_storage, false, "unknown".to_string()),
            )
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
            .run(
                "test",
                &settings,
                LifecycleContext::new(test_storage, false, "unknown".to_string()),
            )
            .await
            .unwrap();

        assert_eq!(result.affected_records, 0);
        assert_eq!(result.affected_blocks, Some(0));
        assert!(test_bucket.begin_read("entry-1/$meta", 1).await.is_ok());
    }

    #[tokio::test]
    #[serial]
    async fn compress_processes_windowed_data_when_system_events_enabled() {
        let (test_storage, test_bucket) = test_context_with_one_record_blocks().await;
        write(&test_bucket, "entry-1", 1, b"r1").await.unwrap();
        write(&test_bucket, "entry-1", 23_999_999, b"r2")
            .await
            .unwrap();
        write(&test_bucket, "entry-1", 24_000_000, b"r3")
            .await
            .unwrap();
        test_bucket.sync_fs().await.unwrap();
        let (test_storage, test_bucket) = restore_storage(&test_storage).await;
        let mut settings = settings_fixture();
        settings.lifecycle_type = LifecycleType::Compress;
        settings.older_than = "0s".to_string();
        settings.interval = "1s".to_string();

        let result = CompressLifecycleAction
            .run(
                "test",
                &settings,
                LifecycleContext::new(test_storage, true, "instance-1".to_string()),
            )
            .await
            .unwrap();

        assert_eq!(result.affected_records, 3);
        assert_eq!(result.affected_blocks, Some(3));
        assert_eq!(
            test_bucket
                .estimate_compressible_data(Some(vec!["entry-1".into()]), None, None)
                .await
                .unwrap()
                .records,
            0
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn compress_resumes_from_last_progress() {
        let (test_storage, test_bucket) = test_context_with_one_record_blocks().await;
        progress::tests::write_lifecycle_stats(&test_storage, "instance-1", "test", 24_000_000)
            .await
            .unwrap();
        write(&test_bucket, "entry-1", 23_999_999, b"r1")
            .await
            .unwrap();
        write(&test_bucket, "entry-1", 24_000_000, b"r2")
            .await
            .unwrap();
        write(&test_bucket, "entry-1", 48_000_000, b"r3")
            .await
            .unwrap();
        test_bucket.sync_fs().await.unwrap();
        let (test_storage, test_bucket) = restore_storage(&test_storage).await;
        let mut settings = settings_fixture();
        settings.lifecycle_type = LifecycleType::Compress;
        settings.older_than = "0s".to_string();
        settings.interval = "1s".to_string();

        let result = CompressLifecycleAction
            .run(
                "test",
                &settings,
                LifecycleContext::new(test_storage, true, "instance-1".to_string()),
            )
            .await
            .unwrap();

        assert_eq!(result.affected_records, 2);
        assert_eq!(result.affected_blocks, Some(2));
        assert_eq!(
            test_bucket
                .estimate_compressible_data(Some(vec!["entry-1".into()]), None, None)
                .await
                .unwrap()
                .records,
            1
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn compress_returns_zero_when_caught_up() {
        let (test_storage, test_bucket) = test_context_with_one_record_blocks().await;
        progress::tests::write_lifecycle_stats(&test_storage, "instance-1", "test", u64::MAX)
            .await
            .unwrap();
        write(&test_bucket, "entry-1", 1, b"r1").await.unwrap();
        test_bucket.sync_fs().await.unwrap();
        let (test_storage, test_bucket) = restore_storage(&test_storage).await;
        let mut settings = settings_fixture();
        settings.lifecycle_type = LifecycleType::Compress;
        settings.older_than = "0s".to_string();
        settings.interval = "1s".to_string();

        let result = CompressLifecycleAction
            .run(
                "test",
                &settings,
                LifecycleContext::new(test_storage, true, "instance-1".to_string()),
            )
            .await
            .unwrap();

        assert_eq!(result.affected_records, 0);
        assert_eq!(result.affected_blocks, Some(0));
        assert_eq!(
            test_bucket
                .estimate_compressible_data(Some(vec!["entry-1".into()]), None, None)
                .await
                .unwrap()
                .records,
            1
        );
    }

    #[tokio::test]
    #[serial]
    async fn compress_processes_all_data_when_system_events_disabled() {
        let (test_storage, test_bucket) = test_context_with_one_record_blocks().await;
        write(&test_bucket, "entry-1", 1, b"r1").await.unwrap();
        write(&test_bucket, "entry-1", 48_000_000, b"r2")
            .await
            .unwrap();
        test_bucket.sync_fs().await.unwrap();
        let (test_storage, test_bucket) = restore_storage(&test_storage).await;
        let mut settings = settings_fixture();
        settings.lifecycle_type = LifecycleType::Compress;
        settings.older_than = "0s".to_string();
        settings.interval = "1s".to_string();

        let result = CompressLifecycleAction
            .run(
                "test",
                &settings,
                LifecycleContext::new(test_storage, false, "unknown".to_string()),
            )
            .await
            .unwrap();

        assert_eq!(result.affected_records, 2);
        assert_eq!(result.affected_blocks, Some(2));
        assert_eq!(
            test_bucket
                .estimate_compressible_data(Some(vec!["entry-1".into()]), None, None)
                .await
                .unwrap()
                .records,
            0
        );
    }
}
