// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::core::duration::parse_duration_to_micros;
use crate::lifecycle::action::progress;
use crate::lifecycle::action::{LifecycleAction, LifecycleContext, LifecycleRunResult};
use async_trait::async_trait;
use reduct_base::error::ReductError;
use reduct_base::msg::entry_api::{QueryEntry, QueryType};
use reduct_base::msg::lifecycle_api::{LifecycleMode, LifecycleSettings, LifecycleType};
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) struct DeleteLifecycleAction;

#[async_trait]
impl LifecycleAction for DeleteLifecycleAction {
    fn lifecycle_type(&self) -> LifecycleType {
        LifecycleType::Delete
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
        let query_entry = QueryEntry {
            query_type: if settings.mode == LifecycleMode::DryRun {
                QueryType::Query
            } else {
                QueryType::Remove
            },
            entries,
            // Use the oldest matching record, clamped to stop, to keep the
            // range valid even when an entry only contains fresher data.
            start: window.start,
            stop: window.stop,
            when: settings.when.clone(),
            ..Default::default()
        };

        let stats = if settings.mode == LifecycleMode::DryRun {
            bucket.query_count_records_with_stats(query_entry).await?
        } else {
            bucket.query_remove_records_with_stats(query_entry).await?
        };

        Ok(LifecycleRunResult {
            affected_records: stats.records,
            affected_blocks: Some(stats.blocks),
            last_processed_ts: window.last_processed_ts,
            caught_up: window.reaches_cutoff,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::action::progress;
    use crate::lifecycle::lifecycle_task::tests::{settings, storage};
    use crate::storage::bucket::tests::{write, write_meta};
    use crate::storage::bucket::Bucket;
    use crate::storage::engine::StorageEngine;
    use rstest::{fixture, rstest};
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

    #[fixture]
    fn action() -> DeleteLifecycleAction {
        DeleteLifecycleAction
    }

    #[rstest]
    #[tokio::test]
    async fn dry_run_delete_counts_without_removing(
        #[future] test_context: (Arc<StorageEngine>, Arc<Bucket>),
        action: DeleteLifecycleAction,
        mut settings: LifecycleSettings,
    ) {
        let (test_storage, test_bucket) = test_context.await;
        write(&test_bucket, "entry-1", 1, b"r1").await.unwrap();
        write(&test_bucket, "entry-1", 2, b"r2").await.unwrap();
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
        assert!(test_bucket.begin_read("entry-1", 1).await.is_ok());
        assert!(test_bucket.begin_read("entry-1", 2).await.is_ok());
    }

    #[tokio::test]
    #[rstest]
    async fn dry_run_delete_with_only_fresh_records_returns_zero(
        #[future] test_context: (Arc<StorageEngine>, Arc<Bucket>),
        action: DeleteLifecycleAction,
        mut settings: LifecycleSettings,
    ) {
        let (test_storage, test_bucket) = test_context.await;
        settings.mode = LifecycleMode::DryRun;

        let now_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        write(&test_bucket, "entry-1", now_us, b"fresh")
            .await
            .unwrap();

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
        assert!(test_bucket.begin_read("entry-1", now_us).await.is_ok());
    }

    #[tokio::test]
    #[rstest]
    async fn delete_lifecycle_keeps_meta_attachments(
        #[future] test_context: (Arc<StorageEngine>, Arc<Bucket>),
        action: DeleteLifecycleAction,
        mut settings: LifecycleSettings,
    ) {
        let (test_storage, test_bucket) = test_context.await;

        write_meta(&test_bucket, "entry-1/$meta", 1, br#"{"k":"v"}"#)
            .await
            .unwrap();
        write(&test_bucket, "entry-1", 1, b"data").await.unwrap();

        settings.mode = LifecycleMode::Enabled;
        settings.older_than = "0s".to_string();
        settings.entries = vec!["entry-1*".to_string()];

        let result = action
            .run(
                "test",
                &settings,
                LifecycleContext::new(test_storage, false, "unknown".to_string()),
            )
            .await
            .unwrap();

        assert_eq!(result.affected_records, 1);
        assert_eq!(result.affected_blocks, Some(1));
        assert!(test_bucket.begin_read("entry-1", 1).await.is_err());
        assert!(test_bucket.begin_read("entry-1/$meta", 1).await.is_ok());
    }

    #[tokio::test]
    #[rstest]
    async fn delete_processes_windowed_data_when_system_events_enabled(
        #[future] test_context: (Arc<StorageEngine>, Arc<Bucket>),
        action: DeleteLifecycleAction,
        mut settings: LifecycleSettings,
    ) {
        let (test_storage, test_bucket) = test_context.await;
        write(&test_bucket, "entry-1", 1, b"r1").await.unwrap();
        write(&test_bucket, "entry-1", 23_999_999, b"r2")
            .await
            .unwrap();
        write(&test_bucket, "entry-1", 24_000_000, b"r3")
            .await
            .unwrap();
        settings.mode = LifecycleMode::Enabled;
        settings.older_than = "0s".to_string();
        settings.interval = "1s".to_string();

        let result = action
            .run(
                "test",
                &settings,
                LifecycleContext::new(test_storage, true, "instance-1".to_string()),
            )
            .await
            .unwrap();

        assert_eq!(result.affected_records, 3);
        assert!(test_bucket.begin_read("entry-1", 1).await.is_err());
        assert!(test_bucket.begin_read("entry-1", 23_999_999).await.is_err());
        assert!(test_bucket.begin_read("entry-1", 24_000_000).await.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    #[rstest]
    async fn delete_resumes_from_last_progress(
        #[future] test_context: (Arc<StorageEngine>, Arc<Bucket>),
        action: DeleteLifecycleAction,
        mut settings: LifecycleSettings,
    ) {
        let (test_storage, test_bucket) = test_context.await;
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
        settings.mode = LifecycleMode::Enabled;
        settings.older_than = "0s".to_string();
        settings.interval = "1s".to_string();

        let result = action
            .run(
                "test",
                &settings,
                LifecycleContext::new(test_storage, true, "instance-1".to_string()),
            )
            .await
            .unwrap();

        assert_eq!(result.affected_records, 2);
        assert!(test_bucket.begin_read("entry-1", 23_999_999).await.is_err());
        assert!(test_bucket.begin_read("entry-1", 24_000_000).await.is_err());
        assert!(test_bucket.begin_read("entry-1", 48_000_000).await.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    #[rstest]
    async fn delete_still_processes_oldest_records_when_caught_up(
        #[future] test_context: (Arc<StorageEngine>, Arc<Bucket>),
        action: DeleteLifecycleAction,
        mut settings: LifecycleSettings,
    ) {
        let (test_storage, test_bucket) = test_context.await;
        progress::tests::write_lifecycle_stats(&test_storage, "instance-1", "test", u64::MAX)
            .await
            .unwrap();
        write(&test_bucket, "entry-1", 1, b"r1").await.unwrap();
        settings.mode = LifecycleMode::Enabled;
        settings.older_than = "0s".to_string();
        settings.interval = "1s".to_string();

        let result = action
            .run(
                "test",
                &settings,
                LifecycleContext::new(test_storage, true, "instance-1".to_string()),
            )
            .await
            .unwrap();

        assert_eq!(result.affected_records, 1);
        assert_eq!(result.affected_blocks, Some(1));
        assert!(result.caught_up);
        assert!(test_bucket.begin_read("entry-1", 1).await.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    #[rstest]
    async fn delete_marks_result_caught_up_when_progress_is_past_latest_data(
        #[future] test_context: (Arc<StorageEngine>, Arc<Bucket>),
        action: DeleteLifecycleAction,
        mut settings: LifecycleSettings,
    ) {
        let (test_storage, test_bucket) = test_context.await;
        let now_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        progress::tests::write_lifecycle_stats(&test_storage, "instance-1", "test", now_us)
            .await
            .unwrap();
        write(&test_bucket, "entry-1", 1, b"r1").await.unwrap();
        settings.mode = LifecycleMode::Enabled;
        settings.older_than = "0s".to_string();
        settings.interval = "1s".to_string();

        let result = action
            .run(
                "test",
                &settings,
                LifecycleContext::new(test_storage, true, "instance-1".to_string()),
            )
            .await
            .unwrap();

        assert_eq!(result.affected_records, 1);
        assert_eq!(result.affected_blocks, Some(1));
        assert!(result.caught_up);
    }

    #[tokio::test]
    #[rstest]
    async fn delete_processes_all_data_when_system_events_disabled(
        #[future] test_context: (Arc<StorageEngine>, Arc<Bucket>),
        action: DeleteLifecycleAction,
        mut settings: LifecycleSettings,
    ) {
        let (test_storage, test_bucket) = test_context.await;
        write(&test_bucket, "entry-1", 1, b"r1").await.unwrap();
        write(&test_bucket, "entry-1", 48_000_000, b"r2")
            .await
            .unwrap();
        settings.mode = LifecycleMode::Enabled;
        settings.older_than = "0s".to_string();
        settings.interval = "1s".to_string();

        let result = action
            .run(
                "test",
                &settings,
                LifecycleContext::new(test_storage, false, "unknown".to_string()),
            )
            .await
            .unwrap();

        assert_eq!(result.affected_records, 2);
        assert!(test_bucket.begin_read("entry-1", 1).await.is_err());
        assert!(test_bucket.begin_read("entry-1", 48_000_000).await.is_err());
    }
}
