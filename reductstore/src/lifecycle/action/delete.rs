// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::core::duration::parse_duration_to_micros;
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
        _name: &str,
        settings: &LifecycleSettings,
        context: LifecycleContext,
    ) -> Result<LifecycleRunResult, ReductError> {
        let max_age_us = parse_duration_to_micros(&settings.max_age)?;
        let now_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        let cutoff = now_us.saturating_sub(max_age_us.max(0) as u64);
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
        let query_entry = QueryEntry {
            query_type: if settings.mode == LifecycleMode::DryRun {
                QueryType::Query
            } else {
                QueryType::Remove
            },
            entries,
            // Use absolute range start to avoid invalid (start > stop) when
            // an entry contains only fresh records newer than `cutoff`.
            start: Some(0),
            stop,
            when: settings.when.clone(),
            ..Default::default()
        };

        let affected_records = if settings.mode == LifecycleMode::DryRun {
            bucket.query_count_records(query_entry).await?
        } else {
            bucket.query_remove_records(query_entry).await?
        };

        Ok(LifecycleRunResult { affected_records })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::lifecycle_task::tests::{settings, storage};
    use crate::storage::bucket::tests::write;
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
        settings.max_age = "0s".to_string();

        let result = action
            .run("test", &settings, LifecycleContext::new(test_storage))
            .await
            .unwrap();

        assert_eq!(result.affected_records, 2);
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
            .run("test", &settings, LifecycleContext::new(test_storage))
            .await
            .unwrap();

        assert_eq!(result.affected_records, 0);
        assert!(test_bucket.begin_read("entry-1", now_us).await.is_ok());
    }
}
