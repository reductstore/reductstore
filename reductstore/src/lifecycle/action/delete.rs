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
            start: None,
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
    use crate::cfg::Cfg;
    use crate::storage::bucket::Bucket;
    use crate::storage::engine::StorageEngine;
    use bytes::Bytes;
    use reduct_base::error::ReductError;
    use reduct_base::internal_server_error;
    use reduct_base::msg::bucket_api::BucketSettings;
    use reduct_base::Labels;
    use std::sync::Arc;

    #[tokio::test]
    async fn dry_run_delete_counts_without_removing() {
        let storage = storage().await;
        let bucket = storage
            .get_bucket("bucket-1")
            .await
            .unwrap()
            .upgrade()
            .unwrap();
        write(&bucket, "entry-1", 1, b"r1").await.unwrap();
        write(&bucket, "entry-1", 2, b"r2").await.unwrap();

        let settings = LifecycleSettings {
            lifecycle_type: LifecycleType::Delete,
            bucket: "bucket-1".to_string(),
            entries: vec!["entry-1".to_string()],
            max_age: "0s".to_string(),
            interval: "1h".to_string(),
            when: None,
            mode: LifecycleMode::DryRun,
        };

        let action = DeleteLifecycleAction;
        let result = action
            .run(
                "test",
                &settings,
                LifecycleContext::new(Arc::clone(&storage)),
            )
            .await
            .unwrap();

        assert_eq!(result.affected_records, 2);
        assert!(bucket.begin_read("entry-1", 1).await.is_ok());
        assert!(bucket.begin_read("entry-1", 2).await.is_ok());
    }

    async fn storage() -> Arc<StorageEngine> {
        let tmp_dir = tempfile::tempdir().unwrap();
        let cfg = Cfg {
            data_path: tmp_dir.keep(),
            ..Cfg::default()
        };

        let storage = StorageEngine::builder()
            .with_data_path(cfg.data_path.clone())
            .with_cfg(cfg)
            .build()
            .await;
        storage
            .create_bucket("bucket-1", BucketSettings::default())
            .await
            .unwrap();
        Arc::new(storage)
    }

    async fn write(
        bucket: &Arc<Bucket>,
        entry_name: &str,
        time: u64,
        content: &'static [u8],
    ) -> Result<(), ReductError> {
        let mut sender = bucket
            .begin_write(
                entry_name,
                time,
                content.len() as u64,
                "".to_string(),
                Labels::new(),
            )
            .await?;
        sender
            .send(Ok(Some(Bytes::from(content))))
            .await
            .map_err(|e| internal_server_error!("Failed to send data: {}", e))?;
        sender
            .send(Ok(None))
            .await
            .map_err(|e| internal_server_error!("Failed to sync channel: {}", e))?;
        Ok(())
    }
}
