// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::cfg::{Cfg, InstanceRole};
use crate::storage::bucket::Bucket;
use crate::storage::engine::{ReadOnlyMode, StorageEngine};
use async_trait::async_trait;
use reduct_base::error::ReductError;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

#[async_trait]
impl ReadOnlyMode for StorageEngine {
    fn cfg(&self) -> &Cfg {
        &self.cfg
    }

    async fn reload(&self) -> Result<(), ReductError> {
        let mut last_sync = self.last_replica_sync.write().await?;
        if self.cfg.role != InstanceRole::Replica
            || last_sync.elapsed() < self.cfg.engine_config.replica_update_interval
        {
            // Only read-only instances need to update bucket list from backend
            return Ok(());
        }

        *last_sync = Instant::now();

        let mut new_buckets = BTreeMap::new();
        let current_bucket_paths = self
            .buckets
            .read()
            .await?
            .values()
            .map(|b| b.path().clone())
            .collect::<HashSet<_>>();

        let mut buckets_to_retain = vec![];
        self.folder_keeper.reload()?;
        for path in self.folder_keeper.list_folders()? {
            if current_bucket_paths.contains(&path) {
                buckets_to_retain.push(path);
                continue;
            }

            // Restore new bucket
            match Bucket::restore(path.clone(), self.cfg.clone()).await {
                Ok(bucket) => {
                    let bucket = Arc::new(bucket);
                    new_buckets.insert(bucket.name().to_string(), bucket);
                }
                Err(e) => {
                    panic!("Failed to load bucket from {:?}: {}", path, e);
                }
            }
        }

        let mut buckets = self.buckets.write().await?;
        buckets.retain(|_, b| buckets_to_retain.contains(&b.path()));
        buckets.extend(new_buckets);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Backend;
    use crate::cfg::storage_engine::StorageEngineConfig;
    use crate::core::file_cache::FILE_CACHE;
    use crate::storage::engine::StorageEngine;
    use reduct_base::msg::bucket_api::BucketSettings;
    use rstest::{fixture, rstest};
    use serial_test::serial;
    use tempfile::tempdir;

    #[rstest]
    #[serial]
    #[tokio::test]
    async fn test_reload_new_bucket(#[future] primary_engine: Arc<StorageEngine>) {
        let primary_engine = primary_engine.await;
        // Create read-only engine
        let mut cfg = primary_engine.cfg().clone();
        cfg.role = InstanceRole::Replica;
        let read_only_engine = Arc::new(
            StorageEngine::builder()
                .with_cfg(cfg.clone())
                .with_data_path(cfg.data_path.clone())
                .build()
                .await
                .unwrap(),
        );

        // Initially, read-only engine has one bucket
        {
            let buckets = read_only_engine.buckets.read().unwrap();
            assert_eq!(buckets.len(), 1);
            assert!(buckets.contains_key("bucket-1"));
        }

        // Create new bucket in primary engine
        let _ = primary_engine
            .create_bucket("bucket-2", BucketSettings::default())
            .await
            .unwrap();
        read_only_engine.reload().await.unwrap();
        assert_eq!(
            read_only_engine.buckets.read().unwrap().len(),
            1,
            "Should not reload before interval"
        );

        // Wait for interval and reload
        tokio::time::sleep(primary_engine.cfg.engine_config.replica_update_interval).await;
        read_only_engine.reload().await.unwrap();
        let buckets = read_only_engine.buckets.read().unwrap();
        assert_eq!(buckets.len(), 2);
        assert!(buckets.contains_key("bucket-1"));
        assert!(buckets.contains_key("bucket-2"));
    }

    #[rstest]
    #[serial]
    #[tokio::test]
    async fn test_remove_bucket(#[future] primary_engine: Arc<StorageEngine>) {
        let primary_engine = primary_engine.await;
        let mut cfg = primary_engine.cfg().clone();
        cfg.role = InstanceRole::Replica;
        let read_only_engine = Arc::new(
            StorageEngine::builder()
                .with_cfg(cfg.clone())
                .with_data_path(cfg.data_path.clone())
                .build()
                .await
                .unwrap(),
        );

        {
            let buckets = read_only_engine.buckets.read().unwrap();
            assert_eq!(buckets.len(), 1);
            assert!(buckets.contains_key("bucket-1"));
        }

        // Remove bucket in primary engine (fire-and-forget) and wait briefly for completion
        primary_engine.remove_bucket("bucket-1").await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        primary_engine.sync_fs().await.unwrap();
        read_only_engine.reload().await.unwrap();
        assert_eq!(
            read_only_engine.buckets.read().unwrap().len(),
            1,
            "Should not reload before interval"
        );

        tokio::time::sleep(primary_engine.cfg.engine_config.replica_update_interval).await;
        read_only_engine.reload().await.unwrap();
        let buckets = read_only_engine.buckets.read().unwrap();
        assert_eq!(buckets.len(), 0);
    }

    mod forbidden {
        use super::*;
        use reduct_base::forbidden;
        #[rstest]
        #[serial]
        #[tokio::test]
        async fn test_prohibited_operations_on_read_only_engine(
            #[future] primary_engine: Arc<StorageEngine>,
        ) {
            let primary_engine = primary_engine.await;
            let mut cfg = primary_engine.cfg().clone();
            cfg.role = InstanceRole::Replica;
            let read_only_engine = Arc::new(
                StorageEngine::builder()
                    .with_cfg(cfg.clone())
                    .with_data_path(cfg.data_path.clone())
                    .build()
                    .await
                    .unwrap(),
            );
            let err = forbidden!("Cannot perform this operation in read-only mode");
            // Example: try to create bucket
            let result = read_only_engine
                .create_bucket("new-bucket", BucketSettings::default())
                .await;
            assert_eq!(result.err().unwrap(), err);

            let result = read_only_engine.remove_bucket("bucket-1").await;
            assert_eq!(result.err().unwrap(), err);

            let result = read_only_engine
                .rename_bucket("bucket-1".to_string(), "bucket-renamed".to_string())
                .await;
            assert_eq!(result.err().unwrap(), err);
        }
    }

    mod reload_before {
        use super::*;
        #[rstest]
        #[serial]
        #[tokio::test]
        async fn test_reload_before_access_buckets(#[future] primary_engine: Arc<StorageEngine>) {
            let primary_engine = primary_engine.await;
            let mut cfg = primary_engine.cfg().clone();
            cfg.role = InstanceRole::Replica;
            let read_only_engine = Arc::new(
                StorageEngine::builder()
                    .with_cfg(cfg.clone())
                    .with_data_path(cfg.data_path.clone())
                    .build()
                    .await
                    .unwrap(),
            );
            {
                let buckets = read_only_engine.buckets.read().unwrap();
                assert_eq!(buckets.len(), 1);
                assert!(buckets.contains_key("bucket-1"));
            }
            // Add new bucket to primary engine
            let _ = primary_engine
                .create_bucket("bucket-2", BucketSettings::default())
                .await
                .unwrap();
            {
                tokio::time::sleep(primary_engine.cfg.engine_config.replica_update_interval).await;
                let buckets = read_only_engine.buckets.read().unwrap();
                assert_eq!(buckets.len(), 1, "Should not reload before reload call");
            }
            read_only_engine.reload().await.unwrap();
            let buckets = read_only_engine.buckets.read().unwrap();
            assert_eq!(buckets.len(), 2);
        }
    }

    #[fixture]
    pub async fn primary_engine() -> Arc<StorageEngine> {
        let path = tempdir().unwrap().keep();
        let cfg = Cfg {
            data_path: path,
            role: InstanceRole::Primary,
            engine_config: StorageEngineConfig {
                replica_update_interval: std::time::Duration::from_millis(500),
                ..StorageEngineConfig::default()
            },
            ..Cfg::default()
        };
        FILE_CACHE.set_storage_backend(
            Backend::builder()
                .local_data_path(cfg.data_path.clone())
                .try_build()
                .unwrap(),
        );
        let storage_engine = StorageEngine::builder()
            .with_cfg(cfg.clone())
            .with_data_path(cfg.data_path.clone())
            .build()
            .await
            .unwrap();

        let _ = storage_engine
            .create_bucket("bucket-1", BucketSettings::default())
            .await
            .unwrap();
        Arc::new(storage_engine)
    }
}
