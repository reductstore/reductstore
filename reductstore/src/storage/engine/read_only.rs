// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::cfg::{Cfg, InstanceRole};
use crate::storage::bucket::Bucket;
use crate::storage::engine::{ReadOnlyMode, StorageEngine};
use reduct_base::error::ReductError;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

impl ReadOnlyMode for StorageEngine {
    fn cfg(&self) -> &Cfg {
        &self.cfg
    }

    fn reload(&self) -> Result<(), ReductError> {
        let mut last_sync = self.last_replica_sync.write()?;
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
            .read()?
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
            match Bucket::restore(path.clone(), self.cfg.clone()) {
                Ok(bucket) => {
                    let bucket = Arc::new(bucket);
                    new_buckets.insert(bucket.name().to_string(), bucket);
                }
                Err(e) => {
                    panic!("Failed to load bucket from {:?}: {}", path, e);
                }
            }
        }

        let mut buckets = self.buckets.write()?;
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
    fn test_reload_new_bucket(primary_engine: StorageEngine) {
        // Create read-only engine
        let mut cfg = primary_engine.cfg().clone();
        cfg.role = InstanceRole::Replica;
        let read_only_engine = StorageEngine::builder()
            .with_cfg(cfg.clone())
            .with_data_path(cfg.data_path.clone())
            .build();

        // Initially, read-only engine has one bucket
        {
            let buckets = read_only_engine.buckets.read().unwrap();
            assert_eq!(buckets.len(), 1);
            assert!(buckets.contains_key("bucket-1"));
        }

        // Create new bucket in primary engine
        let _ = primary_engine
            .create_bucket("bucket-2", BucketSettings::default())
            .unwrap();
        read_only_engine.reload().unwrap();
        assert_eq!(
            read_only_engine.buckets.read().unwrap().len(),
            1,
            "Should not reload before interval"
        );

        // Wait for interval and reload
        std::thread::sleep(primary_engine.cfg.engine_config.replica_update_interval);
        read_only_engine.reload().unwrap();
        let buckets = read_only_engine.buckets.read().unwrap();
        assert_eq!(buckets.len(), 2);
        assert!(buckets.contains_key("bucket-1"));
        assert!(buckets.contains_key("bucket-2"));
    }

    #[rstest]
    #[serial]
    fn test_remove_bucket(primary_engine: StorageEngine) {
        let mut cfg = primary_engine.cfg().clone();
        cfg.role = InstanceRole::Replica;
        let read_only_engine = StorageEngine::builder()
            .with_cfg(cfg.clone())
            .with_data_path(cfg.data_path.clone())
            .build();

        {
            let buckets = read_only_engine.buckets.read().unwrap();
            assert_eq!(buckets.len(), 1);
            assert!(buckets.contains_key("bucket-1"));
        }

        // Remove bucket in primary engine (fire-and-forget) and wait briefly for completion
        primary_engine.remove_bucket("bucket-1").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(50));
        primary_engine.sync_fs().unwrap();
        read_only_engine.reload().unwrap();
        assert_eq!(
            read_only_engine.buckets.read().unwrap().len(),
            1,
            "Should not reload before interval"
        );

        std::thread::sleep(primary_engine.cfg.engine_config.replica_update_interval);
        read_only_engine.reload().unwrap();
        let buckets = read_only_engine.buckets.read().unwrap();
        assert_eq!(buckets.len(), 0);
    }

    mod forbidden {
        use super::*;
        use reduct_base::forbidden;
        #[rstest]
        #[serial]
        fn test_prohibited_operations_on_read_only_engine(primary_engine: StorageEngine) {
            let mut cfg = primary_engine.cfg().clone();
            cfg.role = InstanceRole::Replica;
            let read_only_engine = StorageEngine::builder()
                .with_cfg(cfg.clone())
                .with_data_path(cfg.data_path.clone())
                .build();
            let err = forbidden!("Cannot perform this operation in read-only mode");
            // Example: try to create bucket
            let result = read_only_engine.create_bucket("new-bucket", BucketSettings::default());
            assert_eq!(result.err().unwrap(), err);

            let result = read_only_engine.remove_bucket("bucket-1");
            assert_eq!(result.err().unwrap(), err);

            let result = read_only_engine
                .rename_bucket("bucket-1", "bucket-renamed")
                .wait();
            assert_eq!(result.err().unwrap(), err);
        }
    }

    mod reload_before {
        use super::*;
        #[rstest]
        #[serial]
        fn test_reload_before_access_buckets(primary_engine: StorageEngine) {
            let mut cfg = primary_engine.cfg().clone();
            cfg.role = InstanceRole::Replica;
            let read_only_engine = StorageEngine::builder()
                .with_cfg(cfg.clone())
                .with_data_path(cfg.data_path.clone())
                .build();
            {
                let buckets = read_only_engine.buckets.read().unwrap();
                assert_eq!(buckets.len(), 1);
                assert!(buckets.contains_key("bucket-1"));
            }
            // Add new bucket to primary engine
            let _ = primary_engine
                .create_bucket("bucket-2", BucketSettings::default())
                .unwrap();
            {
                std::thread::sleep(primary_engine.cfg.engine_config.replica_update_interval);
                let buckets = read_only_engine.buckets.read().unwrap();
                assert_eq!(buckets.len(), 1, "Should not reload before reload call");
            }
            read_only_engine.reload().unwrap();
            let buckets = read_only_engine.buckets.read().unwrap();
            assert_eq!(buckets.len(), 2);
        }
    }

    #[fixture]
    fn primary_engine() -> StorageEngine {
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
        let engine = StorageEngine::builder()
            .with_cfg(cfg.clone())
            .with_data_path(cfg.data_path.clone())
            .build();

        let _ = engine
            .create_bucket("bucket-1", BucketSettings::default())
            .unwrap();
        engine
    }
}
