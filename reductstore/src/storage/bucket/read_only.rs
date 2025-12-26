// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::cfg::{Cfg, InstanceRole};
use crate::storage::bucket::Bucket;
use crate::storage::engine::ReadOnlyMode;
use crate::storage::entry::{Entry, EntrySettings};
use reduct_base::error::ReductError;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

impl ReadOnlyMode for Bucket {
    fn cfg(&self) -> &Cfg {
        &self.cfg
    }

    /// List directory and update bucket list
    fn reload(&self) -> Result<(), ReductError> {
        let mut last_sync = self.last_replica_sync.write()?;
        if self.cfg().role != InstanceRole::Replica
            || last_sync.elapsed() < self.cfg.engine_config.replica_update_interval
        {
            // Only read-only instances need to update bucket list from backend
            return Ok(());
        }
        *last_sync = Instant::now();

        let mut task_set = vec![];

        let current_bucket_paths = self
            .entries
            .read()?
            .values()
            .map(|b| b.path().clone())
            .collect::<HashSet<_>>();

        let mut entries_to_retain = vec![];
        self.folder_keeper.reload()?;
        for path in self.folder_keeper.list_folders()? {
            if current_bucket_paths.contains(&path) {
                entries_to_retain.push(path);
                continue;
            }

            // Restore new bucket
            let settings = self.settings.read()?;
            let handler = Entry::restore(
                path,
                EntrySettings {
                    max_block_size: settings.max_block_size.unwrap(),
                    max_block_records: settings.max_block_records.unwrap(),
                },
                self.cfg.clone(),
            );

            task_set.push(handler);
        }

        let mut new_entries = BTreeMap::new();
        for task in task_set {
            if let Some(entry) = task.wait()? {
                new_entries.insert(entry.name().to_string(), Arc::new(entry));
            }
        }

        let mut entries = self.entries.write()?;
        entries.retain(|_, v| entries_to_retain.contains(v.path()));
        entries.extend(new_entries.into_iter());

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Backend;
    use crate::cfg::storage_engine::StorageEngineConfig;
    use crate::storage::bucket::tests::write;
    use crate::storage::bucket::FILE_CACHE;
    use futures::executor::block_on;
    use reduct_base::msg::bucket_api::BucketSettings;
    use rstest::{fixture, rstest};
    use tempfile::tempdir;

    #[rstest]
    #[tokio::test]
    async fn test_reload_new_entry(primary_bucket: Arc<Bucket>) {
        // Create read-only bucket
        let mut cfg = primary_bucket.cfg().clone();
        cfg.role = InstanceRole::Replica;
        let read_only_bucket =
            Arc::new(Bucket::restore(primary_bucket.path().clone(), cfg.clone()).unwrap());

        // Initially, read-only bucket has one entry
        {
            let entries = read_only_bucket.entries.read().unwrap();
            assert_eq!(entries.len(), 1);
            assert!(entries.contains_key("test-1"));
        }

        // Create new entry in primary bucket
        write(&primary_bucket, "test-2", 1, b"test data")
            .await
            .unwrap();

        primary_bucket.sync_fs().await.unwrap();
        read_only_bucket.reload().unwrap();

        assert_eq!(
            read_only_bucket.entries.read().unwrap().len(),
            1,
            "Should not reload before interval"
        );

        // Reload read-only bucket
        tokio::time::sleep(cfg.engine_config.replica_update_interval * 2).await;
        read_only_bucket.reload().unwrap();

        // Now, read-only bucket should have two entries
        {
            let entries = read_only_bucket.entries.read().unwrap();
            assert_eq!(entries.len(), 2);
            assert!(entries.contains_key("test-1"));
            assert!(entries.contains_key("test-2"));
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_entry(primary_bucket: Arc<Bucket>) {
        // Create read-only bucket
        let mut cfg = primary_bucket.cfg().clone();
        cfg.role = InstanceRole::Replica;
        let read_only_bucket =
            Arc::new(Bucket::restore(primary_bucket.path().clone(), cfg.clone()).unwrap());

        // Initially, read-only bucket has one entry
        {
            let entries = read_only_bucket.entries.read().unwrap();
            assert_eq!(entries.len(), 1);
            assert!(entries.contains_key("test-1"));
        }

        // Remove entry in primary bucket
        primary_bucket.remove_entry("test-1").unwrap();
        primary_bucket.sync_fs().wait().unwrap();
        read_only_bucket.reload().unwrap();

        assert_eq!(
            read_only_bucket.entries.read().unwrap().len(),
            1,
            "Should not reload before interval"
        );

        // Reload read-only bucket
        tokio::time::sleep(cfg.engine_config.replica_update_interval).await;
        read_only_bucket.reload().unwrap();

        // Now, read-only bucket should have zero entries
        {
            let entries = read_only_bucket.entries.read().unwrap();
            assert_eq!(entries.len(), 0);
        }
    }
    mod forbidden {
        use super::*;
        use reduct_base::forbidden;
        use rstest::rstest;

        #[rstest]
        #[tokio::test]
        async fn test_prohibited_operations_on_read_only_bucket(primary_bucket: Arc<Bucket>) {
            // Create read-only bucket
            let mut cfg = primary_bucket.cfg().clone();
            cfg.role = InstanceRole::Replica;
            let read_only_bucket =
                Arc::new(Bucket::restore(primary_bucket.path().clone(), cfg.clone()).unwrap());

            let err = forbidden!("Cannot perform this operation in read-only mode");

            // Attempt to perform prohibited operations
            let write_result = read_only_bucket.get_or_create_entry("new-entry");
            assert_eq!(write_result.err().unwrap(), err);

            let remove_result = read_only_bucket.remove_entry("test-1");
            assert_eq!(remove_result.err().unwrap(), err);

            let compact_result = read_only_bucket.rename_entry("test-1", "new-name").await;
            assert_eq!(compact_result.err().unwrap(), err);
        }
    }

    mod reload_before {
        use super::*;
        use rstest::rstest;

        #[rstest]
        #[tokio::test]
        async fn test_reload_before_access_entries(primary_bucket: Arc<Bucket>) {
            let mut cfg = primary_bucket.cfg().clone();
            cfg.role = InstanceRole::Replica;
            let read_only_bucket =
                Arc::new(Bucket::restore(primary_bucket.path().clone(), cfg.clone()).unwrap());

            {
                let entries = read_only_bucket.entries.read().unwrap();
                assert_eq!(entries.len(), 1);
                assert!(entries.contains_key("test-1"));
            }

            // add new entry to primary bucket
            write(&primary_bucket, "test-2", 1, b"test data")
                .await
                .unwrap();
            primary_bucket.sync_fs().await.unwrap();

            tokio::time::sleep(cfg.engine_config.replica_update_interval).await;
            {
                let entries = read_only_bucket.info().wait().unwrap().entries;
                assert_eq!(entries.len(), 2);
            }
        }
    }

    #[fixture]
    fn primary_bucket() -> Arc<Bucket> {
        let path = tempdir().unwrap().keep();
        let mut cfg = Cfg {
            data_path: path,
            role: InstanceRole::Primary,
            engine_config: StorageEngineConfig {
                replica_update_interval: std::time::Duration::from_millis(300),
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

        cfg.role = InstanceRole::Primary;

        FILE_CACHE
            .create_dir_all(&cfg.data_path.join("bucket"))
            .unwrap();
        let bucket = Arc::new(
            Bucket::new(
                "bucket",
                &cfg.data_path.clone(),
                BucketSettings::default(),
                cfg,
            )
            .unwrap(),
        );
        block_on(write(&bucket, "test-1", 1, b"test data")).unwrap();
        bucket.sync_fs().wait().unwrap();
        bucket
    }
}
