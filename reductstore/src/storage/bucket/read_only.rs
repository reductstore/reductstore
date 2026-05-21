// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use super::{settings_for_entry, Bucket};
use crate::storage::entry::Entry;
use log::error;
use reduct_base::error::ReductError;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

fn normalize_entry_name(path: &std::path::Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

impl Bucket {
    /// List directory and update bucket list
    pub(in crate::storage) async fn reload(&self) -> Result<(), ReductError> {
        // Replica reloading is driven by the launcher background task.
        Ok(())
    }

    pub(crate) async fn reload_entries(&self) -> Result<(), ReductError> {
        let current_entry_paths = self
            .entries
            .read()
            .await?
            .values()
            .map(|entry| entry.path().clone())
            .collect::<HashSet<_>>();

        self.folder_keeper.reload().await?;
        let all_paths = self.folder_keeper.list_folders().await?;
        let settings = self.settings.read().await?.clone();

        let mut entries_to_retain = vec![];
        let mut task_set = vec![];
        for entry_path in all_paths {
            if current_entry_paths.contains(&entry_path) {
                entries_to_retain.push(entry_path);
                continue;
            }

            let entry_name = normalize_entry_name(
                entry_path
                    .strip_prefix(self.path())
                    .unwrap_or(entry_path.as_path()),
            );
            let handler = Entry::restore_with_limiter(
                entry_path,
                entry_name.clone(),
                self.name().to_string(),
                settings_for_entry(&entry_name, &settings),
                self.cfg.clone(),
                self.io_limiter.clone(),
            );

            task_set.push(handler);
        }

        let mut new_entries = BTreeMap::new();
        for task in task_set {
            if let Some(entry) = task.await? {
                new_entries.insert(entry.name().to_string(), Arc::new(entry));
            }
        }

        let entry_snapshot = {
            let mut entries = self.entries.write().await?;
            entries.retain(|_, entry| entries_to_retain.contains(entry.path()));
            entries.extend(new_entries);
            entries.values().cloned().collect::<Vec<_>>()
        };

        for entry in entry_snapshot {
            if let Err(err) = entry.reload_index_on_replica().await {
                error!(
                    "Failed to reload replica index for entry '{}' in bucket '{}': {}",
                    entry.name(),
                    self.name(),
                    err
                );
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::cfg::storage_engine::StorageEngineConfig;
    use crate::cfg::Cfg;
    use crate::cfg::InstanceRole;
    use crate::storage::bucket::tests::write;
    use crate::storage::bucket::FILE_CACHE;
    use reduct_base::msg::bucket_api::BucketSettings;
    use rstest::{fixture, rstest};
    use tempfile::tempdir;

    #[rstest]
    #[tokio::test]
    async fn test_reload_new_entry(#[future] primary_bucket: Arc<Bucket>) {
        let primary_bucket = primary_bucket.await;
        // Create read-only bucket
        let mut cfg = primary_bucket.cfg().clone();
        cfg.role = InstanceRole::Replica;
        let read_only_bucket = Arc::new(
            Bucket::restore(primary_bucket.path().clone(), cfg.clone())
                .await
                .unwrap(),
        );
        // Initially, read-only bucket has one entry
        {
            let entries = read_only_bucket.entries.read().await.unwrap();
            assert_eq!(entries.len(), 1);
            assert!(entries.contains_key("test-1"));
        }

        // Create new entry in primary bucket
        write(&primary_bucket, "test-2", 1, b"test data")
            .await
            .unwrap();

        primary_bucket.sync_fs().await.unwrap();
        read_only_bucket.reload().await.unwrap();

        assert_eq!(
            read_only_bucket.entries.read().await.unwrap().len(),
            1,
            "reload() should not refresh replica entries inline"
        );

        // Reload read-only bucket
        read_only_bucket.reload_entries().await.unwrap();

        // Now, read-only bucket should have two entries
        {
            let entries = read_only_bucket.entries.read().await.unwrap();
            assert_eq!(entries.len(), 2);
            assert!(entries.contains_key("test-1"));
            assert!(entries.contains_key("test-2"));
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_entry(#[future] primary_bucket: Arc<Bucket>) {
        let primary_bucket = primary_bucket.await;
        // Create read-only bucket
        let mut cfg = primary_bucket.cfg().clone();
        cfg.role = InstanceRole::Replica;
        let read_only_bucket = Arc::new(
            Bucket::restore(primary_bucket.path().clone(), cfg.clone())
                .await
                .unwrap(),
        );
        // Initially, read-only bucket has one entry
        {
            let entries = read_only_bucket.entries.read().await.unwrap();
            assert_eq!(entries.len(), 1);
            assert!(entries.contains_key("test-1"));
        }

        // Remove entry in primary bucket
        primary_bucket.remove_entry("test-1").await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        primary_bucket.sync_fs().await.unwrap();
        read_only_bucket.reload().await.unwrap();

        assert_eq!(
            read_only_bucket.entries.read().await.unwrap().len(),
            1,
            "reload() should not refresh replica entries inline"
        );

        // Reload read-only bucket
        read_only_bucket.reload_entries().await.unwrap();

        // Now, read-only bucket should have zero entries
        {
            let entries = read_only_bucket.entries.read().await.unwrap();
            assert_eq!(entries.len(), 0);
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_reload_updates_existing_entry_index(#[future] primary_bucket: Arc<Bucket>) {
        let primary_bucket = primary_bucket.await;
        let mut cfg = primary_bucket.cfg().clone();
        cfg.role = InstanceRole::Replica;
        let read_only_bucket = Arc::new(
            Bucket::restore(primary_bucket.path().clone(), cfg.clone())
                .await
                .unwrap(),
        );

        write(&primary_bucket, "test-1", 2, b"new data")
            .await
            .unwrap();
        primary_bucket.sync_fs().await.unwrap();

        let info = read_only_bucket.clone().info().await.unwrap();
        let entry = info
            .entries
            .iter()
            .find(|entry| entry.name == "test-1")
            .unwrap();
        assert_eq!(entry.record_count, 1);

        read_only_bucket.reload_entries().await.unwrap();

        let info = read_only_bucket.clone().info().await.unwrap();
        let entry = info
            .entries
            .iter()
            .find(|entry| entry.name == "test-1")
            .unwrap();
        assert_eq!(entry.record_count, 2);
    }

    mod forbidden {
        use super::*;
        use reduct_base::forbidden;
        use rstest::rstest;

        #[rstest]
        #[tokio::test]
        async fn test_prohibited_operations_on_read_only_bucket(
            #[future] primary_bucket: Arc<Bucket>,
        ) {
            let primary_bucket = primary_bucket.await;
            // Create read-only bucket
            let mut cfg = primary_bucket.cfg().clone();
            cfg.role = InstanceRole::Replica;
            let read_only_bucket = Arc::new(
                Bucket::restore(primary_bucket.path().clone(), cfg.clone())
                    .await
                    .unwrap(),
            );

            let err = forbidden!("Cannot perform this operation in read-only mode");

            // Attempt to perform prohibited operations
            let write_result = read_only_bucket.get_or_create_entry("new-entry").await;
            assert_eq!(write_result.err().unwrap(), err);

            let remove_result = read_only_bucket.remove_entry("test-1").await;
            assert_eq!(remove_result.err().unwrap(), err);

            let compact_result = read_only_bucket.rename_entry("test-1", "new-name").await;
            assert_eq!(compact_result.err().unwrap(), err);
        }

        #[rstest]
        #[tokio::test]
        async fn test_maintenance_operations_are_noop_on_replica(
            #[future] primary_bucket: Arc<Bucket>,
        ) {
            let primary_bucket = primary_bucket.await;
            let mut cfg = primary_bucket.cfg().clone();
            cfg.role = InstanceRole::Replica;
            let read_only_bucket = Arc::new(
                Bucket::restore(primary_bucket.path().clone(), cfg.clone())
                    .await
                    .unwrap(),
            );

            read_only_bucket.compact().await.unwrap();
            read_only_bucket.sync_fs().await.unwrap();
        }
    }

    mod reload_before {
        use super::*;
        use rstest::rstest;

        #[rstest]
        #[tokio::test]
        async fn test_info_does_not_reload_entries_inline(#[future] primary_bucket: Arc<Bucket>) {
            let primary_bucket = primary_bucket.await;
            let mut cfg = primary_bucket.cfg().clone();
            cfg.role = InstanceRole::Replica;
            let read_only_bucket = Arc::new(
                Bucket::restore(primary_bucket.path().clone(), cfg.clone())
                    .await
                    .unwrap(),
            );

            {
                let entries = read_only_bucket.entries.read().await.unwrap();
                assert_eq!(entries.len(), 1);
                assert!(entries.contains_key("test-1"));
            }

            // add new entry to primary bucket
            write(&primary_bucket, "test-2", 1, b"test data")
                .await
                .unwrap();
            primary_bucket.sync_fs().await.unwrap();

            {
                let entries = read_only_bucket.clone().info().await.unwrap().entries;
                assert_eq!(entries.len(), 1);
            }

            read_only_bucket.reload_entries().await.unwrap();
            let entries = read_only_bucket.clone().info().await.unwrap().entries;
            assert_eq!(entries.len(), 2);
        }

        #[rstest]
        #[tokio::test]
        async fn test_get_entry_does_not_wait_for_replica_reload(
            #[future] primary_bucket: Arc<Bucket>,
        ) {
            let primary_bucket = primary_bucket.await;
            let mut cfg = primary_bucket.cfg().clone();
            cfg.role = InstanceRole::Replica;
            let read_only_bucket = Arc::new(
                Bucket::restore(primary_bucket.path().clone(), cfg.clone())
                    .await
                    .unwrap(),
            );

            write(&primary_bucket, "test-2", 1, b"test data")
                .await
                .unwrap();
            primary_bucket.sync_fs().await.unwrap();

            let reloader = {
                let read_only_bucket = read_only_bucket.clone();
                tokio::spawn(async move { read_only_bucket.reload_entries().await })
            };

            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                read_only_bucket.get_entry("test-1"),
            )
            .await
            .expect("get_entry should not wait for replica entry restoration")
            .unwrap();

            reloader.await.unwrap().unwrap();
        }
    }

    #[fixture]
    pub async fn primary_bucket() -> Arc<Bucket> {
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

        cfg.role = InstanceRole::Primary;

        FILE_CACHE
            .create_dir_all(&cfg.data_path.join("bucket"))
            .await
            .unwrap();
        let bucket = Arc::new(
            Bucket::try_build(
                "bucket",
                &cfg.data_path.clone(),
                BucketSettings::default(),
                cfg,
            )
            .await
            .unwrap(),
        );
        write(&bucket, "test-1", 1, b"test data").await.unwrap();
        bucket.sync_fs().await.unwrap();
        bucket
    }
}
