// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use super::Bucket;
use crate::cfg::Cfg;
use crate::core::file_cache::FILE_CACHE;
use crate::core::sync::AsyncRwLock;
use crate::storage::entry::is_system_meta_entry;
use crate::storage::entry::Entry;
use crate::storage::in_flight::InFlightIoLimiter;
use crate::storage::usage::UsageCounters;
use log::{debug, error, warn};
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::msg::status::ResourceStatus;
use reduct_base::{conflict, not_found};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

const REMOVE_ENTRY_MAP_RETRIES: usize = 3;
const REMOVE_ENTRY_MAP_RETRY_DELAY: Duration = Duration::from_millis(50);

fn sort_entries_for_removal(entries: &mut [(String, Arc<Entry>)]) {
    // Remove system meta entries first to avoid removing a parent folder
    // before its `$meta` folder.
    entries.sort_by(|(a, _), (b, _)| {
        let a_meta = is_system_meta_entry(a);
        let b_meta = is_system_meta_entry(b);
        let a_depth = a.split('/').count();
        let b_depth = b.split('/').count();
        b_meta
            .cmp(&a_meta)
            .then_with(|| b_depth.cmp(&a_depth))
            .then_with(|| b.cmp(a))
    });
}

impl Bucket {
    /// Remove entry from the bucket
    ///
    /// # Arguments
    ///
    /// * `name` - Entry name.
    ///
    /// # Returns
    ///
    /// * `HTTPError` - The error if any.
    pub async fn remove_entry(&self, name: &str) -> Result<(), ReductError> {
        self.ensure_not_deleting().await?;
        self.check_mode()?;
        if is_system_meta_entry(name) {
            return Err(conflict!(
                "System entry '{}' can be removed only with its parent entry",
                name
            ));
        }

        let mut entries_to_remove: Vec<(String, Arc<Entry>)> = {
            let entries = self.entries.read().await?;
            entries
                .iter()
                .filter(|(entry_name, _)| {
                    *entry_name == name || entry_name.starts_with(&format!("{name}/"))
                })
                .map(|(entry_name, entry)| (entry_name.clone(), Arc::clone(entry)))
                .collect()
        };

        if entries_to_remove.is_empty() {
            return Err(not_found!(&format!(
                "Entry '{}' not found in bucket '{}'",
                name, self.name
            )));
        }

        sort_entries_for_removal(&mut entries_to_remove);

        for (_, entry) in &entries_to_remove {
            entry.ensure_not_deleting().await?;
            entry.mark_deleting().await?;
        }

        let entries_map = self.entries.clone();
        let bucket_name = self.name.clone();
        let folder_keeper = self.folder_keeper.clone();
        let cfg = self.cfg.clone();
        let io_limiter = self.io_limiter.clone();
        let usage_counters = self.usage_counters.clone();

        tokio::spawn(async move {
            for (entry_name, entry) in entries_to_remove {
                let path = entry.path().to_path_buf();

                match FILE_CACHE.try_exists(&path).await {
                    Ok(true) => {
                        if let Err(err) = entry.remove_all_blocks().await {
                            warn!(
                                "Failed to remove blocks for entry '{}' in bucket '{}', trying folder cleanup: {}",
                                entry_name, bucket_name, err
                            );
                        }
                    }
                    Ok(false) => {
                        debug!(
                            "Skip removing blocks for entry '{}' in bucket '{}' because folder '{}' is already missing",
                            entry_name,
                            bucket_name,
                            path.display()
                        );
                    }
                    Err(err) => {
                        warn!(
                            "Failed to check folder for entry '{}' in bucket '{}', trying folder cleanup: {}",
                            entry_name, bucket_name, err
                        );
                    }
                }

                let folder_removed = match folder_keeper.remove_folder(&entry_name).await {
                    Ok(()) => true,
                    Err(err) if err.status() == ErrorCode::NotFound => true,
                    Err(err) => {
                        error!(
                            "Failed to remove folder for entry '{}' in bucket '{}': {}",
                            entry_name, bucket_name, err
                        );
                        false
                    }
                };

                if !folder_removed {
                    Self::recover_entry_after_failed_removal(
                        &entries_map,
                        &entry_name,
                        &bucket_name,
                        path,
                        &entry,
                        cfg.clone(),
                        io_limiter.clone(),
                        Arc::clone(&usage_counters),
                    )
                    .await;
                    continue;
                }

                debug!(
                    "Remove entry '{}' from bucket '{}' and folder '{}'",
                    entry_name,
                    bucket_name,
                    path.display()
                );

                Self::remove_entry_from_map_with_retry(&entries_map, &entry_name, &bucket_name)
                    .await;
            }
        });

        Ok(())
    }

    pub(in crate::storage) async fn remove_entries_for_bucket_removal(
        &self,
    ) -> Result<(), ReductError> {
        self.reload().await?;

        let entries_snapshot: Vec<(String, Arc<Entry>)> = {
            let entries = self.entries.read().await?;
            entries
                .iter()
                .map(|(name, entry)| (name.clone(), Arc::clone(entry)))
                .collect()
        };
        let mut entries_snapshot = entries_snapshot;
        sort_entries_for_removal(&mut entries_snapshot);

        for (name, entry) in &entries_snapshot {
            if let Err(err) = entry.mark_deleting().await {
                if err.status() != ErrorCode::Conflict {
                    return Err(err);
                }
                if let Ok(status) = entry.status().await {
                    if status != ResourceStatus::Deleting {
                        error!(
                            "Entry '{}' in bucket '{}' is in {:?} state after conflict on delete",
                            name, self.name, status
                        );
                    }
                }
            }

            entry.remove_all_blocks().await?;
            if crate::core::file_cache::FILE_CACHE
                .try_exists(&self.path.join(name))
                .await?
            {
                self.folder_keeper.remove_folder(name).await?;
            }
        }

        let mut entries = self.entries.write().await?;
        for (name, _) in entries_snapshot {
            entries.remove(&name);
        }

        Ok(())
    }

    async fn remove_entry_from_map_with_retry(
        entries_map: &Arc<AsyncRwLock<BTreeMap<String, Arc<Entry>>>>,
        entry_name: &str,
        bucket_name: &str,
    ) {
        let _ = Self::write_entry_map_with_retry(
            entries_map,
            entry_name,
            bucket_name,
            "remove",
            |entries| {
                entries.remove(entry_name);
            },
        )
        .await;
    }

    async fn replace_entry_in_map_with_retry(
        entries_map: &Arc<AsyncRwLock<BTreeMap<String, Arc<Entry>>>>,
        entry_name: &str,
        bucket_name: &str,
        entry: Arc<Entry>,
    ) -> Result<(), ReductError> {
        Self::write_entry_map_with_retry(
            entries_map,
            entry_name,
            bucket_name,
            "recover",
            |entries| {
                entries.insert(entry_name.to_string(), Arc::clone(&entry));
            },
        )
        .await
    }

    async fn write_entry_map_with_retry(
        entries_map: &Arc<AsyncRwLock<BTreeMap<String, Arc<Entry>>>>,
        entry_name: &str,
        bucket_name: &str,
        action: &str,
        mut write_map: impl FnMut(&mut BTreeMap<String, Arc<Entry>>),
    ) -> Result<(), ReductError> {
        for attempt in 1..=REMOVE_ENTRY_MAP_RETRIES {
            match entries_map.write().await {
                Ok(mut entries) => {
                    write_map(&mut entries);
                    return Ok(());
                }
                Err(err) if attempt == REMOVE_ENTRY_MAP_RETRIES => {
                    error!(
                        "Failed to {} entry '{}' in bucket map '{}' after {} attempts: {}",
                        action, entry_name, bucket_name, attempt, err
                    );
                    return Err(err);
                }
                Err(err) => {
                    warn!(
                        "Failed to {} entry '{}' in bucket map '{}' on attempt {}: {}",
                        action, entry_name, bucket_name, attempt, err
                    );
                    sleep(REMOVE_ENTRY_MAP_RETRY_DELAY).await;
                }
            }
        }

        Ok(())
    }

    async fn recover_entry_after_failed_removal(
        entries_map: &Arc<AsyncRwLock<BTreeMap<String, Arc<Entry>>>>,
        entry_name: &str,
        bucket_name: &str,
        path: PathBuf,
        entry: &Arc<Entry>,
        cfg: Arc<Cfg>,
        io_limiter: InFlightIoLimiter,
        usage_counters: Arc<UsageCounters>,
    ) {
        let recovered_entry = match entry.settings().await {
            Ok(settings) => {
                Entry::restore_with_limiter(
                    path,
                    entry_name.to_string(),
                    bucket_name.to_string(),
                    settings,
                    cfg,
                    io_limiter,
                    usage_counters,
                )
                .await
            }
            Err(err) => Err(err),
        };

        match recovered_entry {
            Ok(Some(recovered_entry)) => {
                if let Err(err) = Self::replace_entry_in_map_with_retry(
                    entries_map,
                    entry_name,
                    bucket_name,
                    Arc::new(recovered_entry),
                )
                .await
                {
                    error!(
                        "Entry '{}' in bucket '{}' stays DELETING because recovery replacement failed: {}",
                        entry_name, bucket_name, err
                    );
                }
            }
            Ok(None) => {
                error!(
                    "Entry '{}' in bucket '{}' stays DELETING because it could not be restored",
                    entry_name, bucket_name
                );
            }
            Err(err) => {
                error!(
                    "Entry '{}' in bucket '{}' stays DELETING because recovery failed: {}",
                    entry_name, bucket_name, err
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Bucket;
    use crate::cfg::{Cfg, InstanceRole};
    use crate::core::file_cache::FILE_CACHE;
    use crate::core::sync::{reset_rwlock_config, set_rwlock_timeout};
    use prost::bytes::Bytes;
    use reduct_base::conflict;
    use reduct_base::error::ReductError;
    use reduct_base::internal_server_error;
    use reduct_base::msg::bucket_api::{BucketSettings, QuotaType};
    use reduct_base::msg::status::ResourceStatus;
    use reduct_base::Labels;
    use rstest::{fixture, rstest};
    use serial_test::serial;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::time::{sleep, Duration};

    #[rstest]
    #[tokio::test]
    async fn test_remove_entry(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        write(&bucket, "test-1", 1, b"test").await.unwrap();

        bucket.remove_entry("test-1").await.unwrap();
        let err = bucket.get_entry("test-1").await.err().unwrap();
        assert!(
            err == ReductError::conflict("Entry 'test-1' in bucket 'test' is being deleted")
                || err == ReductError::not_found("Entry 'test-1' not found in bucket 'test'"),
            "Should report deleting or already removed"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_entry_removes_meta(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        write(&bucket, "test-1", 1, b"test").await.unwrap();
        write_meta(&bucket, "test-1/$meta", 1, b"meta")
            .await
            .unwrap();

        bucket.remove_entry("test-1").await.unwrap();

        assert!(bucket.get_entry("test-1").await.is_err());
        assert!(bucket.get_entry("test-1/$meta").await.is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_parent_entry_removes_children(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        write(&bucket, "a/b/c", 1, b"test").await.unwrap();
        write_meta(&bucket, "a/b/c/$meta", 1, b"meta")
            .await
            .unwrap();

        bucket.remove_entry("a").await.unwrap();

        assert!(bucket.get_entry("a").await.is_err());
        assert!(bucket.get_entry("a/b").await.is_err());
        assert!(bucket.get_entry("a/b/c").await.is_err());
        assert!(bucket.get_entry("a/b/c/$meta").await.is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_entry_not_found(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        assert_eq!(
            bucket.remove_entry("test-1").await.err(),
            Some(ReductError::not_found(
                "Entry 'test-1' not found in bucket 'test'"
            ))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_meta_entry_directly_forbidden(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        let mut sender = bucket
            .begin_write(
                "test-1/$meta",
                1,
                4,
                "application/octet-stream".to_string(),
                reduct_base::Labels::from_iter([("key".to_string(), "default".to_string())]),
            )
            .await
            .unwrap();
        sender
            .send(Ok(Some(bytes::Bytes::from_static(b"meta"))))
            .await
            .unwrap();
        sender.send(Ok(None)).await.unwrap();
        assert_eq!(
            bucket.remove_entry("test-1/$meta").await.err(),
            Some(ReductError::conflict(
                "System entry 'test-1/$meta' can be removed only with its parent entry"
            ))
        );
        assert!(bucket.get_entry("test-1/$meta").await.is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn remove_entry_returns_conflict_when_entry_is_being_deleted(
        #[future] bucket: Arc<Bucket>,
    ) {
        let bucket = bucket.await;
        write(&bucket, "test-1", 1, b"test").await.unwrap();
        let entry = bucket.get_entry("test-1").await.unwrap().upgrade().unwrap();
        entry.mark_deleting().await.unwrap();

        assert_eq!(
            bucket.remove_entry("test-1").await,
            Err(conflict!(
                "Entry 'test-1' in bucket 'test' is being deleted"
            ))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_entry_is_idempotent_when_folder_is_missing(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        write(&bucket, "test-1", 1, b"test").await.unwrap();
        let entry = bucket.get_entry("test-1").await.unwrap().upgrade().unwrap();

        FILE_CACHE.remove_dir(entry.path()).await.unwrap();
        bucket.remove_entry("test-1").await.unwrap();

        for _ in 0..50 {
            if bucket.get_entry("test-1").await.is_err() {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }

        assert!(bucket.get_entry("test-1").await.is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn compact_removes_stale_entry_when_folder_is_missing(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        write(&bucket, "test-1", 1, b"test").await.unwrap();
        let entry = bucket.get_entry("test-1").await.unwrap().upgrade().unwrap();

        FILE_CACHE.remove_dir(entry.path()).await.unwrap();
        bucket.compact().await.unwrap();

        assert_eq!(
            bucket.get_entry("test-1").await.err(),
            Some(ReductError::not_found(
                "Entry 'test-1' not found in bucket 'test'"
            ))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn sync_fs_removes_stale_entry_when_folder_is_missing(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        write(&bucket, "test-1", 1, b"test").await.unwrap();
        let entry = bucket.get_entry("test-1").await.unwrap().upgrade().unwrap();

        FILE_CACHE.remove_dir(entry.path()).await.unwrap();
        bucket.sync_fs().await.unwrap();

        assert_eq!(
            bucket.get_entry("test-1").await.err(),
            Some(ReductError::not_found(
                "Entry 'test-1' not found in bucket 'test'"
            ))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn recover_entry_after_failed_removal_replaces_deleting_entry(
        #[future] bucket: Arc<Bucket>,
    ) {
        let bucket = bucket.await;
        write(&bucket, "test-1", 1, b"test").await.unwrap();
        let entry = bucket.get_entry("test-1").await.unwrap().upgrade().unwrap();
        entry.mark_deleting().await.unwrap();

        Bucket::recover_entry_after_failed_removal(
            &bucket.entries,
            "test-1",
            &bucket.name,
            entry.path().clone(),
            &entry,
            Arc::clone(&bucket.cfg),
            bucket.io_limiter.clone(),
            Arc::clone(&bucket.usage_counters),
        )
        .await;

        let recovered_entry = bucket.get_entry("test-1").await.unwrap().upgrade().unwrap();
        assert!(!Arc::ptr_eq(&entry, &recovered_entry));
        assert_eq!(entry.status().await.unwrap(), ResourceStatus::Deleting);
        assert_eq!(
            recovered_entry.status().await.unwrap(),
            ResourceStatus::Ready
        );
    }

    #[rstest]
    #[tokio::test]
    async fn recover_entry_after_failed_removal_keeps_deleting_when_replica_cannot_restore(
        #[future] bucket: Arc<Bucket>,
    ) {
        let bucket = bucket.await;
        write(&bucket, "test-1", 1, b"test").await.unwrap();
        let entry = bucket.get_entry("test-1").await.unwrap().upgrade().unwrap();
        entry.mark_deleting().await.unwrap();
        FILE_CACHE.remove_dir(entry.path()).await.unwrap();

        let cfg = Arc::new(Cfg {
            role: InstanceRole::Replica,
            ..Cfg::default()
        });

        Bucket::recover_entry_after_failed_removal(
            &bucket.entries,
            "test-1",
            &bucket.name,
            entry.path().clone(),
            &entry,
            cfg,
            bucket.io_limiter.clone(),
            Arc::clone(&bucket.usage_counters),
        )
        .await;

        let stored_entry = bucket
            .entries
            .read()
            .await
            .unwrap()
            .get("test-1")
            .cloned()
            .unwrap();
        assert!(Arc::ptr_eq(&entry, &stored_entry));
        assert_eq!(
            stored_entry.status().await.unwrap(),
            ResourceStatus::Deleting
        );
    }

    #[rstest]
    #[tokio::test]
    async fn recover_entry_after_failed_removal_keeps_deleting_when_recovery_fails(
        #[future] bucket: Arc<Bucket>,
    ) {
        let bucket = bucket.await;
        write(&bucket, "test-1", 1, b"test").await.unwrap();
        let entry = bucket.get_entry("test-1").await.unwrap().upgrade().unwrap();
        entry.mark_deleting().await.unwrap();
        FILE_CACHE.remove_dir(entry.path()).await.unwrap();

        Bucket::recover_entry_after_failed_removal(
            &bucket.entries,
            "test-1",
            &bucket.name,
            entry.path().clone(),
            &entry,
            Arc::clone(&bucket.cfg),
            bucket.io_limiter.clone(),
            Arc::clone(&bucket.usage_counters),
        )
        .await;

        let stored_entry = bucket
            .entries
            .read()
            .await
            .unwrap()
            .get("test-1")
            .cloned()
            .unwrap();
        assert!(Arc::ptr_eq(&entry, &stored_entry));
        assert_eq!(
            stored_entry.status().await.unwrap(),
            ResourceStatus::Deleting
        );
    }

    #[rstest]
    #[tokio::test]
    async fn remove_entry_from_map_with_retry_removes_entry(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        write(&bucket, "test-1", 1, b"test").await.unwrap();

        Bucket::remove_entry_from_map_with_retry(&bucket.entries, "test-1", &bucket.name).await;

        assert!(!bucket.entries.read().await.unwrap().contains_key("test-1"));
    }

    #[rstest]
    #[tokio::test]
    async fn replace_entry_in_map_with_retry_inserts_entry(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        write(&bucket, "test-1", 1, b"test").await.unwrap();
        let entry = bucket.get_entry("test-1").await.unwrap().upgrade().unwrap();
        bucket.entries.write().await.unwrap().remove("test-1");

        Bucket::replace_entry_in_map_with_retry(&bucket.entries, "test-1", &bucket.name, entry)
            .await
            .unwrap();

        assert!(bucket.entries.read().await.unwrap().contains_key("test-1"));
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn write_entry_map_with_retry_retries_after_lock_timeout(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        write(&bucket, "test-1", 1, b"test").await.unwrap();
        let entry = bucket.get_entry("test-1").await.unwrap().upgrade().unwrap();
        let entries_map = Arc::clone(&bucket.entries);

        set_rwlock_timeout(Duration::from_millis(5));
        let guard = entries_map.write().await.unwrap();
        let retry_entries_map = Arc::clone(&entries_map);
        let retry_task = tokio::spawn(async move {
            Bucket::write_entry_map_with_retry(
                &retry_entries_map,
                "retry-entry",
                "test",
                "test",
                |entries| {
                    entries.insert("retry-entry".to_string(), Arc::clone(&entry));
                },
            )
            .await
        });

        sleep(Duration::from_millis(20)).await;
        drop(guard);
        let result = retry_task.await.unwrap();
        reset_rwlock_config();

        result.unwrap();
        assert!(entries_map
            .read()
            .await
            .unwrap()
            .contains_key("retry-entry"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_entries_for_bucket_removal(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        write(&bucket, "test-1", 1, b"test").await.unwrap();
        write(&bucket, "test-2", 2, b"test").await.unwrap();

        assert!(FILE_CACHE
            .try_exists(&bucket.path.join("test-1"))
            .await
            .unwrap());
        assert!(FILE_CACHE
            .try_exists(&bucket.path.join("test-2"))
            .await
            .unwrap());

        bucket.remove_entries_for_bucket_removal().await.unwrap();

        let bucket_path = bucket.path.clone();
        let info = bucket.clone().info().await.unwrap();
        assert!(info.entries.is_empty());
        assert!(!FILE_CACHE
            .try_exists(&bucket_path.join("test-1"))
            .await
            .unwrap());
        assert!(!FILE_CACHE
            .try_exists(&bucket_path.join("test-2"))
            .await
            .unwrap());
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_entries_for_bucket_removal_with_meta(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        write(&bucket, "test-1", 1, b"test").await.unwrap();
        write_meta(&bucket, "test-1/$meta", 2, b"meta")
            .await
            .unwrap();

        bucket.remove_entries_for_bucket_removal().await.unwrap();

        let info = bucket.clone().info().await.unwrap();
        assert!(info.entries.is_empty());
        assert!(!FILE_CACHE
            .try_exists(&bucket.path.join("test-1"))
            .await
            .unwrap());
        assert!(!FILE_CACHE
            .try_exists(&bucket.path.join("test-1/$meta"))
            .await
            .unwrap());
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_entries_for_bucket_removal_with_deleting_entry(
        #[future] bucket: Arc<Bucket>,
    ) {
        let bucket = bucket.await;
        write(&bucket, "test-1", 1, b"test").await.unwrap();

        let entry = bucket.get_entry("test-1").await.unwrap().upgrade().unwrap();
        entry.mark_deleting().await.unwrap();

        bucket.remove_entries_for_bucket_removal().await.unwrap();

        assert_eq!(
            bucket.get_entry("test-1").await.err(),
            Some(ReductError::not_found(
                "Entry 'test-1' not found in bucket 'test'"
            ))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_entries_for_bucket_removal_with_empty_entry(
        #[future] bucket: Arc<Bucket>,
    ) {
        let bucket = bucket.await;
        bucket.get_or_create_entry("empty").await.unwrap();

        assert!(FILE_CACHE
            .try_exists(&bucket.path.join("empty"))
            .await
            .unwrap());

        bucket.remove_entries_for_bucket_removal().await.unwrap();

        assert!(!FILE_CACHE
            .try_exists(&bucket.path.join("empty"))
            .await
            .unwrap());
    }

    pub async fn write(
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

    pub async fn write_meta(
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
                Labels::from_iter([("key".to_string(), "default".to_string())]),
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

    #[fixture]
    pub fn settings() -> BucketSettings {
        BucketSettings {
            max_block_size: Some(100),
            quota_type: Some(QuotaType::FIFO),
            quota_size: Some(1000),
            max_block_records: Some(100),
        }
    }

    #[fixture]
    pub fn path() -> PathBuf {
        tempdir().unwrap().keep()
    }

    #[fixture]
    pub async fn bucket(settings: BucketSettings, path: PathBuf) -> Arc<Bucket> {
        FILE_CACHE.create_dir_all(&path.join("test")).await.unwrap();
        Arc::new(
            Bucket::builder()
                .name("test")
                .data_path(path)
                .settings(settings)
                .cfg(Cfg::default())
                .usage_counters(Default::default())
                .build()
                .await
                .unwrap(),
        )
    }
}
