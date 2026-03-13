// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use super::Bucket;
use crate::storage::engine::ReadOnlyMode;
use crate::storage::entry::is_system_meta_entry;
use crate::storage::entry::Entry;
use log::{debug, error};
use reduct_base::conflict;
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::msg::status::ResourceStatus;
use std::sync::Arc;

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
            return Err(ReductError::not_found(&format!(
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

        tokio::spawn(async move {
            for (entry_name, entry) in entries_to_remove {
                let path = entry.path().to_path_buf();
                let mut failed = false;

                if let Err(err) = entry.remove_all_blocks().await {
                    error!(
                        "Failed to remove blocks for entry '{}' in bucket '{}': {}",
                        entry_name, bucket_name, err
                    );
                    failed = true;
                }

                if !failed {
                    match crate::core::file_cache::FILE_CACHE.try_exists(&path).await {
                        Ok(true) => {
                            if let Err(err) = folder_keeper.remove_folder(&entry_name).await {
                                if err.status() != ErrorCode::NotFound {
                                    error!(
                                        "Failed to remove folder for entry '{}' in bucket '{}': {}",
                                        entry_name, bucket_name, err
                                    );
                                    failed = true;
                                }
                            }
                        }
                        Ok(false) => {}
                        Err(err) => {
                            error!(
                                "Failed to check folder for entry '{}' in bucket '{}': {}",
                                entry_name, bucket_name, err
                            );
                            failed = true;
                        }
                    }
                }

                if failed {
                    if let Err(err) = entry.mark_ready().await {
                        error!(
                            "Failed to recover entry '{}' to READY in bucket '{}': {}",
                            entry_name, bucket_name, err
                        );
                    }
                    continue;
                }

                debug!(
                    "Remove entry '{}' from bucket '{}' and folder '{}'",
                    entry_name,
                    bucket_name,
                    path.display()
                );

                match entries_map.write().await {
                    Ok(mut entries) => {
                        entries.remove(&entry_name);
                    }
                    Err(err) => {
                        error!(
                            "Failed to remove entry '{}' from bucket map '{}': {}",
                            entry_name, bucket_name, err
                        );
                    }
                }
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
}

#[cfg(test)]
mod tests {
    use super::Bucket;
    use crate::cfg::Cfg;
    use crate::core::file_cache::FILE_CACHE;
    use prost::bytes::Bytes;
    use reduct_base::conflict;
    use reduct_base::error::ReductError;
    use reduct_base::internal_server_error;
    use reduct_base::msg::bucket_api::{BucketSettings, QuotaType};
    use reduct_base::msg::status::ResourceStatus;
    use reduct_base::Labels;
    use rstest::{fixture, rstest};
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
    async fn test_remove_entry_recovers_when_blocks_are_missing(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        write(&bucket, "test-1", 1, b"test").await.unwrap();
        let entry = bucket.get_entry("test-1").await.unwrap().upgrade().unwrap();

        FILE_CACHE.remove_dir(entry.path()).await.unwrap();
        bucket.remove_entry("test-1").await.unwrap();

        for _ in 0..50 {
            if entry.status().await.unwrap() == ResourceStatus::Ready {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }

        assert_eq!(entry.status().await.unwrap(), ResourceStatus::Ready);
        assert!(bucket.get_entry("test-1").await.is_ok());
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
            Bucket::try_build("test", &path, settings, Cfg::default())
                .await
                .unwrap(),
        )
    }
}
