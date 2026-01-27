// Copyright 2023-2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

use super::Bucket;
use crate::storage::engine::ReadOnlyMode;
use crate::storage::entry::Entry;
use log::{debug, error};
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::msg::status::ResourceStatus;
use std::sync::Arc;

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

        let entry = self.get_entry(name).await?.upgrade()?;
        entry.mark_deleting().await?;

        let entries = self.entries.clone();
        let path = self.path.join(name);
        let bucket_name = self.name.clone();
        let entry_name = name.to_string();
        let folder_keeper = self.folder_keeper.clone();

        tokio::spawn(async move {
            let remove_entry_from_backend = async || {
                entry.remove_all_blocks().await?;
                folder_keeper.remove_folder(&entry_name).await?;

                debug!(
                    "Remove entry '{}' from bucket '{}' and folder '{}'",
                    entry_name,
                    bucket_name,
                    path.display()
                );
                entries.write().await?.remove(&entry_name);
                Ok::<(), ReductError>(())
            };

            if let Err(err) = remove_entry_from_backend().await {
                error!(
                    "Failed to remove entry '{}' from bucket '{}': {}",
                    entry_name, bucket_name, err
                );
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
            self.folder_keeper.remove_folder(name).await?;
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
    use reduct_base::Labels;
    use rstest::{fixture, rstest};
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::tempdir;

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
