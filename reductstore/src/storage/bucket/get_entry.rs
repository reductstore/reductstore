// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use super::{settings_for_entry, Bucket};
use crate::core::weak::Weak;
use crate::storage::engine::{check_entry_name_convention, ReadOnlyMode};
use crate::storage::entry::Entry;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::sync::Arc;

impl Bucket {
    /// Get or create an entry in the bucket
    ///
    /// # Arguments
    ///
    /// * `key` - The key of the entry
    ///
    /// # Returns
    ///
    /// * `&mut Entry` - The entry or an HTTPError
    pub async fn get_or_create_entry(&self, key: &str) -> Result<Weak<Entry>, ReductError> {
        check_entry_name_convention(key)?;
        self.ensure_not_deleting().await?;
        if let Some(entry) = self.entries.read().await?.get(key).cloned() {
            entry.ensure_not_deleting().await?;
            return Ok(entry.into());
        }

        self.check_mode()?;
        let mut target_entry = None;
        let settings = self.settings().await?;
        for prefix in Self::parent_prefixes(key) {
            let existing = {
                let entries = self.entries.read().await?;
                entries.get(&prefix).cloned()
            };
            let entry = if let Some(entry) = existing {
                entry
            } else {
                self.folder_keeper.add_folder(&prefix).await?;
                let entry = Arc::new(
                    Entry::try_build(
                        &prefix,
                        self.path.clone(),
                        settings_for_entry(&prefix, &settings),
                        self.cfg.clone(),
                    )
                    .await?,
                );
                let mut entries = self.entries.write().await?;
                entries
                    .entry(prefix.clone())
                    .or_insert_with(|| Arc::clone(&entry))
                    .clone()
            };

            if prefix == key {
                target_entry = Some(entry);
            }
        }
        let entry = Self::target_entry_or_err(target_entry, key)?;
        entry.ensure_not_deleting().await?;
        Ok(entry.into())
    }

    fn target_entry_or_err(
        target_entry: Option<Arc<Entry>>,
        key: &str,
    ) -> Result<Arc<Entry>, ReductError> {
        target_entry.ok_or_else(|| {
            internal_server_error!(
                "Failed to resolve target entry '{}' while creating bucket entry",
                key
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::Cfg;
    use crate::core::file_cache::FILE_CACHE;
    use crate::storage::block_manager::BLOCK_INDEX_FILE;
    use crate::storage::entry::META_ENTRY_MAX_BLOCK_SIZE;
    use reduct_base::msg::bucket_api::{BucketSettings, QuotaType};
    use reduct_base::unprocessable_entity;
    use rstest::{fixture, rstest};
    use std::path::PathBuf;
    use tempfile::tempdir;

    #[rstest]
    #[tokio::test]
    async fn test_get_or_create_entry(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        let entry = bucket.get_or_create_entry("test-1").await.unwrap();
        assert_eq!(entry.upgrade().unwrap().name(), "test-1");
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_or_create_entry_invalid_name(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        assert_eq!(
            bucket.get_or_create_entry("test-1$").await.err(),
            Some(unprocessable_entity!(
                "Entry name can contain only letters, digits and [-,_,/] symbols or end with '/$meta'"
            ))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_or_create_entry_rejects_empty_path_segments(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        assert_eq!(
            bucket.get_or_create_entry("test-1//a").await.err(),
            Some(unprocessable_entity!(
                "Entry name must be non-empty and must not contain empty path segments"
            ))
        );
    }

    #[test]
    fn test_target_entry_or_err_returns_error_for_missing_target() {
        assert_eq!(
            Bucket::target_entry_or_err(None, "test-1").err(),
            Some(internal_server_error!(
                "Failed to resolve target entry 'test-1' while creating bucket entry"
            ))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_or_create_entry_with_path(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        let entry = bucket.get_or_create_entry("test-1/a").await.unwrap();
        assert_eq!(entry.upgrade().unwrap().name(), "test-1/a");
        assert!(bucket.get_entry("test-1").await.is_ok());
        assert!(bucket.get_entry("test-1/a").await.is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_or_create_entry_creates_parent_entries(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        bucket.get_or_create_entry("a/b/c").await.unwrap();

        assert!(bucket.get_entry("a").await.is_ok());
        assert!(bucket.get_entry("a/b").await.is_ok());
        assert!(bucket.get_entry("a/b/c").await.is_ok());
        assert!(FILE_CACHE
            .try_exists(&bucket.path().join("a").join(BLOCK_INDEX_FILE))
            .await
            .unwrap());
        assert!(FILE_CACHE
            .try_exists(&bucket.path().join("a/b").join(BLOCK_INDEX_FILE))
            .await
            .unwrap());
        assert!(FILE_CACHE
            .try_exists(&bucket.path().join("a/b/c").join(BLOCK_INDEX_FILE))
            .await
            .unwrap());
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_or_create_entry_with_wal_segment(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        let entry = bucket.get_or_create_entry("test/wal").await.unwrap();
        assert_eq!(entry.upgrade().unwrap().name(), "test/wal");
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_or_create_meta_entry(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        let entry = bucket.get_or_create_entry("test/$meta").await.unwrap();
        assert_eq!(entry.upgrade().unwrap().name(), "test/$meta");
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_or_create_meta_entry_uses_small_block_size(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        let entry = bucket
            .get_or_create_entry("test/$meta")
            .await
            .unwrap()
            .upgrade()
            .unwrap();
        let settings = entry.settings().await.unwrap();
        assert_eq!(settings.max_block_size, META_ENTRY_MAX_BLOCK_SIZE);
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
