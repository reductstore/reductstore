// Copyright 2023-2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

use super::{settings_for_entry, Bucket};
use crate::core::file_cache::FILE_CACHE;
use crate::storage::engine::{check_entry_name_convention, ReadOnlyMode};
use crate::storage::entry::Entry;
use reduct_base::conflict;
use reduct_base::error::ReductError;
use reduct_base::not_found;
use std::sync::Arc;

impl Bucket {
    pub async fn rename_entry(&self, old_name: &str, new_name: &str) -> Result<(), ReductError> {
        self.check_mode()?;
        self.ensure_not_deleting().await?;

        let new_path = self.path.join(new_name);
        let bucket_name = self.name.clone();
        let entries = self.entries.clone();
        let settings = self.settings().await?;
        let cfg = self.cfg.clone();
        let folder_keeper = self.folder_keeper.clone();

        check_entry_name_convention(&new_name)?;
        if new_name == old_name || new_name.starts_with(&format!("{old_name}/")) {
            return Err(conflict!(
                "Entry '{}' already exists in bucket '{}'",
                new_name,
                bucket_name
            ));
        }

        let affected_entries: Vec<(String, Arc<Entry>)> = {
            let entries_guard = entries.read().await?;
            entries_guard
                .iter()
                .filter(|(entry_name, _)| Self::entry_with_descendants(old_name, entry_name))
                .map(|(entry_name, entry)| (entry_name.clone(), Arc::clone(entry)))
                .collect()
        };

        if affected_entries.is_empty() {
            return Err(not_found!(
                "Entry '{}' not found in bucket '{}'",
                old_name,
                bucket_name
            ));
        }

        if FILE_CACHE.try_exists(&new_path).await? {
            return Err(conflict!(
                "Entry '{}' already exists in bucket '{}'",
                new_name,
                bucket_name
            ));
        }

        let renamed_entries: Vec<(String, String, Arc<Entry>)> = affected_entries
            .into_iter()
            .map(|(entry_name, entry)| {
                (
                    entry_name.clone(),
                    format!("{}{}", new_name, &entry_name[old_name.len()..]),
                    entry,
                )
            })
            .collect();

        let old_entry_names: Vec<String> = renamed_entries
            .iter()
            .map(|(old_entry_name, _, _)| old_entry_name.clone())
            .collect();
        {
            let entries_guard = entries.read().await?;
            for (_, new_entry_name, _) in &renamed_entries {
                if entries_guard.contains_key(new_entry_name)
                    && !old_entry_names.iter().any(|name| name == new_entry_name)
                {
                    return Err(conflict!(
                        "Entry '{}' already exists in bucket '{}'",
                        new_entry_name,
                        bucket_name
                    ));
                }
            }
        }

        for (_, _, entry) in &renamed_entries {
            entry.ensure_not_deleting().await?;
            entry.compact().await?;
        }

        folder_keeper.rename_folder(old_name, new_name).await?;

        {
            let mut entries_guard = entries.write().await?;
            for (old_entry_name, _, _) in &renamed_entries {
                entries_guard.remove(old_entry_name);
            }
        }

        for (_, new_entry_name, _) in &renamed_entries {
            if let Some(entry) = Entry::restore(
                self.path.join(new_entry_name),
                new_entry_name.clone(),
                bucket_name.clone(),
                settings_for_entry(new_entry_name, &settings),
                cfg.clone(),
            )
            .await?
            {
                entries
                    .write()
                    .await?
                    .insert(new_entry_name.clone(), Arc::new(entry));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::Cfg;
    use crate::core::file_cache::FILE_CACHE;
    use prost::bytes::Bytes;
    use reduct_base::error::ReductError;
    use reduct_base::internal_server_error;
    use reduct_base::io::ReadRecord;
    use reduct_base::msg::bucket_api::{BucketSettings, QuotaType};
    use reduct_base::unprocessable_entity;
    use reduct_base::Labels;
    use rstest::{fixture, rstest};
    use std::path::PathBuf;
    use tempfile::tempdir;

    #[rstest]
    #[tokio::test]
    async fn test_rename_entry(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        write(&bucket, "test-1", 1, b"test").await.unwrap();

        bucket.rename_entry("test-1", "test-2").await.unwrap();
        assert_eq!(
            bucket.get_entry("test-1").await.err(),
            Some(ReductError::not_found(
                "Entry 'test-1' not found in bucket 'test'"
            ))
        );
        assert_eq!(
            bucket
                .get_entry("test-2")
                .await
                .unwrap()
                .upgrade()
                .unwrap()
                .name(),
            "test-2"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_rename_entry_not_found(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        assert_eq!(
            bucket.rename_entry("test-1", "test-2").await.err(),
            Some(not_found!("Entry 'test-1' not found in bucket 'test'"))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_rename_entry_already_exists(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        write(&bucket, "test-1", 1, b"test").await.unwrap();
        write(&bucket, "test-2", 1, b"test").await.unwrap();

        assert_eq!(
            bucket.rename_entry("test-1", "test-2").await.err(),
            Some(conflict!("Entry 'test-2' already exists in bucket 'test'"))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_rename_invalid_name(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        assert_eq!(
            bucket.rename_entry("test-1", "test-2$").await.err(),
            Some(unprocessable_entity!(
                "Entry name can contain only letters, digits and [-,_,/] symbols or end with '/$meta'"
            ))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_rename_with_wal_segment(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        write(&bucket, "test/a", 1, b"test").await.unwrap();
        bucket.rename_entry("test/a", "test/wal").await.unwrap();
        assert!(bucket.get_entry("test/a").await.is_err());
        assert_eq!(
            bucket
                .get_entry("test/wal")
                .await
                .unwrap()
                .upgrade()
                .unwrap()
                .name(),
            "test/wal"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_rename_moves_meta_entry(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        write(&bucket, "test/a", 1, b"data").await.unwrap();
        write_meta(&bucket, "test/a/$meta", 1, b"meta")
            .await
            .unwrap();

        bucket.rename_entry("test/a", "test/b").await.unwrap();

        assert!(bucket.get_entry("test/a").await.is_err());
        assert!(bucket.get_entry("test/a/$meta").await.is_err());
        assert!(bucket.get_entry("test/b").await.is_ok());
        assert!(bucket.get_entry("test/b/$meta").await.is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn test_rename_parent_renames_children(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        write(&bucket, "a/b/c", 1, b"test").await.unwrap();
        write_meta(&bucket, "a/b/c/$meta", 2, b"meta")
            .await
            .unwrap();

        bucket.rename_entry("a", "renamed").await.unwrap();

        assert!(bucket.get_entry("a").await.is_err());
        assert!(bucket.get_entry("a/b").await.is_err());
        assert!(bucket.get_entry("a/b/c").await.is_err());
        assert!(bucket.get_entry("a/b/c/$meta").await.is_err());

        assert!(bucket.get_entry("renamed").await.is_ok());
        assert!(bucket.get_entry("renamed/b").await.is_ok());
        assert!(bucket.get_entry("renamed/b/c").await.is_ok());
        assert!(bucket.get_entry("renamed/b/c/$meta").await.is_ok());
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_rename_entry_persisted(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        write(&bucket, "test-1", 1, b"test").await.unwrap();
        bucket.sync_fs().await.unwrap();
        bucket.rename_entry("test-1", "test-2").await.unwrap();

        let bucket = Bucket::restore(bucket.path.clone(), Cfg::default())
            .await
            .unwrap();
        assert_eq!(
            bucket.get_entry("test-1").await.err(),
            Some(ReductError::not_found(
                "Entry 'test-1' not found in bucket 'test'"
            ))
        );

        let mut reader = bucket.begin_read("test-2", 1).await.unwrap();
        assert_eq!(reader.read_chunk().unwrap().unwrap(), Bytes::from("test"));
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

    async fn write_meta(
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
