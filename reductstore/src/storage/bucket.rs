// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

mod quotas;
mod settings;

use crate::core::thread_pool::{unique, TaskHandle};
use crate::core::weak::Weak;
pub use crate::storage::block_manager::RecordRx;
pub use crate::storage::block_manager::RecordTx;
use crate::storage::bucket::settings::{
    DEFAULT_MAX_BLOCK_SIZE, DEFAULT_MAX_RECORDS, SETTINGS_NAME,
};
use crate::storage::entry::{Entry, EntrySettings, RecordReader, WriteRecordContent};
use crate::storage::file_cache::FILE_CACHE;
use crate::storage::proto::BucketSettings as ProtoBucketSettings;
use log::debug;
use prost::bytes::Bytes;
use prost::Message;
use reduct_base::error::ReductError;
use reduct_base::msg::bucket_api::{BucketInfo, BucketSettings, FullBucketInfo};
use reduct_base::msg::entry_api::EntryInfo;
use reduct_base::{internal_server_error, not_found, Labels};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};

/// Bucket is a single storage bucket.
pub(crate) struct Bucket {
    name: String,
    path: PathBuf,
    entries: Arc<RwLock<BTreeMap<String, Arc<Entry>>>>,
    settings: RwLock<BucketSettings>,
    is_provisioned: AtomicBool,
}

impl Bucket {
    /// Create a new Bucket
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the bucket
    /// * `path` - The path to folder with buckets
    /// * `settings` - The settings for the bucket
    /// * `repl_agent_builder` - The replica agent builder
    ///
    /// # Returns
    ///
    /// * `Bucket` - The bucket or an HTTPError
    pub(crate) fn new(
        name: &str,
        path: &PathBuf,
        settings: BucketSettings,
    ) -> Result<Bucket, ReductError> {
        let path = path.join(name);
        std::fs::create_dir_all(&path)?;

        let settings = Self::fill_settings(settings, Self::defaults());
        let bucket = Bucket {
            name: name.to_string(),
            path,
            entries: Arc::new(RwLock::new(BTreeMap::new())),
            settings: RwLock::new(settings),
            is_provisioned: AtomicBool::new(false),
        };

        bucket.save_settings().wait()?;
        Ok(bucket)
    }

    /// Restore a Bucket from disk
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the bucket
    /// * `repl_agent_builder` - The replica agent builder
    ///
    /// # Returns
    ///
    /// * `Bucket` - The bucket or an HTTPError
    pub fn restore(path: PathBuf) -> Result<Bucket, ReductError> {
        let buf: Vec<u8> = std::fs::read(path.join(SETTINGS_NAME))?;
        let settings = ProtoBucketSettings::decode(&mut Bytes::from(buf))
            .map_err(|e| internal_server_error!("Failed to decode settings: {}", e))?;

        let settings = Self::fill_settings(settings.into(), Self::defaults());
        let bucket_name = path.file_name().unwrap().to_str().unwrap().to_string();

        let mut entries = BTreeMap::new();
        let mut task_set = Vec::new();

        for entry in std::fs::read_dir(&path)? {
            let path = entry?.path();
            if path.is_dir() {
                let handler = Entry::restore(
                    path,
                    EntrySettings {
                        max_block_size: settings.max_block_size.unwrap(),
                        max_block_records: settings.max_block_records.unwrap(),
                    },
                );

                task_set.push(handler);
            }
        }

        for task in task_set {
            let entry = task.wait()?;
            entries.insert(entry.name().to_string(), entry);
        }

        Ok(Bucket {
            name: bucket_name,
            path,
            entries: Arc::new(RwLock::new(
                entries.into_iter().map(|(k, v)| (k, Arc::new(v))).collect(),
            )),
            settings: RwLock::new(settings),
            is_provisioned: AtomicBool::new(false),
        })
    }

    /// Get or create an entry in the bucket
    ///
    /// # Arguments
    ///
    /// * `key` - The key of the entry
    ///
    /// # Returns
    ///
    /// * `&mut Entry` - The entry or an HTTPError
    pub fn get_or_create_entry(&self, key: &str) -> Result<Weak<Entry>, ReductError> {
        let mut entries = self.entries.write().unwrap();
        if !entries.contains_key(key) {
            let settings = self.settings.read().unwrap();
            let entry = Entry::try_new(
                &key,
                self.path.clone(),
                EntrySettings {
                    max_block_size: settings.max_block_size.unwrap(),
                    max_block_records: settings.max_block_records.unwrap(),
                },
            )?;
            entries.insert(key.to_string(), Arc::new(entry));
        }

        Ok(entries.get_mut(key).unwrap().clone().into())
    }

    /// Get an entry in the bucket
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the entry
    ///
    /// # Returns
    ///
    /// * `&Entry` - The entry or an HTTPError
    pub fn get_entry(&self, name: &str) -> Result<Weak<Entry>, ReductError> {
        let entries = self.entries.read().unwrap();
        let entry = entries.get(name).ok_or_else(|| {
            ReductError::not_found(&format!(
                "Entry '{}' not found in bucket '{}'",
                name, self.name
            ))
        })?;
        Ok(entry.clone().into())
    }

    /// Return bucket stats
    pub fn info(&self) -> Result<FullBucketInfo, ReductError> {
        let mut size = 0;
        let mut oldest_record = u64::MAX;
        let mut latest_record = 0u64;
        let mut entries: Vec<EntryInfo> = vec![];

        let entry_map = self.entries.read().unwrap();
        for entry in entry_map.values() {
            let info = entry.info()?;
            entries.push(info.clone());
            size += info.size;
            oldest_record = oldest_record.min(info.oldest_record);
            latest_record = latest_record.max(info.latest_record);
        }
        Ok(FullBucketInfo {
            info: BucketInfo {
                name: self.name.clone(),
                size,
                entry_count: entry_map.len() as u64,
                oldest_record,
                latest_record,
                is_provisioned: self
                    .is_provisioned
                    .load(std::sync::atomic::Ordering::Relaxed),
            },
            settings: self.settings.read().unwrap().clone(),
            entries,
        })
    }

    /// Starts a new record write with
    ///
    /// # Arguments
    ///
    /// * `name` - Entry name.
    /// * `time` - The timestamp of the record.
    /// * `content_size` - The size of the record content.
    /// * `content_type` - The content type of the record.
    /// * `labels` - The labels of the record.
    ///
    /// # Returns
    ///
    /// * `Sender<Result<Bytes, ReductError>>` - The sender to send the record content in chunks.
    /// * `HTTPError` - The error if any.
    pub fn begin_write(
        &self,
        name: &str,
        time: u64,
        content_size: usize,
        content_type: String,
        labels: Labels,
    ) -> TaskHandle<Result<Box<dyn WriteRecordContent + Sync + Send>, ReductError>> {
        let get_entry = || {
            self.keep_quota_for(content_size)?;
            self.get_or_create_entry(name)?.upgrade()
        };

        let entry = match get_entry() {
            Ok(entry) => entry,
            Err(e) => {
                return Err(e).into();
            }
        };

        entry.begin_write(time, content_size, content_type, labels)
    }

    /// Starts a new record read with
    #[allow(dead_code)]
    pub fn begin_read(
        &self,
        name: &str,
        time: u64,
    ) -> TaskHandle<Result<RecordReader, ReductError>> {
        let entry = || self.get_entry(name)?.upgrade();

        match entry() {
            Ok(entry) => entry.begin_read(time),
            Err(e) => Err(e).into(),
        }
    }

    pub fn rename_entry(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> TaskHandle<Result<(), ReductError>> {
        let old_path = self.path.join(old_name);
        let new_path = self.path.join(new_name);
        let bucket_name = self.name.clone();
        let entries = self.entries.clone();
        let old_name = old_name.to_string();
        let new_name = new_name.to_string();
        let settings = self.settings();

        unique(&self.task_group(), "rename entry", move || {
            if entries.write().unwrap().remove(&old_name).is_none() {
                return Err(not_found!(
                    "Entry '{}' not found in bucket '{}'",
                    old_name,
                    bucket_name
                ));
            }

            FILE_CACHE.discard_recursive(&old_path)?; // we need to close all open files
            std::fs::rename(&old_path, &new_path)?;

            let entry = Entry::restore(
                new_path,
                EntrySettings {
                    max_block_size: settings.max_block_size.unwrap_or(DEFAULT_MAX_BLOCK_SIZE),
                    max_block_records: settings.max_block_records.unwrap_or(DEFAULT_MAX_RECORDS),
                },
            )
            .wait()?;

            entries
                .write()?
                .insert(new_name.to_string(), Arc::new(entry));
            Ok(())
        })
    }

    /// Remove entry from the bucket
    ///
    /// # Arguments
    ///
    /// * `name` - Entry name.
    ///
    /// # Returns
    ///
    /// * `HTTPError` - The error if any.
    pub fn remove_entry(&self, name: &str) -> TaskHandle<Result<(), ReductError>> {
        if let Err(e) = self.get_entry(name) {
            return Err(e).into();
        }

        let entries = self.entries.clone();
        let path = self.path.join(name);
        let bucket_name = self.name.clone();
        let entry_name = name.to_string();

        unique(&self.task_group(), "remove entry", move || {
            FILE_CACHE.remove_dir(&path)?;
            debug!(
                "Remove entry '{}' from bucket '{}' and folder '{}'",
                entry_name,
                bucket_name,
                path.display()
            );

            entries.write()?.remove(&entry_name);
            Ok(())
        })
    }

    /// Sync all entries to the file system
    pub fn sync_fs(&self) -> TaskHandle<Result<(), ReductError>> {
        let entries = self.entries.clone();

        unique(&self.task_group(), "sync entires", move || {
            let entries = entries.read().unwrap();
            for entry in entries.values() {
                entry.sync_fs()?;
            }
            Ok(())
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Mark bucket as provisioned to protect
    pub fn set_provisioned(&self, provisioned: bool) {
        self.is_provisioned
            .store(provisioned, std::sync::atomic::Ordering::Relaxed);
    }

    /// Check if bucket is provisioned
    pub fn is_provisioned(&self) -> bool {
        self.is_provisioned
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn task_group(&self) -> String {
        // use folder hierarchy as task group to protect resources
        [
            self.path
                .parent()
                .unwrap()
                .file_name()
                .unwrap()
                .to_str()
                .unwrap(),
            &self.path.file_name().unwrap().to_str().unwrap(),
        ]
        .join("/")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reduct_base::conflict;
    use reduct_base::msg::bucket_api::QuotaType;
    use rstest::{fixture, rstest};
    use tempfile::tempdir;

    mod remove_entry {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_remove_entry(mut bucket: Bucket) {
            write(&mut bucket, "test-1", 1, b"test").await.unwrap();

            bucket.remove_entry("test-1").await.unwrap();
            assert_eq!(
                bucket.get_entry("test-1").err(),
                Some(ReductError::not_found(
                    "Entry 'test-1' not found in bucket 'test'"
                ))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_remove_entry_not_found(bucket: Bucket) {
            assert_eq!(
                bucket.remove_entry("test-1").await.err(),
                Some(ReductError::not_found(
                    "Entry 'test-1' not found in bucket 'test'"
                ))
            );
        }
    }

    mod rename_entry {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_rename_entry(mut bucket: Bucket) {
            write(&mut bucket, "test-1", 1, b"test").await.unwrap();

            bucket.rename_entry("test-1", "test-2").await.unwrap();
            assert_eq!(
                bucket.get_entry("test-1").err(),
                Some(ReductError::not_found(
                    "Entry 'test-1' not found in bucket 'test'"
                ))
            );
            assert_eq!(
                bucket
                    .get_entry("test-2")
                    .unwrap()
                    .upgrade()
                    .unwrap()
                    .name(),
                "test-2"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_rename_entry_not_found(bucket: Bucket) {
            assert_eq!(
                bucket.rename_entry("test-1", "test-2").await.err(),
                Some(not_found!("Entry 'test-1' not found in bucket 'test'"))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_rename_entry_persisted(mut bucket: Bucket) {
            write(&mut bucket, "test-1", 1, b"test").await.unwrap();
            bucket.sync_fs().await.unwrap();
            bucket.rename_entry("test-1", "test-2").await.unwrap();

            let bucket = Bucket::restore(bucket.path.clone()).unwrap();
            assert_eq!(
                bucket.get_entry("test-1").err(),
                Some(ReductError::not_found(
                    "Entry 'test-1' not found in bucket 'test'"
                ))
            );

            let mut reader = bucket.begin_read("test-2", 1).await.unwrap();
            assert_eq!(
                reader.rx().recv().await.unwrap().unwrap(),
                Bytes::from("test")
            );
        }
    }

    #[rstest]
    fn test_provisioned_info(provisioned_bucket: Bucket) {
        let info = provisioned_bucket.info().unwrap().info;
        assert_eq!(info.is_provisioned, true);
    }

    #[rstest]
    fn test_provisioned_settings(provisioned_bucket: Bucket) {
        let err = provisioned_bucket
            .set_settings(BucketSettings::default())
            .wait()
            .err()
            .unwrap();
        assert_eq!(
            err,
            conflict!("Can't change settings of provisioned bucket 'test'")
        );
    }

    pub async fn write(
        bucket: &mut Bucket,
        entry_name: &str,
        time: u64,
        content: &'static [u8],
    ) -> Result<(), ReductError> {
        let sender = bucket
            .begin_write(
                entry_name,
                time,
                content.len(),
                "".to_string(),
                Labels::new(),
            )
            .await?;
        sender
            .tx()
            .send(Ok(Some(Bytes::from(content))))
            .await
            .map_err(|e| internal_server_error!("Failed to send data: {}", e))?;
        sender
            .tx()
            .send(Ok(None))
            .await
            .map_err(|e| internal_server_error!("Failed to sync channel: {}", e))?;
        sender.tx().closed().await;
        Ok(())
    }

    pub async fn read(
        bucket: &mut Bucket,
        entry_name: &str,
        time: u64,
    ) -> Result<Vec<u8>, ReductError> {
        let mut reader = bucket.begin_read(entry_name, time).await?;
        let data = reader.rx().recv().await.unwrap().unwrap();
        Ok(data.to_vec())
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
        tempdir().unwrap().into_path()
    }

    #[fixture]
    pub fn bucket(settings: BucketSettings, path: PathBuf) -> Bucket {
        Bucket::new("test", &path, settings).unwrap()
    }

    #[fixture]
    pub fn provisioned_bucket(settings: BucketSettings, path: PathBuf) -> Bucket {
        let bucket = Bucket::new("test", &path, settings).unwrap();
        bucket.set_provisioned(true);
        bucket
    }
}
