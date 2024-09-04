// Copyright 2023 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::entry::{Entry, EntrySettings, RecordReader, WriteRecordContent};
use crate::storage::proto::BucketSettings as ProtoBucketSettings;
use log::debug;
use prost::bytes::{Bytes, BytesMut};
use prost::Message;
use reduct_base::error::ReductError;
use reduct_base::msg::bucket_api::{BucketInfo, BucketSettings, FullBucketInfo, QuotaType};
use reduct_base::msg::entry_api::EntryInfo;
use reduct_base::Labels;
use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;
use tokio::task::JoinSet;

pub use crate::storage::block_manager::RecordRx;
pub use crate::storage::block_manager::RecordTx;
use crate::storage::file_cache::FILE_CACHE;

const DEFAULT_MAX_RECORDS: u64 = 1024;
const DEFAULT_MAX_BLOCK_SIZE: u64 = 64000000;
const SETTINGS_NAME: &str = "bucket.settings";

impl From<BucketSettings> for ProtoBucketSettings {
    fn from(settings: BucketSettings) -> Self {
        ProtoBucketSettings {
            quota_size: settings.quota_size,
            quota_type: if let Some(quota_type) = settings.quota_type {
                Some(quota_type as i32)
            } else {
                None
            },
            max_block_records: settings.max_block_records,
            max_block_size: settings.max_block_size,
        }
    }
}

impl Into<BucketSettings> for ProtoBucketSettings {
    fn into(self) -> BucketSettings {
        BucketSettings {
            quota_size: self.quota_size,
            quota_type: if let Some(quota_type) = self.quota_type {
                Some(QuotaType::from(quota_type))
            } else {
                None
            },
            max_block_records: self.max_block_records,
            max_block_size: self.max_block_size,
        }
    }
}

/// Bucket is a single storage bucket.
pub(crate) struct Bucket {
    name: String,
    path: PathBuf,
    entries: BTreeMap<String, Entry>,
    settings: BucketSettings,
    is_provisioned: bool,
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
            entries: BTreeMap::new(),
            settings,
            is_provisioned: false,
        };

        bucket.save_settings()?;
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
    pub async fn restore(path: PathBuf) -> Result<Bucket, ReductError> {
        let buf: Vec<u8> = std::fs::read(path.join(SETTINGS_NAME))?;
        let settings = ProtoBucketSettings::decode(&mut Bytes::from(buf)).map_err(|e| {
            ReductError::internal_server_error(format!("Failed to decode settings: {}", e).as_str())
        })?;

        let settings = Self::fill_settings(settings.into(), Self::defaults());
        let bucket_name = path.file_name().unwrap().to_str().unwrap().to_string();

        let mut entries = BTreeMap::new();
        let mut task_set = JoinSet::new();

        for entry in std::fs::read_dir(&path)? {
            let path = entry?.path();
            if path.is_dir() {
                task_set.spawn(async move {
                    Entry::restore(
                        path,
                        EntrySettings {
                            max_block_size: settings.max_block_size.unwrap(),
                            max_block_records: settings.max_block_records.unwrap(),
                        },
                    )
                    .await
                });
            }
        }

        while let Some(entry) = task_set.join_next().await {
            let entry = entry.map_err(|e| {
                ReductError::internal_server_error(
                    format!("Failed to restore entry: {}", e).as_str(),
                )
            })?;
            let entry = entry?;
            entries.insert(entry.name().to_string(), entry);
        }

        Ok(Bucket {
            name: bucket_name,
            path,
            entries,
            settings,
            is_provisioned: false,
        })
    }

    /// Default settings for a new bucket bucket
    pub fn defaults() -> BucketSettings {
        BucketSettings {
            max_block_size: Some(DEFAULT_MAX_BLOCK_SIZE),
            quota_type: Some(QuotaType::NONE),
            quota_size: Some(0),
            max_block_records: Some(DEFAULT_MAX_RECORDS),
        }
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
    pub fn get_or_create_entry(&mut self, key: &str) -> Result<&mut Entry, ReductError> {
        if !self.entries.contains_key(key) {
            let entry = Entry::new(
                &key,
                self.path.clone(),
                EntrySettings {
                    max_block_size: self.settings.max_block_size.unwrap(),
                    max_block_records: self.settings.max_block_records.unwrap(),
                },
            );
            self.entries.insert(key.to_string(), entry?);
        }

        Ok(self.entries.get_mut(key).unwrap())
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
    pub fn get_entry(&self, name: &str) -> Result<&Entry, ReductError> {
        let entry = self.entries.get(name).ok_or_else(|| {
            ReductError::not_found(&format!(
                "Entry '{}' not found in bucket '{}'",
                name, self.name
            ))
        })?;
        Ok(entry)
    }

    /// Get an entry in the bucket
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the entry
    ///
    /// # Returns
    ///
    /// * `&mut Entry` - The entry or an HTTPError
    pub fn get_entry_mut(&mut self, name: &str) -> Result<&mut Entry, ReductError> {
        let entry = self.entries.get_mut(name).ok_or_else(|| {
            ReductError::not_found(&format!(
                "Entry '{}' not found in bucket '{}'",
                name, self.name
            ))
        })?;
        Ok(entry)
    }

    /// Return bucket stats
    pub async fn info(&self) -> Result<FullBucketInfo, ReductError> {
        let mut size = 0;
        let mut oldest_record = u64::MAX;
        let mut latest_record = 0u64;
        let mut entries: Vec<EntryInfo> = vec![];
        for entry in self.entries.values() {
            let info = entry.info().await?;
            entries.push(info.clone());
            size += info.size;
            oldest_record = oldest_record.min(info.oldest_record);
            latest_record = latest_record.max(info.latest_record);
        }
        Ok(FullBucketInfo {
            info: BucketInfo {
                name: self.name.clone(),
                size,
                entry_count: self.entries.len() as u64,
                oldest_record,
                latest_record,
                is_provisioned: self.is_provisioned,
            },
            settings: self.settings.clone(),
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
    pub async fn write_record(
        &mut self,
        name: &str,
        time: u64,
        content_size: usize,
        content_type: String,
        labels: Labels,
    ) -> Result<Box<dyn WriteRecordContent + Sync + Send>, ReductError> {
        self.keep_quota_for(content_size).await?;
        let entry = self.get_or_create_entry(name)?;
        entry
            .begin_write(time, content_size, content_type, labels)
            .await
    }

    /// Starts a new record read with
    ///
    /// # Arguments
    ///
    /// * `name` - Entry name.
    /// * `time` - The timestamp of the record.
    ///
    /// # Returns
    ///
    /// * `RecordReader` - The record reader to read the record content in chunks.
    /// * `HTTPError` - The error if any.
    pub async fn begin_read(&self, name: &str, time: u64) -> Result<RecordReader, ReductError> {
        let entry = self.get_entry(name)?;
        entry.begin_read(time).await
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
    pub async fn remove_entry(&mut self, name: &str) -> Result<(), ReductError> {
        _ = self.get_entry(name)?;

        let path = self.path.join(name);
        FILE_CACHE.remove_dir(&path).await?;
        debug!(
            "Remove entry '{}' from bucket '{}' and folder '{}'",
            name,
            self.name,
            path.display()
        );
        self.entries.remove(name);
        Ok(())
    }

    async fn keep_quota_for(&mut self, content_size: usize) -> Result<(), ReductError> {
        match self.settings.quota_type.clone().unwrap_or(QuotaType::NONE) {
            QuotaType::NONE => Ok(()),
            QuotaType::FIFO => {
                let mut size = self.info().await?.info.size + content_size as u64;
                while size > self.settings.quota_size.unwrap_or(0) {
                    debug!(
                        "Need more space. Remove an oldest block from bucket '{}'",
                        self.name()
                    );

                    let mut candidates: Vec<(u64, &Entry)> = vec![];
                    for (_, entry) in self.entries.iter_mut() {
                        let info = entry.info().await?;
                        candidates.push((info.oldest_record, entry));
                    }
                    candidates.sort_by_key(|entry| entry.0);

                    let candidates = candidates
                        .iter()
                        .map(|(_, entry)| entry.name().to_string())
                        .collect::<Vec<String>>();

                    let mut success = false;
                    for name in candidates {
                        debug!("Remove an oldest block from entry '{}'", name);
                        match self
                            .entries
                            .get_mut(&name)
                            .unwrap()
                            .try_remove_oldest_block()
                            .await
                        {
                            Ok(_) => {
                                success = true;
                                break;
                            }
                            Err(e) => {
                                debug!(
                                    "Failed to remove oldest block from entry '{}': {}",
                                    name, e
                                );
                            }
                        }
                    }

                    if !success {
                        return Err(ReductError::internal_server_error(
                            format!("Failed to keep quota of '{}'", self.name()).as_str(),
                        ));
                    }

                    size = self.info().await?.info.size + content_size as u64;
                }

                // Remove empty entries
                let mut names_to_remove = vec![];
                for (name, entry) in self.entries.iter_mut() {
                    if entry.info().await?.record_count != 0 {
                        continue;
                    }
                    names_to_remove.push(name.clone());
                }

                for name in names_to_remove {
                    self.entries.remove(&name);
                }
                Ok(())
            }
        }
    }

    /// Sync all entries to the file system
    pub async fn sync_fs(&self) -> Result<(), ReductError> {
        for entry in self.entries.values() {
            entry.sync_fs().await?;
        }
        Ok(())
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn settings(&self) -> &BucketSettings {
        &self.settings
    }

    pub fn set_settings(&mut self, settings: BucketSettings) -> Result<(), ReductError> {
        if self.is_provisioned {
            return Err(ReductError::conflict(&format!(
                "Can't change settings of provisioned bucket '{}'",
                self.name()
            )));
        }

        self.settings = Self::fill_settings(settings, self.settings.clone());
        for entry in self.entries.values_mut() {
            entry.set_settings(EntrySettings {
                max_block_size: self.settings.max_block_size.unwrap(),
                max_block_records: self.settings.max_block_records.unwrap(),
            });
        }
        self.save_settings()?;
        Ok(())
    }

    /// Mark bucket as provisioned to protect
    pub fn set_provisioned(&mut self, provisioned: bool) {
        self.is_provisioned = provisioned;
    }

    /// Check if bucket is provisioned
    pub fn is_provisioned(&self) -> bool {
        self.is_provisioned
    }

    /// Fill in missing settings with defaults
    fn fill_settings(settings: BucketSettings, default: BucketSettings) -> BucketSettings {
        let mut settings = settings;
        if settings.max_block_size.is_none() {
            settings.max_block_size = default.max_block_size;
        }
        if settings.quota_type.is_none() {
            settings.quota_type = default.quota_type;
        }
        if settings.quota_size.is_none() {
            settings.quota_size = default.quota_size;
        }
        if settings.max_block_records.is_none() {
            settings.max_block_records = default.max_block_records;
        }
        settings
    }

    fn save_settings(&self) -> Result<(), ReductError> {
        let path = self.path.join(SETTINGS_NAME);
        let mut buf = BytesMut::new();
        ProtoBucketSettings::from(self.settings.clone())
            .encode(&mut buf)
            .map_err(|e| {
                ReductError::internal_server_error(
                    format!("Failed to encode bucket settings: {}", e).as_str(),
                )
            })?;

        let mut file = std::fs::File::create(path)?;
        file.write(&buf)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::{fixture, rstest};
    use tempfile::tempdir;

    #[rstest]
    #[tokio::test]
    async fn test_keep_settings_persistent(settings: BucketSettings, bucket: Bucket) {
        assert_eq!(bucket.settings(), &settings);

        let bucket = Bucket::restore(bucket.path.clone()).await.unwrap();
        assert_eq!(bucket.name(), "test");
        assert_eq!(bucket.settings(), &settings);
    }

    #[rstest]
    fn test_fill_default_settings() {
        let settings = BucketSettings {
            max_block_size: None,
            quota_type: None,
            quota_size: None,
            max_block_records: None,
        };

        let default_settings = Bucket::defaults();
        let filled_settings = Bucket::fill_settings(settings, default_settings.clone());
        assert_eq!(filled_settings, default_settings);
    }

    #[rstest]
    fn test_set_settings_partially(settings: BucketSettings, mut bucket: Bucket) {
        let new_settings = BucketSettings {
            max_block_size: Some(100),
            quota_type: None,
            quota_size: None,
            max_block_records: None,
        };

        bucket.set_settings(new_settings).unwrap();
        assert_eq!(bucket.settings().max_block_size.unwrap(), 100);
        assert_eq!(bucket.settings().quota_type, settings.quota_type);
        assert_eq!(bucket.settings().quota_size, settings.quota_size);
        assert_eq!(
            bucket.settings().max_block_records,
            settings.max_block_records
        );
    }

    #[rstest]
    fn test_apply_settings_to_entries(settings: BucketSettings, mut bucket: Bucket) {
        bucket.get_or_create_entry("entry-1").unwrap();
        bucket.get_or_create_entry("entry-2").unwrap();

        let mut new_settings = settings.clone();
        new_settings.max_block_size = Some(200);
        new_settings.max_block_records = Some(200);
        bucket.set_settings(new_settings).unwrap();

        for entry in bucket.entries.values() {
            assert_eq!(entry.settings().max_block_size, 200);
            assert_eq!(entry.settings().max_block_records, 200);
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_quota_keeping(path: PathBuf) {
        let mut bucket = bucket(
            BucketSettings {
                max_block_size: Some(20),
                quota_type: Some(QuotaType::FIFO),
                quota_size: Some(100),
                max_block_records: Some(100),
            },
            path,
        );

        let blob: &[u8] = &[0u8; 40];

        write(&mut bucket, "test-1", 0, blob).await.unwrap();
        assert_eq!(bucket.info().await.unwrap().info.size, 44);

        write(&mut bucket, "test-2", 1, blob).await.unwrap();
        assert_eq!(bucket.info().await.unwrap().info.size, 91);

        write(&mut bucket, "test-3", 2, blob).await.unwrap();
        assert_eq!(bucket.info().await.unwrap().info.size, 94);

        assert_eq!(
            read(&mut bucket, "test-1", 0).await.err(),
            Some(ReductError::not_found(
                "Entry 'test-1' not found in bucket 'test'"
            ))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_blob_bigger_than_quota(path: PathBuf) {
        let mut bucket = bucket(
            BucketSettings {
                max_block_size: Some(5),
                quota_type: Some(QuotaType::FIFO),
                quota_size: Some(10),
                max_block_records: Some(100),
            },
            path,
        );

        write(&mut bucket, "test-1", 0, b"test").await.unwrap();
        bucket.sync_fs().await.unwrap(); // we need to sync to get the correct size
        assert_eq!(bucket.info().await.unwrap().info.size, 22);

        let result = write(&mut bucket, "test-2", 1, b"0123456789___").await;
        assert_eq!(
            result.err(),
            Some(ReductError::internal_server_error(
                "Failed to keep quota of 'test'"
            ))
        );
    }

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
    async fn test_remove_entry_not_found(mut bucket: Bucket) {
        assert_eq!(
            bucket.remove_entry("test-1").await.err(),
            Some(ReductError::not_found(
                "Entry 'test-1' not found in bucket 'test'"
            ))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_provisioned_info(provisioned_bucket: Bucket) {
        let info = provisioned_bucket.info().await.unwrap().info;
        assert_eq!(info.is_provisioned, true);
    }

    #[rstest]
    fn test_provisioned_settings(mut provisioned_bucket: Bucket) {
        let err = provisioned_bucket
            .set_settings(BucketSettings::default())
            .err()
            .unwrap();
        assert_eq!(
            err,
            ReductError::conflict("Can't change settings of provisioned bucket 'test'")
        );
    }

    async fn write(
        bucket: &mut Bucket,
        entry_name: &str,
        time: u64,
        content: &'static [u8],
    ) -> Result<(), ReductError> {
        let sender = bucket
            .write_record(
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
            .map_err(|e| {
                ReductError::internal_server_error(format!("Failed to send data: {}", e).as_str())
            })?;
        sender.tx().closed().await;
        Ok(())
    }

    async fn read(
        bucket: &mut Bucket,
        entry_name: &str,
        time: u64,
    ) -> Result<Vec<u8>, ReductError> {
        let mut reader = bucket.begin_read(entry_name, time).await?;
        let data = reader.rx().recv().await.unwrap().unwrap();
        Ok(data.to_vec())
    }

    #[fixture]
    fn settings() -> BucketSettings {
        BucketSettings {
            max_block_size: Some(100),
            quota_type: Some(QuotaType::FIFO),
            quota_size: Some(1000),
            max_block_records: Some(100),
        }
    }

    #[fixture]
    fn path() -> PathBuf {
        tempdir().unwrap().into_path()
    }

    #[fixture]
    fn bucket(settings: BucketSettings, path: PathBuf) -> Bucket {
        Bucket::new("test", &path, settings).unwrap()
    }

    #[fixture]
    fn provisioned_bucket(settings: BucketSettings, path: PathBuf) -> Bucket {
        let mut bucket = Bucket::new("test", &path, settings).unwrap();
        bucket.set_provisioned(true);
        bucket
    }
}
