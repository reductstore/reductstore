// Copyright 2023 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::core::thread_pool::{unique, TaskHandle};
use crate::core::weak::Weak;
pub use crate::storage::block_manager::RecordRx;
pub use crate::storage::block_manager::RecordTx;
use crate::storage::entry::{Entry, EntrySettings, WriteRecordContent};
use crate::storage::file_cache::FILE_CACHE;
use crate::storage::proto::BucketSettings as ProtoBucketSettings;
use log::debug;
use prost::bytes::{Bytes, BytesMut};
use prost::Message;
use reduct_base::error::ReductError;
use reduct_base::msg::bucket_api::{BucketInfo, BucketSettings, FullBucketInfo, QuotaType};
use reduct_base::msg::entry_api::EntryInfo;
use reduct_base::{bad_request, conflict, internal_server_error, Labels};
use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};

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
                let entry_name = path.file_name().unwrap().to_str().unwrap();
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
    pub fn write_record(
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

    /// Remove entry from the bucket
    ///
    /// # Arguments
    ///
    /// * `name` - Entry name.
    ///
    /// # Returns
    ///
    /// * `HTTPError` - The error if any.
    pub fn remove_entry(&self, name: &str) -> Result<(), ReductError> {
        _ = self.get_entry(name)?;

        let path = self.path.join(name);
        FILE_CACHE.remove_dir(&path)?;
        debug!(
            "Remove entry '{}' from bucket '{}' and folder '{}'",
            name,
            self.name,
            path.display()
        );

        self.entries.write()?.remove(name);
        Ok(())
    }

    fn keep_quota_for(&self, content_size: usize) -> Result<(), ReductError> {
        let settings = self.settings.read()?;
        let quota_size = settings.quota_size.unwrap_or(0);
        match settings.quota_type.clone().unwrap_or(QuotaType::NONE) {
            QuotaType::NONE => Ok(()),
            QuotaType::FIFO => self.remove_oldest_block(content_size, quota_size),
            QuotaType::HARD => {
                if self.info()?.info.size + content_size as u64 > quota_size {
                    Err(bad_request!("Quota of '{}' exceeded", self.name()))
                } else {
                    Ok(())
                }
            }
        }
    }

    fn remove_oldest_block(&self, content_size: usize, quota_size: u64) -> Result<(), ReductError> {
        let mut size = self.info()?.info.size + content_size as u64;
        while size > quota_size {
            debug!(
                "Need more space. Remove an oldest block from bucket '{}'",
                self.name()
            );

            let mut candidates: Vec<(u64, &Entry)> = vec![];
            let entries = self.entries.read()?;
            for (_, entry) in entries.iter() {
                let info = entry.info()?;
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
                match entries.get(&name).unwrap().try_remove_oldest_block().wait() {
                    Ok(_) => {
                        success = true;
                        break;
                    }
                    Err(e) => {
                        debug!("Failed to remove oldest block from entry '{}': {}", name, e);
                    }
                }
            }

            if !success {
                return Err(internal_server_error!(
                    "Failed to keep quota of '{}'",
                    self.name()
                ));
            }

            size = self.info()?.info.size + content_size as u64;
        }

        // Remove empty entries
        let mut entries = self.entries.write()?;
        let mut names_to_remove = vec![];
        for (name, entry) in entries.iter() {
            if entry.info()?.record_count != 0 {
                continue;
            }
            names_to_remove.push(name.clone());
        }

        for name in names_to_remove {
            entries.remove(&name);
        }
        Ok(())
    }

    /// Sync all entries to the file system
    pub fn sync_fs(&self) -> TaskHandle<Result<(), ReductError>> {
        let entries = self.entries.clone();
        unique(["storage", self.name()], move || {
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

    pub fn settings(&self) -> BucketSettings {
        self.settings.read().unwrap().clone()
    }

    pub fn set_settings(&self, settings: BucketSettings) -> TaskHandle<Result<(), ReductError>> {
        if self
            .is_provisioned
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Err(conflict!(
                "Can't change settings of provisioned bucket '{}'",
                self.name()
            ))
            .into();
        }

        {
            let mut my_settings = self.settings.write().unwrap();
            let entries = self.entries.write().unwrap();

            *my_settings = Self::fill_settings(settings, my_settings.clone());
            for entry in entries.values() {
                entry.set_settings(EntrySettings {
                    max_block_size: my_settings.max_block_size.unwrap(),
                    max_block_records: my_settings.max_block_records.unwrap(),
                });
            }
        }
        self.save_settings()
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

    fn save_settings(&self) -> TaskHandle<Result<(), ReductError>> {
        let settings = self.settings.read().unwrap().clone();
        let path = self.path.join(SETTINGS_NAME);

        unique(["storage", &self.name], move || {
            let mut buf = BytesMut::new();
            ProtoBucketSettings::from(settings)
                .encode(&mut buf)
                .map_err(|e| internal_server_error!("Failed to encode bucket settings: {}", e))?;

            let mut file = std::fs::File::create(path)?;
            file.write(&buf)?;
            Ok(())
        })
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
    async fn test_fifo_quota_keeping(path: PathBuf) {
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
    async fn test_hard_quota_keeping(path: PathBuf) {
        let mut bucket = bucket(
            BucketSettings {
                quota_type: Some(QuotaType::HARD),
                quota_size: Some(100),
                ..BucketSettings::default()
            },
            path,
        );

        let blob: &[u8] = &[0u8; 40];
        write(&mut bucket, "test-1", 0, blob).await.unwrap();
        write(&mut bucket, "test-2", 1, blob).await.unwrap();

        let err = write(&mut bucket, "test-3", 2, blob).await.err().unwrap();
        assert_eq!(err, ReductError::bad_request("Quota of 'test' exceeded"));
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
