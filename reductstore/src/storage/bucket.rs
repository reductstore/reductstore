// Copyright 2023-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

mod query;
mod quotas;
mod read_only;
mod remove_records;
pub(super) mod settings;
pub(crate) mod update_records;

use crate::cfg::{Cfg, InstanceRole};
use crate::core::file_cache::FILE_CACHE;
use crate::core::sync::RwLock;
use crate::core::thread_pool::{spawn, TaskHandle};
use crate::core::weak::Weak;
pub use crate::storage::block_manager::RecordRx;
pub use crate::storage::block_manager::RecordTx;
use crate::storage::bucket::query::MultiEntryQuery;
use crate::storage::bucket::settings::{
    DEFAULT_MAX_BLOCK_SIZE, DEFAULT_MAX_RECORDS, SETTINGS_NAME,
};
use crate::storage::engine::{check_name_convention, ReadOnlyMode};
use crate::storage::entry::{Entry, EntrySettings, RecordReader};
use crate::storage::folder_keeper::FolderKeeper;
use crate::storage::proto::BucketSettings as ProtoBucketSettings;
use log::{debug, error};
use prost::bytes::Bytes;
use prost::Message;
use reduct_base::error::ReductError;
use reduct_base::io::WriteRecord;
use reduct_base::msg::bucket_api::{BucketInfo, BucketSettings, FullBucketInfo};
use reduct_base::msg::status::ResourceStatus;
use reduct_base::{conflict, internal_server_error, not_found, Labels};
use reduct_macros::task;
use std::collections::{BTreeMap, HashMap};
use std::io::{Read, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Bucket is a single storage bucket.
pub(crate) struct Bucket {
    name: String,
    path: PathBuf,
    entries: Arc<RwLock<BTreeMap<String, Arc<Entry>>>>,
    settings: RwLock<BucketSettings>,
    folder_keeper: Arc<FolderKeeper>,
    cfg: Arc<Cfg>,
    last_replica_sync: RwLock<Instant>,
    is_provisioned: AtomicBool,
    status: RwLock<ResourceStatus>,
    #[allow(dead_code)]
    queries: RwLock<HashMap<u64, MultiEntryQuery>>,
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
        cfg: Cfg,
    ) -> Result<Bucket, ReductError> {
        let path = path.join(name);
        let settings = Self::fill_settings(settings, Self::defaults());
        let folder_keeper = FolderKeeper::new(path.clone(), &cfg);

        let bucket = Bucket {
            name: name.to_string(),
            path,
            entries: Arc::new(RwLock::new(BTreeMap::new())),
            settings: RwLock::new(settings),
            is_provisioned: AtomicBool::new(false),
            status: RwLock::new(ResourceStatus::Ready),
            cfg: Arc::new(cfg),
            last_replica_sync: RwLock::new(Instant::now()),
            folder_keeper: Arc::new(folder_keeper),
            queries: RwLock::new(HashMap::new()),
        };

        bucket.save_settings().wait()?;
        Ok(bucket)
    }

    /// Restore a Bucket from disk
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the bucket
    /// * `cfg` - The global configuration
    ///
    /// # Returns
    ///
    /// * `Bucket` - The bucket or an HTTPError
    pub fn restore(path: PathBuf, cfg: Cfg) -> Result<Bucket, ReductError> {
        let buf = {
            let lock = FILE_CACHE
                .read(&path.join(SETTINGS_NAME), SeekFrom::Start(0))?
                .upgrade()?;
            let mut buf = Vec::new();
            lock.write()?.read_to_end(&mut buf)?;
            buf
        };

        let cfg = Arc::new(cfg);
        let settings = ProtoBucketSettings::decode(&mut Bytes::from(buf))
            .map_err(|e| internal_server_error!("Failed to decode settings: {}", e))?;

        let settings = Self::fill_settings(settings.into(), Self::defaults());
        let bucket_name = path.file_name().unwrap().to_str().unwrap().to_string();

        let mut entries = BTreeMap::new();
        let mut task_set = Vec::new();
        let folder_keeper = FolderKeeper::new(path.clone(), &cfg);

        for path in folder_keeper.list_folders()? {
            let handler = Entry::restore(
                path,
                EntrySettings {
                    max_block_size: settings.max_block_size.unwrap(),
                    max_block_records: settings.max_block_records.unwrap(),
                },
                cfg.clone(),
            );

            task_set.push(handler);
        }

        for task in task_set {
            if let Some(entry) = task.wait()? {
                entries.insert(entry.name().to_string(), entry);
            }
        }

        Ok(Bucket {
            name: bucket_name,
            path,
            entries: Arc::new(RwLock::new(
                entries.into_iter().map(|(k, v)| (k, Arc::new(v))).collect(),
            )),
            settings: RwLock::new(settings),
            is_provisioned: AtomicBool::new(false),
            status: RwLock::new(ResourceStatus::Ready),
            last_replica_sync: RwLock::new(Instant::now()),
            cfg,
            folder_keeper: Arc::new(folder_keeper),
            queries: RwLock::new(HashMap::new()),
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
        check_name_convention(key)?;
        self.ensure_not_deleting()?;

        let entry = {
            let entries = self.entries.read()?;
            entries.get(key).cloned()
        };

        let entry = if let Some(entry) = entry {
            entry
        } else {
            self.check_mode()?;
            let settings = self.settings.read()?;
            self.folder_keeper.add_folder(key)?;
            let entry = Arc::new(Entry::try_new(
                &key,
                self.path.clone(),
                EntrySettings {
                    max_block_size: settings.max_block_size.unwrap(),
                    max_block_records: settings.max_block_records.unwrap(),
                },
                self.cfg.clone(),
            )?);
            let mut entries = self.entries.write()?;
            entries
                .entry(key.to_string())
                .or_insert_with(|| Arc::clone(&entry))
                .clone()
        };
        entry.ensure_not_deleting()?;
        Ok(entry.into())
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
        self.reload()?;
        let entries = self.entries.read()?;
        let entry = entries.get(name).ok_or_else(|| {
            ReductError::not_found(&format!(
                "Entry '{}' not found in bucket '{}'",
                name, self.name
            ))
        })?;
        entry.ensure_not_deleting()?;
        Ok(entry.clone().into())
    }

    /// Return bucket stats
    #[task("bucket info")]
    pub fn info(self: Arc<Self>) -> Result<FullBucketInfo, ReductError> {
        self.reload()?;

        let mut size = 0;
        let mut oldest_record = u64::MAX;
        let mut latest_record = 0u64;
        let mut entry_infos = Vec::new();

        let infos = self
            .entries
            .read()?
            .values()
            .into_iter()
            .map(|entry| entry.info())
            .collect::<Vec<_>>();
        for info in infos {
            let info = info?;
            size += info.size;
            oldest_record = oldest_record.min(info.oldest_record);
            latest_record = latest_record.max(info.latest_record);
            entry_infos.push(info);
        }

        Ok(FullBucketInfo {
            info: BucketInfo {
                name: self.name.clone(),
                size,
                entry_count: entry_infos.len() as u64,
                oldest_record,
                latest_record,
                is_provisioned: self.is_provisioned.load(Ordering::Relaxed),
                status: self.status()?,
            },
            settings: self.settings.read()?.clone(),
            entries: entry_infos,
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
        self: &Arc<Self>,
        name: &str,
        time: u64,
        content_size: u64,
        content_type: String,
        labels: Labels,
    ) -> TaskHandle<Result<Box<dyn WriteRecord + Sync + Send>, ReductError>> {
        if let Err(e) = self.check_mode() {
            return Err(e).into();
        }

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
        if let Err(e) = self.check_mode() {
            return Err(e).into();
        }

        let new_path = self.path.join(new_name);
        let bucket_name = self.name.clone();
        let entries = self.entries.clone();
        let old_name = old_name.to_string();
        let new_name = new_name.to_string();
        let settings = self.settings();
        let cfg = self.cfg.clone();
        let folder_keeper = self.folder_keeper.clone();

        spawn("rename entry", move || {
            check_name_convention(&new_name)?;
            if new_path.exists() {
                return Err(conflict!(
                    "Entry '{}' already exists in bucket '{}'",
                    new_name,
                    bucket_name
                ));
            }

            if let Some(entry) = entries.read()?.get(&old_name) {
                entry.ensure_not_deleting()?;
                entry.compact()?;
            } else {
                return Err(not_found!(
                    "Entry '{}' not found in bucket '{}'",
                    old_name,
                    bucket_name
                ));
            }

            folder_keeper.rename_folder(&old_name, &new_name)?;
            entries.write()?.remove(&old_name);

            if let Some(entry) = Entry::restore(
                new_path,
                EntrySettings {
                    max_block_size: settings.max_block_size.unwrap_or(DEFAULT_MAX_BLOCK_SIZE),
                    max_block_records: settings.max_block_records.unwrap_or(DEFAULT_MAX_RECORDS),
                },
                cfg.clone(),
            )
            .wait()?
            {
                entries
                    .write()?
                    .insert(new_name.to_string(), Arc::new(entry));
            }
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
    pub fn remove_entry(&self, name: &str) -> Result<(), ReductError> {
        self.ensure_not_deleting()?;
        self.check_mode()?;

        let entry = self.get_entry(name)?.upgrade()?;
        entry.mark_deleting()?;

        let entries = self.entries.clone();
        let path = self.path.join(name);
        let bucket_name = self.name.clone();
        let entry_name = name.to_string();
        let folder_keeper = self.folder_keeper.clone();

        let folder_remove_result = {
            let remove_bucket_from_backend = || -> Result<(), ReductError> {
                folder_keeper.remove_folder(&entry_name)?;
                debug!(
                    "Remove entry '{}' from bucket '{}' and folder '{}'",
                    entry_name,
                    bucket_name,
                    path.display()
                );
                Ok(())
            };

            remove_bucket_from_backend()
        };

        match folder_remove_result {
            Ok(()) => {
                entries.write()?.remove(&entry_name);
            }
            Err(err) => {
                error!(
                    "Failed to remove entry '{}' from bucket '{}': {}",
                    entry_name, bucket_name, err
                );
                return Err(err);
            }
        }

        Ok(())
    }

    /// Sync all entries to the file system
    pub fn sync_fs(&self) -> TaskHandle<Result<(), ReductError>> {
        if self.cfg.role == InstanceRole::Replica {
            return Ok(()).into();
        }

        let bucket_name = self.name.clone();
        debug!("Syncing bucket '{}'", bucket_name);
        let time_start = Instant::now();

        if let Err(e) = self.save_settings().wait() {
            return Err(e).into();
        }

        let entries = self.entries.clone();
        // use shared task to avoid locking in graceful shutdown
        spawn("sync entries", move || {
            let mut count = 0usize;
            for entry in entries.read()?.values() {
                entry.compact()?;
                count += 1;
            }
            debug!(
                "Bucket '{}' synced {} entries in {:?}",
                bucket_name,
                count,
                Instant::now().duration_since(time_start)
            );
            Ok(())
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
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

    #[allow(dead_code)]
    pub fn entries(&self) -> Result<BTreeMap<String, Arc<Entry>>, ReductError> {
        Ok(self.entries.read()?.clone())
    }

    pub(super) fn status(&self) -> Result<ResourceStatus, ReductError> {
        Ok(*self.status.read()?)
    }

    pub(super) fn mark_deleting(&self) -> Result<(), ReductError> {
        self.ensure_not_deleting()?;
        *self.status.write()? = ResourceStatus::Deleting;
        for entry in self.entries.read()?.values() {
            entry.mark_deleting()?;
        }
        Ok(())
    }

    pub(super) fn ensure_not_deleting(&self) -> Result<(), ReductError> {
        if self.status()? == ResourceStatus::Deleting {
            Err(conflict!("Bucket '{}' is being deleted", self.name))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Backend;
    use reduct_base::conflict;
    use reduct_base::io::ReadRecord;
    use reduct_base::msg::bucket_api::QuotaType;
    use reduct_base::msg::status::ResourceStatus;
    use rstest::{fixture, rstest};
    use std::sync::Arc;
    use tempfile::tempdir;

    mod get_or_create {
        use super::*;
        use reduct_base::unprocessable_entity;

        #[rstest]
        #[tokio::test]
        async fn test_get_or_create_entry(bucket: Arc<Bucket>) {
            let entry = bucket.get_or_create_entry("test-1").unwrap();
            assert_eq!(entry.upgrade().unwrap().name(), "test-1");
        }

        #[rstest]
        #[tokio::test]
        async fn test_get_or_create_entry_invalid_name(bucket: Arc<Bucket>) {
            assert_eq!(
                bucket.get_or_create_entry("test-1/").err(),
                Some(unprocessable_entity!(
                    "Bucket or entry name can contain only letters, digests and [-,_] symbols"
                ))
            );
        }
    }

    mod remove_entry {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_remove_entry(bucket: Arc<Bucket>) {
            write(&bucket, "test-1", 1, b"test").await.unwrap();

            bucket.remove_entry("test-1").unwrap();
            let err = bucket.get_entry("test-1").err().unwrap();
            assert!(
                err == ReductError::conflict("Entry 'test-1' in bucket 'test' is being deleted")
                    || err == ReductError::not_found("Entry 'test-1' not found in bucket 'test'"),
                "Should report deleting or already removed"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_remove_entry_not_found(bucket: Arc<Bucket>) {
            assert_eq!(
                bucket.remove_entry("test-1").err(),
                Some(ReductError::not_found(
                    "Entry 'test-1' not found in bucket 'test'"
                ))
            );
        }
    }

    mod rename_entry {
        use super::*;
        use reduct_base::unprocessable_entity;

        #[rstest]
        #[tokio::test]
        async fn test_rename_entry(bucket: Arc<Bucket>) {
            write(&bucket, "test-1", 1, b"test").await.unwrap();

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
        async fn test_rename_entry_not_found(bucket: Arc<Bucket>) {
            assert_eq!(
                bucket.rename_entry("test-1", "test-2").await.err(),
                Some(not_found!("Entry 'test-1' not found in bucket 'test'"))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_rename_entry_already_exists(bucket: Arc<Bucket>) {
            write(&bucket, "test-1", 1, b"test").await.unwrap();
            write(&bucket, "test-2", 1, b"test").await.unwrap();

            assert_eq!(
                bucket.rename_entry("test-1", "test-2").await.err(),
                Some(conflict!("Entry 'test-2' already exists in bucket 'test'"))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_rename_invalid_name(bucket: Arc<Bucket>) {
            assert_eq!(
                bucket.rename_entry("test-1", "test-2/").await.err(),
                Some(unprocessable_entity!(
                    "Bucket or entry name can contain only letters, digests and [-,_] symbols"
                ))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_rename_entry_persisted(bucket: Arc<Bucket>) {
            write(&bucket, "test-1", 1, b"test").await.unwrap();
            bucket.sync_fs().await.unwrap();
            bucket.rename_entry("test-1", "test-2").await.unwrap();

            let bucket = Bucket::restore(bucket.path.clone(), Cfg::default()).unwrap();
            assert_eq!(
                bucket.get_entry("test-1").err(),
                Some(ReductError::not_found(
                    "Entry 'test-1' not found in bucket 'test'"
                ))
            );

            let mut reader = bucket.begin_read("test-2", 1).await.unwrap();
            assert_eq!(reader.read_chunk().unwrap().unwrap(), Bytes::from("test"));
        }
    }

    mod status {
        use super::*;

        #[rstest]
        fn test_bucket_info_has_status(bucket: Arc<Bucket>) {
            let info = bucket.info().wait().unwrap();
            assert_eq!(info.info.status, ResourceStatus::Ready);
            assert!(info
                .entries
                .iter()
                .all(|entry| entry.status == ResourceStatus::Ready));
        }

        #[rstest]
        fn test_bucket_deleting_rejects_operations(bucket: Arc<Bucket>) {
            bucket.mark_deleting().unwrap();
            let err = bucket.get_or_create_entry("new-entry").err().unwrap();
            assert_eq!(err, conflict!("Bucket 'test' is being deleted"));
        }

        #[rstest]
        fn bucket_mark_deleting_returns_conflict_when_already_deleting(bucket: Arc<Bucket>) {
            bucket.mark_deleting().unwrap();
            assert_eq!(
                bucket.mark_deleting(),
                Err(conflict!("Bucket 'test' is being deleted"))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_entry_deleting_rejects_operations(bucket: Arc<Bucket>) {
            write(&bucket, "test-1", 1, b"test").await.unwrap();
            let entry = bucket.get_entry("test-1").unwrap().upgrade().unwrap();
            entry.mark_deleting().unwrap();

            let err = bucket.begin_read("test-1", 1).await.err().unwrap();
            assert_eq!(
                err,
                conflict!("Entry 'test-1' in bucket 'test' is being deleted")
            );
        }

        #[rstest]
        #[tokio::test]
        async fn entry_mark_deleting_returns_conflict_when_already_deleting(bucket: Arc<Bucket>) {
            write(&bucket, "test-1", 1, b"test").await.unwrap();
            let entry = bucket.get_entry("test-1").unwrap().upgrade().unwrap();
            entry.mark_deleting().unwrap();
            assert_eq!(
                entry.mark_deleting(),
                Err(conflict!(
                    "Entry 'test-1' in bucket 'test' is being deleted"
                ))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn remove_entry_returns_conflict_when_entry_is_being_deleted(bucket: Arc<Bucket>) {
            write(&bucket, "test-1", 1, b"test").await.unwrap();
            let entry = bucket.get_entry("test-1").unwrap().upgrade().unwrap();
            entry.mark_deleting().unwrap();

            assert_eq!(
                bucket.remove_entry("test-1"),
                Err(conflict!(
                    "Entry 'test-1' in bucket 'test' is being deleted"
                ))
            );
        }
    }

    #[rstest]
    fn test_provisioned_info(provisioned_bucket: Arc<Bucket>) {
        let info = provisioned_bucket.info().wait().unwrap().info;
        assert_eq!(info.is_provisioned, true);
    }

    #[rstest]
    fn test_provisioned_settings(provisioned_bucket: Arc<Bucket>) {
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

    pub async fn read(
        bucket: &Arc<Bucket>,
        entry_name: &str,
        time: u64,
    ) -> Result<Vec<u8>, ReductError> {
        let mut reader = bucket.begin_read(entry_name, time).await?;
        let data = reader.read_chunk().unwrap().unwrap();
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
        let path = tempdir().unwrap().keep();
        FILE_CACHE.set_storage_backend(
            Backend::builder()
                .local_data_path(path.clone())
                .try_build()
                .unwrap(),
        );
        path
    }

    #[fixture]
    pub fn bucket(settings: BucketSettings, path: PathBuf) -> Arc<Bucket> {
        FILE_CACHE.create_dir_all(&path.join("test")).unwrap();
        Arc::new(Bucket::new("test", &path, settings, Cfg::default()).unwrap())
    }

    #[fixture]
    pub fn provisioned_bucket(settings: BucketSettings, path: PathBuf) -> Arc<Bucket> {
        FILE_CACHE.create_dir_all(&path.join("test")).unwrap();
        let bucket = Bucket::new("test", &path, settings, Cfg::default()).unwrap();
        bucket.set_provisioned(true);
        Arc::new(bucket)
    }
}
