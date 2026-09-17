// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

mod builder;
mod compress_blocks;
mod get_entry;
mod query;
mod quotas;
mod read_only;
mod remove_entry;
mod remove_records;
mod rename_entry;
pub(super) mod settings;
pub(crate) mod update_records;

use crate::cfg::{Cfg, InstanceRole};
use crate::core::file_cache::FILE_CACHE;
use crate::core::sync::AsyncRwLock;
use crate::core::weak::Weak;
pub use crate::storage::block_manager::RecordRx;
pub use crate::storage::block_manager::RecordTx;
use crate::storage::bucket::query::MultiEntryQuery;
use crate::storage::bucket::settings::{DEFAULT_MAX_BLOCK_SIZE, DEFAULT_MAX_RECORDS};
use crate::storage::entry::{
    is_system_meta_entry, Entry, EntrySettings, META_ENTRY_MAX_BLOCK_SIZE,
};
use crate::storage::folder_keeper::FolderKeeper;
use crate::storage::in_flight::InFlightIoLimiter;
use crate::storage::usage::UsageCounters;
use log::{debug, error};
use reduct_base::error::ReductError;
use reduct_base::io::WriteRecord;
use reduct_base::msg::bucket_api::{BucketInfo, BucketSettings, FullBucketInfo};
use reduct_base::msg::status::ResourceStatus;
use reduct_base::{conflict, forbidden, Labels};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

pub(crate) use builder::BucketBuilder;

fn normalize_entry_name(path: &std::path::Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

/// A function that returns the number of bytes available for writing on the
/// filesystem that contains the given path.
///
/// It is injectable so the write-admission logic can be tested deterministically
/// without depending on the real free space of the host filesystem.
pub(crate) type FreeSpaceFn = Arc<dyn Fn(&std::path::Path) -> std::io::Result<u64> + Send + Sync>;

/// Default free-space provider backed by the host filesystem containing the data folder.
pub(crate) fn default_free_space_fn() -> FreeSpaceFn {
    Arc::new(|path| fs4::available_space(path))
}

fn settings_for_entry(entry_name: &str, settings: &BucketSettings) -> EntrySettings {
    EntrySettings {
        max_block_size: if is_system_meta_entry(entry_name) {
            META_ENTRY_MAX_BLOCK_SIZE
        } else {
            settings.max_block_size.unwrap_or(DEFAULT_MAX_BLOCK_SIZE)
        },
        max_block_records: settings.max_block_records.unwrap_or(DEFAULT_MAX_RECORDS),
    }
}

/// Bucket is a single storage bucket.
pub(crate) struct Bucket {
    name: String,
    path: PathBuf,
    entries: Arc<AsyncRwLock<BTreeMap<String, Arc<Entry>>>>,
    settings: AsyncRwLock<BucketSettings>,
    folder_keeper: Arc<FolderKeeper>,
    cfg: Arc<Cfg>,
    is_provisioned: AtomicBool,
    status: AsyncRwLock<ResourceStatus>,
    #[allow(dead_code)]
    queries: AsyncRwLock<HashMap<u64, MultiEntryQuery>>,
    io_limiter: InFlightIoLimiter,
    usage_counters: Arc<UsageCounters>,
    free_space_fn: FreeSpaceFn,
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum EntryMaintenanceMode {
    Compact,
    SyncFs,
}

impl Bucket {
    pub(crate) fn builder() -> BucketBuilder {
        BucketBuilder::default()
    }

    #[cfg(test)]
    pub(crate) fn cfg(&self) -> &Cfg {
        &self.cfg
    }

    fn check_mode(&self) -> Result<(), ReductError> {
        if self.cfg.role == InstanceRole::Replica {
            return Err(forbidden!(
                "Cannot perform this operation in read-only mode"
            ));
        }

        Ok(())
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
    pub async fn get_entry(&self, name: &str) -> Result<Weak<Entry>, ReductError> {
        self.reload().await?;
        let entries = self.entries.read().await?;
        let entry = entries.get(name).ok_or_else(|| {
            ReductError::not_found(&format!(
                "Entry '{}' not found in bucket '{}'",
                name, self.name
            ))
        })?;
        entry.ensure_not_deleting().await?;
        Ok(entry.clone().into())
    }

    /// Return bucket stats
    pub async fn info(self: Arc<Self>) -> Result<FullBucketInfo, ReductError> {
        self.reload().await?;

        let mut size = 0;
        let mut oldest_record = u64::MAX;
        let mut latest_record = 0u64;
        let mut entry_infos = Vec::new();

        let entries_guard = self.entries.read().await?;
        let entries = entries_guard.values().cloned().collect::<Vec<_>>();
        drop(entries_guard);

        for entry in entries {
            let info = entry.info().await?;
            size += info.size;

            if entry.is_visible_in_bucket_info() {
                if info.record_count > 0 {
                    oldest_record = oldest_record.min(info.oldest_record);
                    latest_record = latest_record.max(info.latest_record);
                }
                entry_infos.push(info);
            }
        }

        if oldest_record == u64::MAX {
            oldest_record = 0;
        }

        Ok(FullBucketInfo {
            info: BucketInfo {
                name: self.name.clone(),
                size,
                entry_count: entry_infos.len() as u64,
                oldest_record,
                latest_record,
                is_provisioned: self.is_provisioned.load(Ordering::Relaxed),
                status: self.status().await?,
            },
            settings: self.settings.read().await?.clone(),
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
    #[cfg(not(test))]
    pub(in crate::storage) async fn begin_write(
        self: &Arc<Self>,
        name: &str,
        time: u64,
        content_size: u64,
        content_type: String,
        labels: Labels,
    ) -> Result<Box<dyn WriteRecord + Sync + Send>, ReductError> {
        self.begin_write_impl(name, time, content_size, content_type, labels)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn begin_write(
        self: &Arc<Self>,
        name: &str,
        time: u64,
        content_size: u64,
        content_type: String,
        labels: Labels,
    ) -> Result<Box<dyn WriteRecord + Sync + Send>, ReductError> {
        self.begin_write_impl(name, time, content_size, content_type, labels)
            .await
    }

    async fn begin_write_impl(
        self: &Arc<Self>,
        name: &str,
        time: u64,
        content_size: u64,
        content_type: String,
        labels: Labels,
    ) -> Result<Box<dyn WriteRecord + Sync + Send>, ReductError> {
        self.check_mode()?;
        crate::storage::engine::check_entry_name_convention(name)?;
        self.ensure_not_deleting().await?;
        self.ensure_entry_path_not_deleting(name).await?;

        let get_entry = async || {
            self.keep_quota_for(content_size).await?;
            self.check_free_disk_space(content_size).await?;
            self.get_or_create_entry(name).await?.upgrade()
        };

        let entry = match get_entry().await {
            Ok(entry) => entry,
            Err(e) => {
                return Err(e).into();
            }
        };

        entry
            .begin_write(time, content_size, content_type, labels)
            .await
    }

    async fn ensure_entry_path_not_deleting(&self, name: &str) -> Result<(), ReductError> {
        let entries_in_path = {
            let entries = self.entries.read().await?;
            Self::parent_prefixes(name)
                .into_iter()
                .filter_map(|prefix| entries.get(&prefix).cloned())
                .collect::<Vec<_>>()
        };

        for entry in entries_in_path {
            entry.ensure_not_deleting().await?;
        }

        Ok(())
    }

    /// Starts a new record read with
    #[cfg(test)]
    pub async fn begin_read(
        &self,
        name: &str,
        time: u64,
    ) -> Result<crate::storage::entry::RecordReader, ReductError> {
        match self.get_entry(name).await?.upgrade() {
            Ok(entry) => entry.begin_read(time).await,
            Err(e) => Err(e).into(),
        }
    }

    /// Compact all entries in the bucket without blocking active writers.
    pub async fn compact(&self) -> Result<(), ReductError> {
        debug!("Compact metainformation in bucket '{}'", self.name);
        self.run_entry_maintenance(EntryMaintenanceMode::Compact)
            .await
    }

    /// Sync all entries to the file system.
    ///
    /// This method waits for active writers and should be used only for strict
    /// durability points (for example graceful shutdown).
    pub async fn sync_fs(&self) -> Result<(), ReductError> {
        debug!("Sync bucket '{}'with backend", self.name);
        self.run_entry_maintenance(EntryMaintenanceMode::SyncFs)
            .await
    }

    async fn run_entry_maintenance(&self, mode: EntryMaintenanceMode) -> Result<(), ReductError> {
        if self.cfg.role == InstanceRole::Replica {
            return Ok(());
        }

        let bucket_name = self.name.clone();
        let time_start = Instant::now();

        self.save_settings().await?;

        let entries = self
            .entries
            .read()
            .await?
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut count = 0usize;
        for entry in entries {
            if entry.status().await? == ResourceStatus::Deleting {
                debug!(
                    "Skipping compact/sync for deleting entry '{}' in bucket '{}'",
                    entry.name(),
                    bucket_name
                );
                continue;
            }

            if !FILE_CACHE.try_exists(entry.path()).await? {
                debug!(
                    "Remove stale entry '{}' from bucket '{}' because its folder is missing",
                    entry.name(),
                    bucket_name
                );
                if let Err(err) = self.folder_keeper.remove_folder(entry.name()).await {
                    if err.status() != reduct_base::error::ErrorCode::NotFound {
                        error!(
                            "Failed to remove stale folder map entry '{}' from bucket '{}': {}",
                            entry.name(),
                            bucket_name,
                            err
                        );
                    }
                }
                self.entries.write().await?.remove(entry.name());
                continue;
            }

            let result = match mode {
                EntryMaintenanceMode::Compact => entry.compact().await,
                EntryMaintenanceMode::SyncFs => entry.sync_fs().await,
            };
            if let Err(err) = result {
                error!(
                    "Failed to compact/sync entry '{}' in bucket '{}': {}",
                    entry.name(),
                    bucket_name,
                    err
                );
            }
            count += 1;
        }
        debug!(
            "Bucket '{}' processed {} entries in {:?}",
            bucket_name,
            count,
            Instant::now().duration_since(time_start)
        );

        Ok(())
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

    pub(super) async fn status(&self) -> Result<ResourceStatus, ReductError> {
        Ok(*self.status.read().await?)
    }

    pub(super) async fn mark_deleting(&self) -> Result<(), ReductError> {
        self.ensure_not_deleting().await?;
        *self.status.write().await? = ResourceStatus::Deleting;
        for entry in self.entries.read().await?.values() {
            entry.mark_deleting().await?;
        }
        Ok(())
    }

    pub(super) async fn ensure_not_deleting(&self) -> Result<(), ReductError> {
        if self.status().await? == ResourceStatus::Deleting {
            Err(conflict!("Bucket '{}' is being deleted", self.name))
        } else {
            Ok(())
        }
    }

    fn parent_prefixes(key: &str) -> Vec<String> {
        let mut prefixes = Vec::new();
        let mut current = String::new();
        for segment in key.split('/') {
            if !current.is_empty() {
                current.push('/');
            }
            current.push_str(segment);
            prefixes.push(current.clone());
        }
        prefixes
    }

    fn entry_with_descendants<'a>(entry_name: &'a str, candidate: &'a str) -> bool {
        candidate == entry_name
            || candidate
                .strip_prefix(entry_name)
                .is_some_and(|suffix| suffix.starts_with('/'))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use bytes::Bytes;
    use reduct_base::io::ReadRecord;
    use reduct_base::msg::bucket_api::QuotaType;
    use reduct_base::msg::status::ResourceStatus;
    use reduct_base::{conflict, internal_server_error};
    use rstest::{fixture, rstest};
    use std::sync::Arc;
    use tempfile::tempdir;

    mod status {
        use super::*;
        use crate::storage::bucket::update_records::UpdateLabelsMulti;
        use reduct_base::msg::entry_api::QueryEntry;
        use std::collections::HashSet;

        #[rstest]
        #[tokio::test]
        async fn test_bucket_info_has_status(#[future] bucket: Arc<Bucket>) {
            let bucket = bucket.await;
            let info = bucket.info().await.unwrap();
            assert_eq!(info.info.status, ResourceStatus::Ready);
            assert!(info
                .entries
                .iter()
                .all(|entry| entry.status == ResourceStatus::Ready));
        }

        #[rstest]
        #[tokio::test]
        async fn test_bucket_info_hides_meta_entries_from_count(#[future] bucket: Arc<Bucket>) {
            let bucket = bucket.await;
            write(&bucket, "entry-1", 1, b"data").await.unwrap();
            write_meta(&bucket, "entry-1/$meta", 1, b"meta")
                .await
                .unwrap();

            let info = bucket.info().await.unwrap();
            assert_eq!(info.info.entry_count, 1);
            assert_eq!(info.entries.len(), 1);
            assert_eq!(info.entries[0].name, "entry-1");
            assert!(info.info.size >= 8);
        }

        #[rstest]
        #[tokio::test]
        async fn test_bucket_info_ignores_meta_entries_for_history(#[future] bucket: Arc<Bucket>) {
            let bucket = bucket.await;
            write_meta(&bucket, "entry-1/$meta", 1, b"meta")
                .await
                .unwrap();
            write(&bucket, "entry-1", 100, b"data").await.unwrap();
            write(&bucket, "entry-1", 200, b"more").await.unwrap();

            let info = bucket.info().await.unwrap();
            assert_eq!(info.info.oldest_record, 100);
            assert_eq!(info.info.latest_record, 200);
            assert_eq!(info.info.entry_count, 1);
            assert_eq!(info.entries.len(), 1);
            assert_eq!(info.entries[0].name, "entry-1");
            assert_eq!(info.entries[0].oldest_record, 100);
            assert_eq!(info.entries[0].latest_record, 200);
        }

        #[rstest]
        #[tokio::test]
        async fn test_bucket_info_ignores_empty_parent_entries_for_oldest_record(
            #[future] bucket: Arc<Bucket>,
        ) {
            let bucket = bucket.await;
            let settings = bucket.settings().await.unwrap();
            let empty_entry = Arc::new(
                Entry::builder()
                    .name("empty")
                    .bucket_path(bucket.path.clone())
                    .settings(settings_for_entry("empty", &settings))
                    .cfg(bucket.cfg.clone())
                    .usage_counters(Default::default())
                    .build()
                    .await
                    .unwrap(),
            );
            bucket
                .entries
                .write()
                .await
                .unwrap()
                .insert("empty".to_string(), empty_entry);

            write(&bucket, "filled", 1, b"data").await.unwrap();
            write(&bucket, "filled", 2, b"more").await.unwrap();

            let info = bucket.info().await.unwrap();
            assert_eq!(info.info.oldest_record, 1);
            assert_eq!(info.info.latest_record, 2);
            assert_eq!(info.info.entry_count, 2);

            let entries = info
                .entries
                .into_iter()
                .map(|entry| (entry.name.clone(), entry))
                .collect::<HashMap<_, _>>();
            assert_eq!(entries["filled"].record_count, 2);
            assert_eq!(entries["filled"].oldest_record, 1);
            assert_eq!(entries["filled"].latest_record, 2);
        }

        #[rstest]
        #[tokio::test]
        async fn test_bucket_info_normalizes_history_when_only_meta_entries_have_records(
            #[future] bucket: Arc<Bucket>,
        ) {
            let bucket = bucket.await;
            write_meta(&bucket, "entry/$meta", 1, b"meta")
                .await
                .unwrap();

            let info = bucket.info().await.unwrap();
            assert_eq!(info.info.oldest_record, 0);
            assert_eq!(info.info.latest_record, 0);
            assert_eq!(info.info.entry_count, 1);
            assert_eq!(info.entries.len(), 1);
            assert_eq!(info.entries[0].name, "entry");
            assert_eq!(info.entries[0].record_count, 0);
        }

        #[rstest]
        #[tokio::test]
        async fn test_bucket_deleting_rejects_operations(#[future] bucket: Arc<Bucket>) {
            let bucket = bucket.await;
            bucket.mark_deleting().await.unwrap();
            let err = bucket.get_or_create_entry("new-entry").await.err().unwrap();
            assert_eq!(err, conflict!("Bucket 'test' is being deleted"));
        }

        #[rstest]
        #[tokio::test]
        async fn set_settings_returns_conflict_when_bucket_deleting(#[future] bucket: Arc<Bucket>) {
            let bucket = bucket.await;
            bucket.mark_deleting().await.unwrap();

            let err = bucket
                .set_settings(BucketSettings::default())
                .await
                .err()
                .unwrap();
            assert_eq!(err, conflict!("Bucket 'test' is being deleted"));
        }

        #[rstest]
        #[tokio::test]
        async fn update_labels_returns_conflict_when_bucket_deleting(
            #[future] bucket: Arc<Bucket>,
        ) {
            let bucket = bucket.await;
            bucket.mark_deleting().await.unwrap();

            let err = bucket
                .update_labels(vec![UpdateLabelsMulti {
                    entry_name: "entry".to_string(),
                    time: 1,
                    update: Labels::new(),
                    remove: HashSet::new(),
                }])
                .await
                .err()
                .unwrap();
            assert_eq!(err, conflict!("Bucket 'test' is being deleted"));
        }

        #[rstest]
        #[tokio::test]
        async fn remove_records_returns_conflict_when_bucket_deleting(
            #[future] bucket: Arc<Bucket>,
        ) {
            let bucket = bucket.await;
            bucket.mark_deleting().await.unwrap();

            let err = bucket
                .clone()
                .remove_records(HashMap::from([("entry".to_string(), vec![1])]))
                .await
                .err()
                .unwrap();
            assert_eq!(err, conflict!("Bucket 'test' is being deleted"));
        }

        #[rstest]
        #[tokio::test]
        async fn query_remove_records_returns_conflict_when_bucket_deleting(
            #[future] bucket: Arc<Bucket>,
        ) {
            let bucket = bucket.await;
            bucket.mark_deleting().await.unwrap();

            let err = bucket
                .clone()
                .query_remove_records(QueryEntry::default())
                .await
                .err()
                .unwrap();
            assert_eq!(err, conflict!("Bucket 'test' is being deleted"));
        }

        #[rstest]
        #[tokio::test]
        async fn bucket_mark_deleting_returns_conflict_when_already_deleting(
            #[future] bucket: Arc<Bucket>,
        ) {
            let bucket = bucket.await;
            bucket.mark_deleting().await.unwrap();
            assert_eq!(
                bucket.mark_deleting().await,
                Err(conflict!("Bucket 'test' is being deleted"))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_entry_deleting_rejects_operations(#[future] bucket: Arc<Bucket>) {
            let bucket = bucket.await;
            write(&bucket, "test-1", 1, b"test").await.unwrap();
            let entry = bucket.get_entry("test-1").await.unwrap().upgrade().unwrap();
            entry.mark_deleting().await.unwrap();

            let err = bucket.begin_read("test-1", 1).await.err().unwrap();
            assert_eq!(
                err,
                conflict!("Entry 'test-1' in bucket 'test' is being deleted")
            );
        }

        #[rstest]
        #[tokio::test]
        async fn begin_write_returns_conflict_when_target_entry_deleting_before_quota(
            path: PathBuf,
        ) {
            let bucket = bucket(
                BucketSettings {
                    quota_type: Some(QuotaType::HARD),
                    quota_size: Some(100),
                    ..BucketSettings::default()
                },
                path,
            )
            .await;
            write(&bucket, "test-1", 1, b"test").await.unwrap();
            let entry = bucket.get_entry("test-1").await.unwrap().upgrade().unwrap();
            entry.mark_deleting().await.unwrap();
            *bucket.settings.write().await.unwrap() = BucketSettings {
                quota_type: Some(QuotaType::HARD),
                quota_size: Some(1),
                ..BucketSettings::default()
            };

            let err = write(&bucket, "test-1", 2, b"x").await.err().unwrap();
            assert_eq!(
                err,
                conflict!("Entry 'test-1' in bucket 'test' is being deleted")
            );
        }

        #[rstest]
        #[tokio::test]
        async fn entry_mark_deleting_returns_conflict_when_already_deleting(
            #[future] bucket: Arc<Bucket>,
        ) {
            let bucket = bucket.await;
            write(&bucket, "test-1", 1, b"test").await.unwrap();
            let entry = bucket.get_entry("test-1").await.unwrap().upgrade().unwrap();
            entry.mark_deleting().await.unwrap();
            assert_eq!(
                entry.mark_deleting().await,
                Err(conflict!(
                    "Entry 'test-1' in bucket 'test' is being deleted"
                ))
            );
        }
    }

    mod restore {
        use super::*;

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn test_restore_nested_entry_path(#[future] bucket: Arc<Bucket>) {
            let bucket = bucket.await;
            write(&bucket, "entry/a", 1, b"test").await.unwrap();
            bucket.sync_fs().await.unwrap();

            let bucket = Bucket::builder()
                .path(bucket.path.clone())
                .cfg(Cfg::default())
                .usage_counters(Default::default())
                .restore()
                .await
                .unwrap();
            let mut reader = bucket.begin_read("entry/a", 1).await.unwrap();
            assert_eq!(reader.read_chunk().unwrap().unwrap(), Bytes::from("test"));
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn test_restore_meta_entry_uses_small_block_size(#[future] bucket: Arc<Bucket>) {
            let bucket = bucket.await;
            write_meta(&bucket, "entry/a/$meta", 1, b"meta")
                .await
                .unwrap();
            bucket.sync_fs().await.unwrap();

            let bucket = Bucket::builder()
                .path(bucket.path.clone())
                .cfg(Cfg::default())
                .usage_counters(Default::default())
                .restore()
                .await
                .unwrap();
            let entry = bucket
                .get_entry("entry/a/$meta")
                .await
                .unwrap()
                .upgrade()
                .unwrap();
            let settings = entry.settings().await.unwrap();
            assert_eq!(settings.max_block_size, META_ENTRY_MAX_BLOCK_SIZE);
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_provisioned_info(#[future] provisioned_bucket: Arc<Bucket>) {
        let provisioned_bucket = provisioned_bucket.await;
        let info = provisioned_bucket.info().await.unwrap().info;
        assert_eq!(info.is_provisioned, true);
    }

    #[rstest]
    #[tokio::test]
    async fn test_provisioned_settings(#[future] provisioned_bucket: Arc<Bucket>) {
        let provisioned_bucket = provisioned_bucket.await;
        let err = provisioned_bucket
            .set_settings(BucketSettings::default())
            .await
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

    #[fixture]
    pub async fn provisioned_bucket(settings: BucketSettings, path: PathBuf) -> Arc<Bucket> {
        FILE_CACHE.create_dir_all(&path.join("test")).await.unwrap();
        let bucket = Bucket::builder()
            .name("test")
            .data_path(path)
            .settings(settings)
            .cfg(Cfg::default())
            .usage_counters(Default::default())
            .build()
            .await
            .unwrap();
        bucket.set_provisioned(true);
        Arc::new(bucket)
    }
}
