// Copyright 2023-2026 ReductSoftware UG
// Licensed under the Business Source License 1.1
mod read_only;
use crate::cfg::Cfg;
use crate::cfg::InstanceRole;
use crate::core::file_cache::FILE_CACHE;
use crate::core::sync::AsyncRwLock;
use crate::core::weak::Weak;
use crate::storage::bucket::Bucket;
use crate::storage::folder_keeper::FolderKeeper;
use async_trait::async_trait;
use log::{debug, error, info};
use reduct_base::error::ReductError;
use reduct_base::msg::bucket_api::BucketSettings;
use reduct_base::msg::server_api::{BucketInfoList, Defaults, License, ServerInfo};
use reduct_base::{conflict, forbidden, not_found, unprocessable_entity};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

pub(crate) const MAX_IO_BUFFER_SIZE: usize = 1024 * 512;
pub(crate) const CHANNEL_BUFFER_SIZE: usize = 16;

pub struct StorageEngineBuilder {
    cfg: Option<Cfg>,
    license: Option<License>,
    data_path: Option<PathBuf>,
}

#[async_trait]
pub(super) trait ReadOnlyMode {
    fn cfg(&self) -> &Cfg;

    async fn reload(&self) -> Result<(), ReductError>;

    fn check_mode(&self) -> Result<(), ReductError> {
        if self.cfg().role == InstanceRole::Replica {
            return Err(forbidden!(
                "Cannot perform this operation in read-only mode"
            ));
        }
        Ok(())
    }
}

impl StorageEngineBuilder {
    pub fn with_cfg(mut self, cfg: Cfg) -> Self {
        self.cfg = Some(cfg);
        self
    }

    pub fn with_license(mut self, license: License) -> Self {
        self.license = Some(license);
        self
    }

    pub fn with_data_path(mut self, data_path: PathBuf) -> Self {
        self.data_path = Some(data_path);
        self
    }

    pub async fn build(self) -> StorageEngine {
        let cfg = self.cfg.expect("Config must be set");
        let data_path = self.data_path.expect("Data path must be set");

        if !FILE_CACHE.try_exists(&data_path).await.unwrap_or(false) {
            info!("Folder {:?} doesn't exist. Create it.", data_path);
            FILE_CACHE.create_dir_all(&data_path).await.unwrap();
        }

        let data_path = data_path.canonicalize().unwrap();

        // restore buckets
        let time = Instant::now();
        let mut buckets = BTreeMap::new();
        let folder_keeper = FolderKeeper::new(data_path.clone(), &cfg).await;

        for path in folder_keeper
            .list_folders()
            .await
            .expect("Failed to list folders")
        {
            match Bucket::restore(data_path.join(&path), cfg.clone()).await {
                Ok(bucket) => {
                    let bucket = Arc::new(bucket);
                    buckets.insert(bucket.name().to_string(), bucket);
                }
                Err(e) => {
                    panic!("Failed to load bucket from {:?}: {}", path, e);
                }
            }
        }

        info!("Load {} bucket(s) in {:?}", buckets.len(), time.elapsed());

        StorageEngine {
            data_path,
            start_time: Instant::now(),
            buckets: Arc::new(AsyncRwLock::new(buckets)),
            license: self.license,
            cfg,
            last_replica_sync: AsyncRwLock::new(Instant::now()),
            folder_keeper: Arc::new(folder_keeper),
        }
    }
}

/// Storage is the main entry point for the storage service.
pub struct StorageEngine {
    data_path: PathBuf,
    start_time: Instant,
    buckets: Arc<AsyncRwLock<BTreeMap<String, Arc<Bucket>>>>,
    license: Option<License>,
    cfg: Cfg,
    last_replica_sync: AsyncRwLock<Instant>,
    folder_keeper: Arc<FolderKeeper>,
}

impl StorageEngine {
    pub fn builder() -> StorageEngineBuilder {
        StorageEngineBuilder {
            cfg: None,
            license: None,
            data_path: None,
        }
    }

    /// Get the reductstore info.
    ///
    /// # Returns
    ///
    /// * `ServerInfo` - The reductstore info or an HTTPError
    ///
    pub async fn info(&self) -> Result<ServerInfo, ReductError> {
        self.reload().await?;

        let mut usage = 0u64;
        let mut oldest_record = u64::MAX;
        let mut latest_record = 0u64;

        let buckets = self.buckets.read().await?;
        let infos = buckets
            .values()
            .map(|bucket| bucket.clone().info())
            .collect::<Vec<_>>();

        for task in infos {
            let bucket = task.await?.info;
            usage += bucket.size;
            oldest_record = oldest_record.min(bucket.oldest_record);
            latest_record = latest_record.max(bucket.latest_record);
        }

        Ok(ServerInfo {
            version: option_env!("CARGO_PKG_VERSION")
                .unwrap_or("unknown")
                .to_string(),
            bucket_count: buckets.len() as u64,
            usage,
            uptime: self.start_time.elapsed().as_secs(),
            oldest_record,
            latest_record,
            defaults: Defaults {
                bucket: Bucket::defaults(),
            },
            license: self.license.clone(),
        })
    }

    /// Creat a new bucket.
    pub(crate) async fn create_bucket(
        &self,
        name: &str,
        settings: BucketSettings,
    ) -> Result<Weak<Bucket>, ReductError> {
        self.check_mode()?;

        check_name_convention(name)?;
        let mut buckets = self.buckets.write().await?;
        if buckets.contains_key(name) {
            return Err(conflict!("Bucket '{}' already exists", name));
        }

        self.folder_keeper.add_folder(name).await?;
        let bucket =
            Arc::new(Bucket::try_build(name, &self.data_path, settings, self.cfg.clone()).await?);
        buckets.insert(name.to_string(), Arc::clone(&bucket));

        Ok(bucket.into())
    }

    /// Get a bucket by name
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the bucket
    ///
    /// # Returns
    ///
    /// * `Bucket` - The bucket or an HTTPError
    pub(crate) async fn get_bucket(&self, name: &str) -> Result<Weak<Bucket>, ReductError> {
        self.reload().await?;
        let buckets = self.buckets.read().await?;
        match buckets.get(name) {
            Some(bucket) => {
                bucket.ensure_not_deleting().await?;
                Ok(Arc::clone(bucket).into())
            }
            None => Err(ReductError::not_found(
                format!("Bucket '{}' is not found", name).as_str(),
            )),
        }
    }

    /// Remove a bucket by name.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the bucket
    ///
    /// # Returns
    ///
    /// * HTTPError - An error if the bucket doesn't exist
    pub(crate) async fn remove_bucket(&self, name: &str) -> Result<(), ReductError> {
        self.check_mode()?;

        let buckets = self.buckets.clone();
        let bucket = {
            let buckets = buckets.read().await?;
            buckets
                .get(name)
                .cloned()
                .ok_or_else(|| not_found!("Bucket '{}' is not found", name))?
        };

        if bucket.is_provisioned() {
            return Err(conflict!(
                "Can't remove provisioned bucket '{}'",
                bucket.name()
            ));
        }

        bucket.mark_deleting().await?;

        let path = self.data_path.join(name);
        let name = name.to_string();
        let folder_keeper = self.folder_keeper.clone();
        let bucket = Arc::clone(&bucket);

        let _ = tokio::spawn(async move {
            let remove_bucket_from_backend = async || {
                bucket.remove_entries_for_bucket_removal().await?;
                folder_keeper.remove_folder(&name).await?;
                debug!("Bucket '{}' and folder {:?} are removed", name, path);
                let mut buckets = buckets.write().await?;
                buckets.remove(&name);
                Ok::<(), ReductError>(())
            };

            if let Err(err) = remove_bucket_from_backend().await {
                error!("Failed to sync bucket '{}': {}", name, err);
            }
        });

        Ok(())
    }

    pub(crate) async fn rename_bucket(
        &self,
        old_name: String,
        new_name: String,
    ) -> Result<(), ReductError> {
        self.check_mode()?;
        check_name_convention(&new_name)?;
        {
            let buckets = self.buckets.read().await?;
            if let Some(bucket) = buckets.get(&new_name) {
                return Err(conflict!("Bucket '{}' already exists", bucket.name()));
            }

            if let Some(bucket) = buckets.get(&old_name) {
                if bucket.is_provisioned() {
                    return Err(conflict!(
                        "Can't rename provisioned bucket '{}'",
                        bucket.name()
                    ));
                }

                bucket.sync_fs().await?;
            } else {
                Err(not_found!("Bucket '{}' is not found", old_name))?;
            }
        }

        let new_path = self.data_path.join(&new_name);
        let cfg = self.cfg.clone();
        let folder_keeper = self.folder_keeper.clone();

        let mut buckets = self.buckets.write().await?;

        folder_keeper.rename_folder(&old_name, &new_name).await?;

        buckets.remove(&old_name);
        let bucket = Bucket::restore(new_path, cfg).await?;
        buckets.insert(new_name.to_string(), Arc::new(bucket));
        debug!("Bucket '{}' is renamed to '{}'", old_name, new_name);
        Ok(())
    }

    pub(crate) async fn get_bucket_list(&self) -> Result<BucketInfoList, ReductError> {
        self.reload().await?;
        let mut buckets = Vec::new();

        let infos = {
            let buckets = self.buckets.read().await?;
            buckets
                .values()
                .map(|bucket| bucket.clone().info())
                .collect::<Vec<_>>()
        };

        for task in infos {
            let bucket = task.await?.info;
            buckets.push(bucket);
        }

        Ok(BucketInfoList { buckets })
    }

    pub async fn sync_fs(&self) -> Result<(), ReductError> {
        self.compact().await?;
        FILE_CACHE.force_sync_all().await?;
        Ok(())
    }

    /// Update index from WALs and remove them
    pub async fn compact(&self) -> Result<(), ReductError> {
        if self.cfg.role == InstanceRole::Replica {
            return Ok(());
        }

        let mut handlers = vec![];
        let buckets = self
            .buckets
            .read()
            .await?
            .values()
            .map(|bucket| Arc::clone(bucket))
            .collect::<Vec<_>>();
        for bucket in buckets {
            handlers.push(tokio::spawn(async move { bucket.sync_fs().await }));
        }

        for handler in handlers {
            match handler.await.unwrap() {
                Ok(_) => {}
                Err(e) => {
                    error!("Failed to sync bucket: {}", e);
                }
            }
        }
        Ok(())
    }

    pub fn data_path(&self) -> &PathBuf {
        &self.data_path
    }
}

pub(super) fn check_name_convention(name: &str) -> Result<(), ReductError> {
    let regex = regex::Regex::new(r"^[A-Za-z0-9_-]*$").unwrap();
    if !regex.is_match(name) {
        return Err(unprocessable_entity!(
            "Bucket or entry name can contain only letters, digests and [-,_] symbols",
        ));
    }
    Ok(())
}

#[cfg(test)]
impl StorageEngine {
    pub async fn reset_last_replica_sync(&self) {
        let mut sync = self.last_replica_sync.write().await.unwrap();
        *sync = Instant::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::Bytes;
    use reduct_base::msg::bucket_api::QuotaType;
    use reduct_base::Labels;
    use rstest::{fixture, rstest};
    use std::time::Duration;
    use tempfile::tempdir;

    #[rstest]
    #[tokio::test]
    async fn test_create_folder() {
        let path = tempdir().unwrap().keep().join("data_path");
        let cfg = Cfg {
            data_path: path.clone(),
            ..Cfg::default()
        };

        assert!(!path.exists());
        let _ = StorageEngine::builder()
            .with_data_path(cfg.data_path.clone())
            .with_cfg(cfg)
            .build()
            .await;
        assert!(path.exists(), "Engine creates a folder if it doesn't exist");
    }

    #[rstest]
    #[tokio::test]
    async fn test_info(#[future] storage: Arc<StorageEngine>) {
        let storage = storage.await;
        tokio::time::sleep(Duration::from_secs(1)).await; // uptime is 1 second

        let info = storage.info().await.unwrap();
        assert_eq!(
            info,
            ServerInfo {
                version: env!("CARGO_PKG_VERSION").to_string(),
                bucket_count: 0,
                usage: 0,
                uptime: 1,
                oldest_record: u64::MAX,
                latest_record: 0,
                defaults: Defaults {
                    bucket: Bucket::defaults(),
                },
                license: None,
            }
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_license_info(#[future] storage: Arc<StorageEngine>) {
        let storage = storage.await;
        let license = License {
            licensee: "ReductSoftware UG".to_string(),
            invoice: "2021-0001".to_string(),
            expiry_date: chrono::Utc::now(),
            plan: "Enterprise".to_string(),
            device_number: 100,
            disk_quota: 100,
            fingerprint: "fingerprint".to_string(),
        };

        let cfg = Cfg {
            data_path: storage.data_path.clone(),
            ..Cfg::default()
        };
        let storage = Arc::new(
            StorageEngine::builder()
                .with_data_path(cfg.data_path.clone())
                .with_cfg(cfg)
                .with_license(license.clone())
                .build()
                .await,
        );
        assert_eq!(storage.info().await.unwrap().license, Some(license));
    }

    mod recovery {
        use super::*;
        use crate::storage::bucket::settings::SETTINGS_NAME;
        #[rstest]
        #[tokio::test]
        async fn test_recover_from_fs(#[future] storage: Arc<StorageEngine>) {
            let storage = storage.await;
            let bucket_settings = BucketSettings {
                quota_size: Some(100),
                quota_type: Some(QuotaType::FIFO),
                ..Bucket::defaults()
            };
            let bucket = storage
                .create_bucket("test", bucket_settings.clone())
                .await
                .unwrap()
                .upgrade_and_unwrap();
            assert_eq!(bucket.name(), "test");

            macro_rules! write_entry {
                ($bucket:expr, $entry_name:expr, $record_ts:expr) => {
                    let entry = $bucket
                        .get_or_create_entry($entry_name)
                        .await
                        .unwrap()
                        .upgrade_and_unwrap();
                    let mut sender = entry
                        .begin_write($record_ts, 10, "text/plain".to_string(), Labels::new())
                        .await
                        .unwrap();
                    sender
                        .send(Ok(Some(Bytes::from("0123456789"))))
                        .await
                        .unwrap();

                    sender.send(Ok(None)).await.unwrap();
                };
            }

            write_entry!(bucket, "entry-1", 1000);
            write_entry!(bucket, "entry-2", 2000);
            write_entry!(bucket, "entry-2", 5000);

            tokio::time::sleep(Duration::from_millis(10)).await; // to make sure that write tasks are completed
            storage.sync_fs().await.unwrap();
            let cfg = Cfg {
                data_path: storage.data_path.clone(),
                ..Cfg::default()
            };
            let storage = Arc::new(
                StorageEngine::builder()
                    .with_data_path(cfg.data_path.clone())
                    .with_cfg(cfg)
                    .build()
                    .await,
            );
            assert_eq!(
                storage.info().await.unwrap(),
                ServerInfo {
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    bucket_count: 1,
                    usage: 142,
                    uptime: 0,
                    oldest_record: 1000,
                    latest_record: 5000,
                    defaults: Defaults {
                        bucket: Bucket::defaults(),
                    },
                    license: None,
                }
            );

            let bucket = storage
                .get_bucket("test")
                .await
                .unwrap()
                .upgrade_and_unwrap();
            assert_eq!(bucket.name(), "test");
            assert_eq!(bucket.settings().await.unwrap(), bucket_settings);
        }

        #[rstest]
        #[tokio::test]
        #[should_panic(expected = "Failed to load bucket from")]
        async fn test_broken_bucket(#[future] storage: Arc<StorageEngine>) {
            let storage = storage.await;
            let bucket_settings = BucketSettings {
                quota_size: Some(100),
                quota_type: Some(QuotaType::FIFO),
                ..Bucket::defaults()
            };

            let bucket = storage
                .create_bucket("test", bucket_settings.clone())
                .await
                .unwrap()
                .upgrade_and_unwrap();
            assert_eq!(bucket.name(), "test");

            let path = storage.data_path.join("test");
            let settings_path = path.join(SETTINGS_NAME);
            FILE_CACHE.remove(&settings_path).await.unwrap();
            let cfg = Cfg {
                data_path: storage.data_path.clone(),
                ..Cfg::default()
            };
            let storage = Arc::new(
                StorageEngine::builder()
                    .with_data_path(cfg.data_path.clone())
                    .with_cfg(cfg)
                    .build()
                    .await,
            );
            assert_eq!(
                storage.info().await.unwrap(),
                ServerInfo {
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    bucket_count: 0,
                    usage: 0,
                    uptime: 0,
                    oldest_record: u64::MAX,
                    latest_record: 0,
                    defaults: Defaults {
                        bucket: Bucket::defaults(),
                    },
                    license: None,
                }
            );
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_bucket(#[future] storage: Arc<StorageEngine>) {
        let storage = storage.await;
        let bucket = storage
            .create_bucket("test", BucketSettings::default())
            .await
            .unwrap()
            .upgrade_and_unwrap();
        assert_eq!(bucket.name(), "test");
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_bucket_with_invalid_name(#[future] storage: Arc<StorageEngine>) {
        let storage = storage.await;
        let result = storage
            .create_bucket("test$", BucketSettings::default())
            .await;
        assert_eq!(
            result.err(),
            Some(unprocessable_entity!(
                "Bucket or entry name can contain only letters, digests and [-,_] symbols"
            ))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_bucket_with_existing_name(#[future] storage: Arc<StorageEngine>) {
        let storage = storage.await;
        let bucket = storage
            .create_bucket("test", BucketSettings::default())
            .await
            .unwrap()
            .upgrade_and_unwrap();
        assert_eq!(bucket.name(), "test");

        let result = storage
            .create_bucket("test", BucketSettings::default())
            .await;
        assert_eq!(
            result.err(),
            Some(conflict!("Bucket 'test' already exists"))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_bucket(#[future] storage: Arc<StorageEngine>) {
        let storage = storage.await;
        let bucket = storage
            .create_bucket("test", BucketSettings::default())
            .await
            .unwrap()
            .upgrade_and_unwrap();
        assert_eq!(bucket.name(), "test");

        let bucket = storage
            .get_bucket("test")
            .await
            .unwrap()
            .upgrade_and_unwrap();
        assert_eq!(bucket.name(), "test");
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_bucket_with_non_existing_name(#[future] storage: Arc<StorageEngine>) {
        let storage = storage.await;
        let result = storage.get_bucket("test").await;
        assert_eq!(result.err(), Some(not_found!("Bucket 'test' is not found")));
    }

    mod remove_bucket {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_remove_bucket(#[future] storage: Arc<StorageEngine>) {
            let storage = storage.await;
            let bucket = storage
                .create_bucket("test", BucketSettings::default())
                .await
                .unwrap()
                .upgrade_and_unwrap();
            assert_eq!(bucket.name(), "test");

            let result = storage.remove_bucket("test").await;
            assert_eq!(result, Ok(()));

            let result = storage.get_bucket("test").await;
            let err = result.err().unwrap();
            assert!(
                err == conflict!("Bucket 'test' is being deleted")
                    || err == not_found!("Bucket 'test' is not found"),
                "Bucket should be deleting or removed"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_remove_bucket_with_non_existing_name(#[future] storage: Arc<StorageEngine>) {
            let storage = storage.await;
            let result = storage.remove_bucket("test").await;
            assert_eq!(result, Err(not_found!("Bucket 'test' is not found")));
        }

        #[rstest]
        #[tokio::test]
        async fn remove_bucket_returns_conflict_when_bucket_is_already_deleting(
            #[future] storage: Arc<StorageEngine>,
        ) {
            let storage = storage.await;
            storage
                .create_bucket("test", BucketSettings::default())
                .await
                .unwrap()
                .upgrade_and_unwrap();

            let bucket = storage
                .buckets
                .read()
                .await
                .unwrap()
                .get("test")
                .unwrap()
                .clone();
            bucket.mark_deleting().await.unwrap();

            assert_eq!(
                storage.remove_bucket("test").await,
                Err(conflict!("Bucket 'test' is being deleted"))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_remove_bucket_persistent(cfg: Cfg, #[future] storage: Arc<StorageEngine>) {
            let storage = storage.await;
            let bucket = storage
                .create_bucket("test", BucketSettings::default())
                .await
                .unwrap()
                .upgrade_and_unwrap();
            assert_eq!(bucket.name(), "test");

            let result = storage.remove_bucket("test").await;
            assert_eq!(result, Ok(()));

            let storage = Arc::new(
                StorageEngine::builder()
                    .with_data_path(cfg.data_path.clone())
                    .with_cfg(cfg)
                    .build()
                    .await,
            );

            let result = storage.get_bucket("test").await;
            assert_eq!(result.err(), Some(not_found!("Bucket 'test' is not found")));
        }
    }

    mod rename_bucket {
        use super::*;
        use crate::core::sync::{
            reset_rwlock_config, set_rwlock_failure_action, set_rwlock_timeout, RwLockFailureAction,
        };
        use reduct_base::io::ReadRecord;
        use reduct_base::logger::Logger;
        use serial_test::serial;

        struct ResetGuard;
        impl Drop for ResetGuard {
            fn drop(&mut self) {
                reset_rwlock_config();
            }
        }

        fn relax_locks() -> ResetGuard {
            set_rwlock_timeout(Duration::from_secs(2));
            set_rwlock_failure_action(RwLockFailureAction::Error);
            ResetGuard
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_rename_bucket(#[future] storage: Arc<StorageEngine>) {
            let _reset = relax_locks();
            let storage = storage.await;
            Logger::init("TRACE");
            let bucket = storage
                .create_bucket("test", BucketSettings::default())
                .await
                .unwrap()
                .upgrade_and_unwrap();
            assert_eq!(bucket.name(), "test");

            let mut writer = bucket
                .begin_write("entry-1", 0, 10, "text/plain".to_string(), Labels::new())
                .await
                .unwrap();
            writer
                .send(Ok(Some(Bytes::from("0123456789"))))
                .await
                .unwrap();
            writer.send(Ok(None)).await.unwrap();
            // Ensure writer is dropped before renaming so Windows can release file handles
            drop(writer);

            let result = storage
                .rename_bucket("test".to_string(), "new".to_string())
                .await;
            assert_eq!(result, Ok(()));

            let result = storage.get_bucket("test").await;
            assert_eq!(result.err(), Some(not_found!("Bucket 'test' is not found")));

            let bucket = storage
                .get_bucket("new")
                .await
                .unwrap()
                .upgrade_and_unwrap();
            assert_eq!(bucket.name(), "new");

            let mut reader = bucket.begin_read("entry-1", 0).await.unwrap();
            let record = reader.read_chunk().unwrap().unwrap();
            assert_eq!(record, Bytes::from("0123456789"));
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_rename_bucket_with_non_existing_name(#[future] storage: Arc<StorageEngine>) {
            let _reset = relax_locks();
            let storage = storage.await;
            let result = storage
                .rename_bucket("test".to_string(), "new".to_string())
                .await;
            assert_eq!(result, Err(not_found!("Bucket 'test' is not found")));
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_rename_bucket_with_existing_name(#[future] storage: Arc<StorageEngine>) {
            let _reset = relax_locks();
            let storage = storage.await;
            let bucket = storage
                .create_bucket("test", BucketSettings::default())
                .await
                .unwrap()
                .upgrade_and_unwrap();
            assert_eq!(bucket.name(), "test");

            let bucket = storage
                .create_bucket("new", BucketSettings::default())
                .await
                .unwrap()
                .upgrade_and_unwrap();
            assert_eq!(bucket.name(), "new");

            let result = storage
                .rename_bucket("test".to_string(), "new".to_string())
                .await;
            assert_eq!(result, Err(conflict!("Bucket 'new' already exists")));
        }

        #[rstest]
        #[tokio::test]
        #[serial]
        async fn test_rename_bucket_with_invalid_name(#[future] storage: Arc<StorageEngine>) {
            let _reset = relax_locks();
            let storage = storage.await;
            let result = storage
                .rename_bucket("test".to_string(), "new$".to_string())
                .await;
            assert_eq!(
                result,
                Err(unprocessable_entity!(
                    "Bucket or entry name can contain only letters, digests and [-,_] symbols"
                ))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_rename_provisioned_bucket(#[future] storage: Arc<StorageEngine>) {
            let storage = storage.await;
            let bucket = storage
                .create_bucket("test", BucketSettings::default())
                .await
                .unwrap()
                .upgrade_and_unwrap();
            bucket.set_provisioned(true);
            let result = storage
                .rename_bucket("test".to_string(), "new".to_string())
                .await;
            assert_eq!(
                result,
                Err(conflict!("Can't rename provisioned bucket 'test'"))
            );
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_bucket_list(#[future] storage: Arc<StorageEngine>) {
        let storage = storage.await;
        storage
            .create_bucket("test1", Bucket::defaults())
            .await
            .unwrap();
        storage
            .create_bucket("test2", Bucket::defaults())
            .await
            .unwrap();

        let bucket_list = storage.get_bucket_list().await.unwrap();
        assert_eq!(bucket_list.buckets.len(), 2);
        assert_eq!(bucket_list.buckets[0].name, "test1");
        assert_eq!(bucket_list.buckets[1].name, "test2");
    }

    #[rstest]
    #[tokio::test]
    async fn test_provisioned_remove(#[future] storage: Arc<StorageEngine>) {
        let storage = storage.await;
        let bucket = storage
            .create_bucket("test", BucketSettings::default())
            .await
            .unwrap()
            .upgrade_and_unwrap();
        bucket.set_provisioned(true);
        let err = storage.remove_bucket("test").await.err().unwrap();
        assert_eq!(
            err,
            ReductError::conflict("Can't remove provisioned bucket 'test'")
        );
    }

    #[fixture]
    fn cfg() -> Cfg {
        Cfg {
            data_path: tempdir().unwrap().keep(),
            ..Cfg::default()
        }
    }

    #[fixture]
    async fn storage(cfg: Cfg) -> Arc<StorageEngine> {
        Arc::new(
            StorageEngine::builder()
                .with_data_path(cfg.data_path.clone())
                .with_cfg(cfg)
                .build()
                .await,
        )
    }
}
