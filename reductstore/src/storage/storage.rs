// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use log::{debug, error, info};
use std::collections::BTreeMap;

use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::storage::bucket::Bucket;
use reduct_base::error::ReductError;

use crate::core::file_cache::FILE_CACHE;
use crate::core::thread_pool::GroupDepth::BUCKET;
use crate::core::thread_pool::{group_from_path, unique, TaskHandle};
use crate::core::weak::Weak;
use reduct_base::msg::bucket_api::BucketSettings;
use reduct_base::msg::server_api::{BucketInfoList, Defaults, License, ServerInfo};
use reduct_base::{conflict, not_found, unprocessable_entity};

pub(crate) const MAX_IO_BUFFER_SIZE: usize = 1024 * 512;
pub(crate) const CHANNEL_BUFFER_SIZE: usize = 16;
pub(crate) const IO_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

/// Storage is the main entry point for the storage service.
pub struct Storage {
    data_path: PathBuf,
    start_time: Instant,
    buckets: Arc<RwLock<BTreeMap<String, Arc<Bucket>>>>,
    license: Option<License>,
}

impl Storage {
    /// Load storage from the file system.
    /// If the data_path doesn't exist, it will be created.
    ///
    /// # Arguments
    ///
    /// * `data_path` - The path to the data folder
    /// * `license` - The license info
    ///
    /// # Returns
    ///
    /// * `Storage` - The storage instance
    ///
    /// # Panics
    ///
    /// If the data_path doesn't exist and can't be created, or if a bucket can't be restored.
    pub fn load(data_path: PathBuf, license: Option<License>) -> Storage {
        if !data_path.try_exists().unwrap_or(false) {
            info!("Folder {:?} doesn't exist. Create it.", data_path);
            std::fs::create_dir_all(&data_path).unwrap();
        }

        // restore buckets
        let mut buckets = BTreeMap::new();
        for entry in std::fs::read_dir(&data_path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                match Bucket::restore(path.clone()) {
                    Ok(bucket) => {
                        let bucket = Arc::new(bucket);
                        buckets.insert(bucket.name().to_string(), bucket);
                    }
                    Err(e) => {
                        error!("Failed to restore bucket from {:?}: {}", path, e);
                    }
                }
            }
        }

        info!("Load {} buckets", buckets.len());

        Storage {
            data_path,
            start_time: Instant::now(),
            buckets: Arc::new(RwLock::new(buckets)),
            license,
        }
    }

    /// Get the reductstore info.
    ///
    /// # Returns
    ///
    /// * `ServerInfo` - The reductstore info or an HTTPError
    pub fn info(&self) -> Result<ServerInfo, ReductError> {
        let mut usage = 0u64;
        let mut oldest_record = u64::MAX;
        let mut latest_record = 0u64;

        let buckets = self.buckets.read().unwrap();
        for bucket in buckets.values() {
            let bucket = bucket.info()?.info;
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
    pub(crate) fn create_bucket(
        &self,
        name: &str,
        settings: BucketSettings,
    ) -> Result<Weak<Bucket>, ReductError> {
        check_name_convention(name)?;
        let mut buckets = self.buckets.write()?;
        if buckets.contains_key(name) {
            return Err(conflict!("Bucket '{}' already exists", name));
        }

        let bucket = Arc::new(Bucket::new(name, &self.data_path, settings)?);
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
    pub(crate) fn get_bucket(&self, name: &str) -> Result<Weak<Bucket>, ReductError> {
        let buckets = self.buckets.read().unwrap();
        match buckets.get(name) {
            Some(bucket) => Ok(Arc::clone(bucket).into()),
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
    pub(crate) fn remove_bucket(&self, name: &str) -> TaskHandle<Result<(), ReductError>> {
        let task_group = [self.data_path.file_name().unwrap().to_str().unwrap(), name].join("/");

        let path = self.data_path.join(name);
        let buckets = self.buckets.clone();
        let name = name.to_string();
        unique(&task_group, "remove bucket", move || {
            let mut buckets = buckets.write().unwrap();
            if let Some(bucket) = buckets.get(&name) {
                if bucket.is_provisioned() {
                    return Err(conflict!(
                        "Can't remove provisioned bucket '{}'",
                        bucket.name()
                    ));
                }
            }

            match buckets.remove(&name) {
                Some(_) => {
                    FILE_CACHE.remove_dir(&path)?;
                    debug!("Bucket '{}' and folder {:?} are removed", name, path);
                    Ok(())
                }
                None => Err(not_found!("Bucket '{}' is not found", name)),
            }
        })
    }

    pub(crate) fn rename_bucket(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> TaskHandle<Result<(), ReductError>> {
        let check_and_prepare_bucket = || {
            check_name_convention(new_name)?;
            let buckets = self.buckets.read().unwrap();
            if let Some(bucket) = buckets.get(new_name) {
                return Err(conflict!("Bucket '{}' already exists", bucket.name()));
            }

            if let Some(bucket) = buckets.get(old_name) {
                if bucket.is_provisioned() {
                    return Err(conflict!(
                        "Can't rename provisioned bucket '{}'",
                        bucket.name()
                    ));
                }

                let sync_task = bucket.sync_fs();
                // wait for the start of the sync_fs task
                // to avoid lock with unique task on the bucket level
                sync_task.wait_started();
                Ok(sync_task)
            } else {
                Err(not_found!("Bucket '{}' is not found", old_name))
            }
        };

        let sync_task = match check_and_prepare_bucket() {
            Ok(sync_task) => sync_task,
            Err(err) => return TaskHandle::from(Err(err)),
        };

        let task_group = group_from_path(&self.data_path.join(old_name), BUCKET);
        let buckets = self.buckets.clone();
        let path = self.data_path.join(old_name);
        let new_path = self.data_path.join(new_name);
        let old_name = old_name.to_string();
        let new_name = new_name.to_string();

        unique(&task_group, "rename bucket", move || {
            let mut buckets = buckets.write().unwrap();
            match buckets.remove(&old_name) {
                Some(_) => {
                    sync_task.wait()?;
                    FILE_CACHE.discard_recursive(&path)?;
                    std::fs::rename(&path, &new_path)?;
                    let bucket = Bucket::restore(new_path)?;
                    buckets.insert(new_name.to_string(), Arc::new(bucket));
                    debug!("Bucket '{}' is renamed to '{}'", old_name, new_name);
                    Ok(())
                }
                None => Err(not_found!("Bucket '{}' is not found", old_name)),
            }
        })
    }

    pub(crate) fn get_bucket_list(&self) -> Result<BucketInfoList, ReductError> {
        let mut buckets = Vec::new();
        for bucket in self.buckets.read().unwrap().values() {
            buckets.push(bucket.info()?.info);
        }

        Ok(BucketInfoList { buckets })
    }

    pub fn sync_fs(&self) -> Result<(), ReductError> {
        let mut handlers = vec![];
        let buckets = self.buckets.read()?.clone();
        for (name, bucket) in buckets {
            info!("Sync bucket '{}'", name);
            handlers.push(bucket.sync_fs());
        }

        for handler in handlers {
            match handler.wait() {
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
mod tests {
    use super::*;
    use bytes::Bytes;
    use reduct_base::msg::bucket_api::QuotaType;
    use reduct_base::Labels;
    use rstest::{fixture, rstest};
    use std::thread::sleep;
    use std::time::Duration;
    use tempfile::tempdir;

    #[rstest]
    fn test_create_folder(path: PathBuf) {
        let path = path.join("test");
        assert!(!path.exists());
        let _ = Storage::load(path.clone(), None);
        assert!(path.exists(), "Engine creates a folder if it doesn't exist");
    }

    #[rstest]
    fn test_info(storage: Storage) {
        sleep(Duration::from_secs(1)); // uptime is 1 second

        let info = storage.info().unwrap();
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
    fn test_license_info(storage: Storage) {
        let license = License {
            licensee: "ReductSoftware UG".to_string(),
            invoice: "2021-0001".to_string(),
            expiry_date: chrono::Utc::now(),
            plan: "Enterprise".to_string(),
            device_number: 100,
            disk_quota: 100,
            fingerprint: "fingerprint".to_string(),
        };

        let storage = Storage::load(storage.data_path, Some(license.clone()));
        assert_eq!(storage.info().unwrap().license, Some(license));
    }

    mod recovery {
        use super::*;
        use crate::storage::bucket::settings::SETTINGS_NAME;
        #[rstest]
        fn test_recover_from_fs(storage: Storage) {
            let bucket_settings = BucketSettings {
                quota_size: Some(100),
                quota_type: Some(QuotaType::FIFO),
                ..Bucket::defaults()
            };
            let bucket = storage
                .create_bucket("test", bucket_settings.clone())
                .unwrap()
                .upgrade_and_unwrap();
            assert_eq!(bucket.name(), "test");

            macro_rules! write_entry {
                ($bucket:expr, $entry_name:expr, $record_ts:expr) => {
                    let entry = $bucket
                        .get_or_create_entry($entry_name)
                        .unwrap()
                        .upgrade_and_unwrap();
                    let mut sender = entry
                        .begin_write($record_ts, 10, "text/plain".to_string(), Labels::new())
                        .wait()
                        .unwrap();
                    sender
                        .blocking_send(Ok(Some(Bytes::from("0123456789"))))
                        .unwrap();

                    sender.blocking_send(Ok(None)).unwrap();
                };
            }

            write_entry!(bucket, "entry-1", 1000);
            write_entry!(bucket, "entry-2", 2000);
            write_entry!(bucket, "entry-2", 5000);

            sleep(Duration::from_millis(10)); // to make sure that write tasks are completed
            storage.sync_fs().unwrap();
            let storage = Storage::load(storage.data_path.clone(), None);
            assert_eq!(
                storage.info().unwrap(),
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

            let bucket = storage.get_bucket("test").unwrap().upgrade_and_unwrap();
            assert_eq!(bucket.name(), "test");
            assert_eq!(bucket.settings(), bucket_settings);
        }

        #[rstest]
        fn test_ignore_broken_buket(storage: Storage) {
            let bucket_settings = BucketSettings {
                quota_size: Some(100),
                quota_type: Some(QuotaType::FIFO),
                ..Bucket::defaults()
            };
            let bucket = storage
                .create_bucket("test", bucket_settings.clone())
                .unwrap()
                .upgrade_and_unwrap();
            assert_eq!(bucket.name(), "test");

            let path = storage.data_path.join("test");
            std::fs::remove_file(path.join(SETTINGS_NAME)).unwrap();
            let storage = Storage::load(storage.data_path.clone(), None);
            assert_eq!(
                storage.info().unwrap(),
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
    fn test_create_bucket(storage: Storage) {
        let bucket = storage
            .create_bucket("test", BucketSettings::default())
            .unwrap()
            .upgrade_and_unwrap();
        assert_eq!(bucket.name(), "test");
    }

    #[rstest]
    fn test_create_bucket_with_invalid_name(storage: Storage) {
        let result = storage.create_bucket("test$", BucketSettings::default());
        assert_eq!(
            result.err(),
            Some(unprocessable_entity!(
                "Bucket or entry name can contain only letters, digests and [-,_] symbols"
            ))
        );
    }

    #[rstest]
    fn test_create_bucket_with_existing_name(storage: Storage) {
        let bucket = storage
            .create_bucket("test", BucketSettings::default())
            .unwrap()
            .upgrade_and_unwrap();
        assert_eq!(bucket.name(), "test");

        let result = storage.create_bucket("test", BucketSettings::default());
        assert_eq!(
            result.err(),
            Some(conflict!("Bucket 'test' already exists"))
        );
    }

    #[rstest]
    fn test_get_bucket(storage: Storage) {
        let bucket = storage
            .create_bucket("test", BucketSettings::default())
            .unwrap()
            .upgrade_and_unwrap();
        assert_eq!(bucket.name(), "test");

        let bucket = storage.get_bucket("test").unwrap().upgrade_and_unwrap();
        assert_eq!(bucket.name(), "test");
    }

    #[rstest]
    fn test_get_bucket_with_non_existing_name(storage: Storage) {
        let result = storage.get_bucket("test");
        assert_eq!(result.err(), Some(not_found!("Bucket 'test' is not found")));
    }

    mod remove_bucket {
        use super::*;

        #[rstest]
        fn test_remove_bucket(storage: Storage) {
            let bucket = storage
                .create_bucket("test", BucketSettings::default())
                .unwrap()
                .upgrade_and_unwrap();
            assert_eq!(bucket.name(), "test");

            let result = storage.remove_bucket("test").wait();
            assert_eq!(result, Ok(()));

            let result = storage.get_bucket("test");
            assert_eq!(result.err(), Some(not_found!("Bucket 'test' is not found")));
        }

        #[rstest]
        fn test_remove_bucket_with_non_existing_name(storage: Storage) {
            let result = storage.remove_bucket("test").wait();
            assert_eq!(result, Err(not_found!("Bucket 'test' is not found")));
        }

        #[rstest]
        fn test_remove_bucket_persistent(path: PathBuf, storage: Storage) {
            let bucket = storage
                .create_bucket("test", BucketSettings::default())
                .unwrap()
                .upgrade_and_unwrap();
            assert_eq!(bucket.name(), "test");

            let result = storage.remove_bucket("test").wait();
            assert_eq!(result, Ok(()));

            let storage = Storage::load(path, None);
            let result = storage.get_bucket("test");
            assert_eq!(result.err(), Some(not_found!("Bucket 'test' is not found")));
        }
    }

    mod rename_bucket {
        use super::*;
        use reduct_base::io::ReadRecord;
        use reduct_base::logger::Logger;

        #[rstest]
        #[tokio::test]
        async fn test_rename_bucket(storage: Storage) {
            Logger::init("TRACE");
            let bucket = storage
                .create_bucket("test", BucketSettings::default())
                .unwrap()
                .upgrade_and_unwrap();
            assert_eq!(bucket.name(), "test");

            let mut writer = bucket
                .begin_write("entry-1", 0, 10, "text/plain".to_string(), Labels::new())
                .wait()
                .unwrap();
            writer
                .send(Ok(Some(Bytes::from("0123456789"))))
                .await
                .unwrap();
            writer.send(Ok(None)).await.unwrap();

            let result = storage.rename_bucket("test", "new").wait();
            assert_eq!(result, Ok(()));

            let result = storage.get_bucket("test");
            assert_eq!(result.err(), Some(not_found!("Bucket 'test' is not found")));

            let bucket = storage.get_bucket("new").unwrap().upgrade_and_unwrap();
            assert_eq!(bucket.name(), "new");

            let mut reader = bucket.begin_read("entry-1", 0).wait().unwrap();
            let record = reader.read().await.unwrap().unwrap();
            assert_eq!(record, Bytes::from("0123456789"));
        }

        #[rstest]
        fn test_rename_bucket_with_non_existing_name(storage: Storage) {
            let result = storage.rename_bucket("test", "new").wait();
            assert_eq!(result, Err(not_found!("Bucket 'test' is not found")));
        }

        #[rstest]
        fn test_rename_bucket_with_existing_name(storage: Storage) {
            let bucket = storage
                .create_bucket("test", BucketSettings::default())
                .unwrap()
                .upgrade_and_unwrap();
            assert_eq!(bucket.name(), "test");

            let bucket = storage
                .create_bucket("new", BucketSettings::default())
                .unwrap()
                .upgrade_and_unwrap();
            assert_eq!(bucket.name(), "new");

            let result = storage.rename_bucket("test", "new").wait();
            assert_eq!(result, Err(conflict!("Bucket 'new' already exists")));
        }

        #[rstest]
        fn test_rename_bucket_with_invalid_name(storage: Storage) {
            let result = storage.rename_bucket("test", "new$").wait();
            assert_eq!(
                result,
                Err(unprocessable_entity!(
                    "Bucket or entry name can contain only letters, digests and [-,_] symbols"
                ))
            );
        }

        #[rstest]
        fn test_rename_provisioned_bucket(storage: Storage) {
            let bucket = storage
                .create_bucket("test", BucketSettings::default())
                .unwrap()
                .upgrade_and_unwrap();
            bucket.set_provisioned(true);
            let result = storage.rename_bucket("test", "new").wait();
            assert_eq!(
                result,
                Err(conflict!("Can't rename provisioned bucket 'test'"))
            );
        }
    }

    #[rstest]
    fn test_get_bucket_list(storage: Storage) {
        storage.create_bucket("test1", Bucket::defaults()).unwrap();
        storage.create_bucket("test2", Bucket::defaults()).unwrap();

        let bucket_list = storage.get_bucket_list().unwrap();
        assert_eq!(bucket_list.buckets.len(), 2);
        assert_eq!(bucket_list.buckets[0].name, "test1");
        assert_eq!(bucket_list.buckets[1].name, "test2");
    }

    #[rstest]
    fn test_provisioned_remove(storage: Storage) {
        let bucket = storage
            .create_bucket("test", BucketSettings::default())
            .unwrap()
            .upgrade_and_unwrap();
        bucket.set_provisioned(true);
        let err = storage.remove_bucket("test").wait().err().unwrap();
        assert_eq!(
            err,
            ReductError::conflict("Can't remove provisioned bucket 'test'")
        );
    }
    #[fixture]

    fn path() -> PathBuf {
        tempdir().unwrap().into_path()
    }

    #[fixture]
    fn storage(path: PathBuf) -> Storage {
        Storage::load(path, None)
    }
}
