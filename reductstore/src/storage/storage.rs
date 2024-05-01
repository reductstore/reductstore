// Copyright 2023-2024 ReductStore
// Licensed under the Business Source License 1.1

use log::info;
use std::collections::BTreeMap;

use std::fs::remove_dir_all;
use std::path::PathBuf;

use std::time::Instant;

use crate::storage::bucket::Bucket;
use reduct_base::error::ReductError;

use reduct_base::msg::bucket_api::BucketSettings;
use reduct_base::msg::server_api::{BucketInfoList, Defaults, License, ServerInfo};

/// Storage is the main entry point for the storage service.
pub struct Storage {
    data_path: PathBuf,
    start_time: Instant,
    buckets: BTreeMap<String, Bucket>,
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
    pub(crate) async fn load(data_path: PathBuf, license: Option<License>) -> Storage {
        if !data_path.exists() {
            info!("Folder '{:?}' doesn't exist. Create it.", data_path);
            std::fs::create_dir_all(&data_path).unwrap();
        }

        // restore buckets
        let mut buckets = BTreeMap::new();
        for entry in std::fs::read_dir(&data_path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                let bucket = Bucket::restore(path.clone())
                    .await
                    .expect(format!("Failed to restore bucket '{:?}'", path).as_str());
                buckets.insert(bucket.name().to_string(), bucket);
            }
        }

        info!("Load {} buckets", buckets.len());

        Storage {
            data_path,
            start_time: Instant::now(),
            buckets,
            license,
        }
    }

    /// Get the reductstore info.
    ///
    /// # Returns
    ///
    /// * `ServerInfo` - The reductstore info or an HTTPError
    pub async fn info(&self) -> Result<ServerInfo, ReductError> {
        let mut usage = 0u64;
        let mut oldest_record = u64::MAX;
        let mut latest_record = 0u64;

        for bucket in self.buckets.values() {
            let bucket = bucket.info().await?.info;
            usage += bucket.size;
            oldest_record = oldest_record.min(bucket.oldest_record);
            latest_record = latest_record.max(bucket.latest_record);
        }

        Ok(ServerInfo {
            version: option_env!("CARGO_PKG_VERSION")
                .unwrap_or("unknown")
                .to_string(),
            bucket_count: self.buckets.len() as u64,
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
        &mut self,
        name: &str,
        settings: BucketSettings,
    ) -> Result<&mut Bucket, ReductError> {
        let regex = regex::Regex::new(r"^[A-Za-z0-9_-]*$").unwrap();
        if !regex.is_match(name) {
            return Err(ReductError::unprocessable_entity(
                "Bucket name can contain only letters, digests and [-,_] symbols",
            ));
        }

        if self.buckets.contains_key(name) {
            return Err(ReductError::conflict(
                format!("Bucket '{}' already exists", name).as_str(),
            ));
        }

        let bucket = Bucket::new(name, &self.data_path, settings)?;
        self.buckets.insert(name.to_string(), bucket);

        Ok(self.buckets.get_mut(name).unwrap())
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
    pub fn get_bucket(&self, name: &str) -> Result<&Bucket, ReductError> {
        match self.buckets.get(name) {
            Some(bucket) => Ok(bucket),
            None => Err(ReductError::not_found(
                format!("Bucket '{}' is not found", name).as_str(),
            )),
        }
    }

    /// Get a bucket by name.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the bucket
    ///
    /// # Returns
    ///
    /// * `Bucket` - The bucket or an HTTPError
    pub fn get_mut_bucket(&mut self, name: &str) -> Result<&mut Bucket, ReductError> {
        match self.buckets.get_mut(name) {
            Some(bucket) => Ok(bucket),
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
    pub fn remove_bucket(&mut self, name: &str) -> Result<(), ReductError> {
        if let Some(bucket) = self.buckets.get(name) {
            if bucket.is_provisioned() {
                return Err(ReductError::conflict(&format!(
                    "Can't remove provisioned bucket '{}'",
                    bucket.name()
                )));
            }
        }

        match self.buckets.remove(name) {
            Some(_) => {
                remove_dir_all(&self.data_path.join(name))?;
                Ok(())
            }
            None => Err(ReductError::not_found(
                format!("Bucket '{}' is not found", name).as_str(),
            )),
        }
    }

    pub async fn get_bucket_list(&self) -> Result<BucketInfoList, ReductError> {
        let mut buckets = Vec::new();
        for bucket in self.buckets.values() {
            buckets.push(bucket.info().await?.info);
        }

        Ok(BucketInfoList { buckets })
    }

    pub fn data_path(&self) -> &PathBuf {
        &self.data_path
    }
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
    #[tokio::test]
    async fn test_info(#[future] storage: Storage) {
        let storage = storage.await;
        sleep(Duration::from_secs(1)); // uptime is 1 second

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
    async fn test_recover_from_fs(#[future] storage: Storage) {
        let mut storage = storage.await;
        let bucket_settings = BucketSettings {
            quota_size: Some(100),
            quota_type: Some(QuotaType::FIFO),
            ..Bucket::defaults()
        };
        let bucket = storage
            .create_bucket("test", bucket_settings.clone())
            .unwrap();
        assert_eq!(bucket.name(), "test");

        macro_rules! write_entry {
            ($bucket:expr, $entry_name:expr, $record_ts:expr) => {
                let entry = $bucket.get_or_create_entry($entry_name).unwrap();
                let sender = entry
                    .begin_write($record_ts, 10, "text/plain".to_string(), Labels::new())
                    .await
                    .unwrap();
                sender
                    .send(Ok(Some(Bytes::from("0123456789"))))
                    .await
                    .unwrap();
            };
        }

        write_entry!(bucket, "entry-1", 1000);
        write_entry!(bucket, "entry-2", 2000);
        write_entry!(bucket, "entry-2", 5000);

        tokio::time::sleep(Duration::from_micros(1)).await; // to make sure that write tasks are completed
        let storage = Storage::load(storage.data_path.clone(), None).await;
        assert_eq!(
            storage.info().await.unwrap(),
            ServerInfo {
                version: env!("CARGO_PKG_VERSION").to_string(),
                bucket_count: 1,
                usage: 127,
                uptime: 0,
                oldest_record: 1000,
                latest_record: 5000,
                defaults: Defaults {
                    bucket: Bucket::defaults(),
                },
                license: None,
            }
        );

        let bucket = storage.get_bucket("test").unwrap();
        assert_eq!(bucket.name(), "test");
        assert_eq!(bucket.settings(), &bucket_settings);
    }

    #[rstest]
    #[tokio::test]
    async fn test_license_info(#[future] storage: Storage) {
        let license = License {
            licensee: "ReductStore".to_string(),
            invoice: "2021-0001".to_string(),
            expiry_date: chrono::Utc::now(),
            plan: "Enterprise".to_string(),
            device_number: 100,
            disk_quota: 100,
            fingerprint: "fingerprint".to_string(),
        };

        let storage = Storage::load(storage.await.data_path, Some(license.clone())).await;
        assert_eq!(storage.info().await.unwrap().license, Some(license));
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_bucket(#[future] storage: Storage) {
        let mut storage = storage.await;
        let bucket = storage
            .create_bucket("test", BucketSettings::default())
            .unwrap();
        assert_eq!(bucket.name(), "test");
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_bucket_with_invalid_name(#[future] storage: Storage) {
        let mut storage = storage.await;
        let result = storage.create_bucket("test$", BucketSettings::default());
        assert_eq!(
            result.err(),
            Some(ReductError::unprocessable_entity(
                "Bucket name can contain only letters, digests and [-,_] symbols"
            ))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_bucket_with_existing_name(#[future] storage: Storage) {
        let mut storage = storage.await;
        let bucket = storage
            .create_bucket("test", BucketSettings::default())
            .unwrap();
        assert_eq!(bucket.name(), "test");

        let result = storage.create_bucket("test", BucketSettings::default());
        assert_eq!(
            result.err(),
            Some(ReductError::conflict("Bucket 'test' already exists"))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_bucket(#[future] storage: Storage) {
        let mut storage = storage.await;
        let bucket = storage
            .create_bucket("test", BucketSettings::default())
            .unwrap();
        assert_eq!(bucket.name(), "test");

        let bucket = storage.get_bucket("test").unwrap();
        assert_eq!(bucket.name(), "test");
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_bucket_with_non_existing_name(#[future] storage: Storage) {
        let storage = storage.await;
        let result = storage.get_bucket("test");
        assert_eq!(
            result.err(),
            Some(ReductError::not_found("Bucket 'test' is not found"))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_bucket(#[future] storage: Storage) {
        let mut storage = storage.await;
        let bucket = storage
            .create_bucket("test", BucketSettings::default())
            .unwrap();
        assert_eq!(bucket.name(), "test");

        let result = storage.remove_bucket("test");
        assert_eq!(result, Ok(()));

        let result = storage.get_bucket("test");
        assert_eq!(
            result.err(),
            Some(ReductError::not_found("Bucket 'test' is not found"))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_bucket_with_non_existing_name(#[future] mut storage: Storage) {
        let result = storage.await.remove_bucket("test");
        assert_eq!(
            result,
            Err(ReductError::not_found("Bucket 'test' is not found"))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_bucket_persistent(path: PathBuf, #[future] storage: Storage) {
        let mut storage = storage.await;
        let bucket = storage
            .create_bucket("test", BucketSettings::default())
            .unwrap();
        assert_eq!(bucket.name(), "test");

        let result = storage.remove_bucket("test");
        assert_eq!(result, Ok(()));

        let storage = Storage::load(path, None).await;
        let result = storage.get_bucket("test");
        assert_eq!(
            result.err(),
            Some(ReductError::not_found("Bucket 'test' is not found"))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_bucket_list(#[future] storage: Storage) {
        let mut storage = storage.await;
        storage.create_bucket("test1", Bucket::defaults()).unwrap();
        storage.create_bucket("test2", Bucket::defaults()).unwrap();

        let bucket_list = storage.get_bucket_list().await.unwrap();
        assert_eq!(bucket_list.buckets.len(), 2);
        assert_eq!(bucket_list.buckets[0].name, "test1");
        assert_eq!(bucket_list.buckets[1].name, "test2");
    }

    #[rstest]
    #[tokio::test]
    async fn test_provisioned_remove(#[future] storage: Storage) {
        let mut storage = storage.await;
        let bucket = storage
            .create_bucket("test", BucketSettings::default())
            .unwrap();
        bucket.set_provisioned(true);
        let err = storage.remove_bucket("test").err().unwrap();
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
    async fn storage(path: PathBuf) -> Storage {
        Storage::load(path, None).await
    }
}
