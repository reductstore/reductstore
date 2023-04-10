// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use log::info;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::thread::sleep;
use std::time::{Duration, Instant};

use crate::core::status::HTTPError;
use crate::storage::bucket::Bucket;
use crate::storage::proto::bucket_settings::QuotaType;
use crate::storage::proto::{BucketInfoList, BucketSettings, Defaults, ServerInfo};

/// Storage is the main entry point for the storage service.
pub struct Storage {
    data_path: PathBuf,
    start_time: Instant,
    buckets: BTreeMap<String, Bucket>,
}

impl Storage {
    /// Create a new Storage
    pub fn new(data_path: PathBuf) -> Storage {
        if !data_path.exists() {
            info!("Folder '{}' doesn't exist. Create it.", data_path.display());
            std::fs::create_dir_all(&data_path).unwrap();
        }

        // restore buckets
        let mut buckets = BTreeMap::new();
        for entry in std::fs::read_dir(&data_path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                let bucket = Bucket::restore(path).unwrap();
                buckets.insert(bucket.name().to_string(), bucket);
            }
        }

        info!("Load {} buckets", buckets.len());

        Storage {
            data_path,
            start_time: Instant::now(),
            buckets,
        }
    }

    /// Get the server info.
    ///
    /// # Returns
    ///
    /// * `ServerInfo` - The server info or an HTTPError
    pub fn info(&self) -> Result<ServerInfo, HTTPError> {
        Ok(ServerInfo {
            version: option_env!("CARGO_PKG_VERSION")
                .unwrap_or("unknown")
                .to_string(),
            bucket_count: self.buckets.len() as u64,
            usage: 0,
            uptime: self.start_time.elapsed().as_secs(),
            oldest_record: 0,
            latest_record: 0,
            defaults: Some(Defaults {
                bucket: Some(Bucket::defaults()),
            }),
        })
    }

    /// Creat a new bucket.
    fn create_bucket(
        &mut self,
        name: &str,
        settings: BucketSettings,
    ) -> Result<&Bucket, HTTPError> {
        let regex = regex::Regex::new(r"^[A-Za-z0-9_-]*$").unwrap();
        if !regex.is_match(name) {
            return Err(HTTPError::unprocessable_entity(
                "Bucket name can contain only letters, digests and [-,_] symbols",
            ));
        }

        if self.buckets.contains_key(name) {
            return Err(HTTPError::conflict(
                format!("Bucket '{}' already exists", name).as_str(),
            ));
        }

        let bucket = Bucket::new(name, &self.data_path, settings)?;
        self.buckets.insert(name.to_string(), bucket);

        Ok(self.buckets.get(name).unwrap())
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
    pub fn get_bucket(&self, name: &str) -> Result<&Bucket, HTTPError> {
        match self.buckets.get(name) {
            Some(bucket) => Ok(bucket),
            None => Err(HTTPError::not_found(
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
    pub fn remove_bucket(&mut self, name: &str) -> Result<(), HTTPError> {
        match self.buckets.remove(name) {
            Some(bucket) => {
                bucket.remove()?;
                Ok(())
            }
            None => Err(HTTPError::not_found(
                format!("Bucket '{}' is not found", name).as_str(),
            )),
        }
    }

    pub fn get_bucket_list(&self) -> Result<BucketInfoList, HTTPError> {
        let mut buckets = Vec::new();
        for bucket in self.buckets.values() {
            buckets.push(bucket.info()?);
        }

        Ok(BucketInfoList { buckets })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_info() {
        let storage = Storage::new(tempdir().unwrap().into_path());

        sleep(Duration::from_secs(1)); // uptime is 1 second

        let info = storage.info().unwrap();
        assert_eq!(
            info,
            ServerInfo {
                version: "1.4.0".to_string(),
                bucket_count: 0,
                usage: 0,
                uptime: 1,
                oldest_record: 0,
                latest_record: 0,
                defaults: Some(Defaults {
                    bucket: Some(Bucket::defaults()),
                }),
            }
        );
    }

    #[test]
    fn test_recover_from_fs() {
        let path = tempdir().unwrap().into_path();
        let mut storage = Storage::new(path.clone());

        let bucket_settings = BucketSettings {
            quota_size: Some(100),
            quota_type: Some(QuotaType::Fifo as i32),
            ..Bucket::defaults()
        };
        let bucket = storage
            .create_bucket("test", bucket_settings.clone())
            .unwrap();
        assert_eq!(bucket.name(), "test");

        let storage = Storage::new(path);
        assert_eq!(
            storage.info().unwrap(),
            ServerInfo {
                version: "1.4.0".to_string(),
                bucket_count: 1,
                usage: 0,
                uptime: 0,
                oldest_record: 0,
                latest_record: 0,
                defaults: Some(Defaults {
                    bucket: Some(Bucket::defaults()),
                }),
            }
        );

        let bucket = storage.get_bucket("test").unwrap();
        assert_eq!(bucket.name(), "test");
        assert_eq!(bucket.settings(), &bucket_settings);
    }

    #[test]
    fn test_create_bucket() {
        let mut storage = Storage::new(tempdir().unwrap().into_path());
        let bucket = storage
            .create_bucket("test", BucketSettings::default())
            .unwrap();
        assert_eq!(bucket.name(), "test");
    }

    #[test]
    fn test_create_bucket_with_invalid_name() {
        let mut storage = Storage::new(tempdir().unwrap().into_path());
        let result = storage.create_bucket("test$", BucketSettings::default());
        assert_eq!(
            result,
            Err(HTTPError::unprocessable_entity(
                "Bucket name can contain only letters, digests and [-,_] symbols"
            ))
        );
    }

    #[test]
    fn test_create_bucket_with_existing_name() {
        let mut storage = Storage::new(tempdir().unwrap().into_path());
        let bucket = storage
            .create_bucket("test", BucketSettings::default())
            .unwrap();
        assert_eq!(bucket.name(), "test");

        let result = storage.create_bucket("test", BucketSettings::default());
        assert_eq!(
            result,
            Err(HTTPError::conflict("Bucket 'test' already exists"))
        );
    }

    #[test]
    fn test_get_bucket() {
        let mut storage = Storage::new(tempdir().unwrap().into_path());
        let bucket = storage
            .create_bucket("test", BucketSettings::default())
            .unwrap();
        assert_eq!(bucket.name(), "test");

        let bucket = storage.get_bucket("test").unwrap();
        assert_eq!(bucket.name(), "test");
    }

    #[test]
    fn test_get_bucket_with_non_existing_name() {
        let storage = Storage::new(tempdir().unwrap().into_path());
        let result = storage.get_bucket("test");
        assert_eq!(
            result,
            Err(HTTPError::not_found("Bucket 'test' is not found"))
        );
    }

    #[test]
    fn test_remove_bucket() {
        let mut storage = Storage::new(tempdir().unwrap().into_path());
        let bucket = storage
            .create_bucket("test", BucketSettings::default())
            .unwrap();
        assert_eq!(bucket.name(), "test");

        let result = storage.remove_bucket("test");
        assert_eq!(result, Ok(()));

        let result = storage.get_bucket("test");
        assert_eq!(
            result,
            Err(HTTPError::not_found("Bucket 'test' is not found"))
        );
    }

    #[test]
    fn test_remove_bucket_with_non_existing_name() {
        let mut storage = Storage::new(tempdir().unwrap().into_path());
        let result = storage.remove_bucket("test");
        assert_eq!(
            result,
            Err(HTTPError::not_found("Bucket 'test' is not found"))
        );
    }

    #[test]
    fn test_remove_bucket_persistent() {
        let path = tempdir().unwrap().into_path();
        let mut storage = Storage::new(path.clone());
        let bucket = storage
            .create_bucket("test", BucketSettings::default())
            .unwrap();
        assert_eq!(bucket.name(), "test");

        let result = storage.remove_bucket("test");
        assert_eq!(result, Ok(()));

        let storage = Storage::new(path);
        let result = storage.get_bucket("test");
        assert_eq!(
            result,
            Err(HTTPError::not_found("Bucket 'test' is not found"))
        );
    }

    #[test]
    fn test_get_bucket_list() {
        let mut storage = Storage::new(tempdir().unwrap().into_path());

        storage.create_bucket("test1", Bucket::defaults()).unwrap();
        storage.create_bucket("test2", Bucket::defaults()).unwrap();

        let bucket_list = storage.get_bucket_list().unwrap();
        assert_eq!(bucket_list.buckets.len(), 2);
        assert_eq!(bucket_list.buckets[0].name, "test1");
        assert_eq!(bucket_list.buckets[1].name, "test2");
    }
}
