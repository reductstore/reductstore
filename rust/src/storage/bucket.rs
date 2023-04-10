// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use log::debug;
use prost::bytes::{Bytes, BytesMut};
use prost::Message;
use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;

use crate::core::status::HTTPError;
use crate::storage::entry::Entry;
use crate::storage::proto::bucket_settings::QuotaType;
use crate::storage::proto::{bucket_settings, BucketInfo, BucketSettings};

const DEFAULT_MAX_RECORDS: u64 = 1024;
const DEFAULT_MAX_BLOCK_SIZE: u64 = 64000000;
const SETTINGS_NAME: &str = "bucket.settings";

/// Bucket is a single storage bucket.
#[derive(PartialEq, Debug)]
pub struct Bucket {
    name: String,
    path: PathBuf,
    entries: BTreeMap<String, Entry>,
    settings: BucketSettings,
}

impl Bucket {
    /// Create a new Bucket
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the bucket
    /// * `path` - The path to folder with buckets
    /// * `settings` - The settings for the bucket
    ///
    /// # Returns
    ///
    /// * `Bucket` - The bucket or an HTTPError
    pub(crate) fn new(
        name: &str,
        path: &PathBuf,
        settings: BucketSettings,
    ) -> Result<Bucket, HTTPError> {
        let path = path.join(name);
        std::fs::create_dir_all(&path)?;

        let settings = Self::fill_settings(settings, Self::defaults());
        let bucket = Bucket {
            name: name.to_string(),
            path,
            entries: BTreeMap::new(),
            settings,
        };

        bucket.save_settings()?;
        Ok(bucket)
    }

    /// Restore a Bucket from disk
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the bucket
    ///
    /// # Returns
    ///
    /// * `Bucket` - The bucket or an HTTPError
    pub fn restore(path: PathBuf) -> Result<Bucket, HTTPError> {
        let mut entries = BTreeMap::new();
        for entry in std::fs::read_dir(&path)? {
            let path = entry?.path();
            if path.is_dir() {
                let entry = Entry::restore(path)?;
                entries.insert(entry.name().to_string(), entry);
            }
        }

        let buf: Vec<u8> = std::fs::read(path.join(SETTINGS_NAME))?;
        let settings = BucketSettings::decode(&mut Bytes::from(buf)).map_err(|e| {
            HTTPError::internal_server_error(format!("Failed to decode settings: {}", e).as_str())
        })?;

        let settings = Self::fill_settings(settings, Self::defaults());

        Ok(Bucket {
            name: path.file_name().unwrap().to_str().unwrap().to_string(),
            path,
            entries,
            settings,
        })
    }

    /// Default settings for a new bucket bucket
    pub fn defaults() -> BucketSettings {
        BucketSettings {
            max_block_size: Some(DEFAULT_MAX_RECORDS),
            quota_type: Some(QuotaType::None as i32),
            quota_size: Some(0),
            max_block_records: Some(DEFAULT_MAX_RECORDS),
        }
    }

    pub fn get_or_create_entry(&mut self, key: &str) -> Result<&Entry, HTTPError> {
        if !self.entries.contains_key(key) {
            let entry = Entry::new(&key, self.path.clone());
            self.entries.insert(key.to_string(), entry);
        }

        Ok(self.entries.get(key).unwrap())
    }

    /// Remove a Bucket from disk
    ///
    /// # Returns
    ///
    /// * `Result<(), HTTPError>` - The result or an HTTPError
    pub fn remove(&self) -> Result<(), HTTPError> {
        std::fs::remove_dir_all(&self.path)?;
        Ok(())
    }

    /// Return bucket stats
    pub fn info(&self) -> Result<BucketInfo, HTTPError> {
        Ok(BucketInfo {
            name: self.name.clone(),
            size: 0,
            entry_count: self.entries.len() as u64,
            oldest_record: 0,
            latest_record: 0,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn settings(&self) -> &BucketSettings {
        &self.settings
    }

    pub fn set_settings(&mut self, settings: BucketSettings) -> Result<(), HTTPError> {
        self.settings = Self::fill_settings(settings, Self::defaults());
        self.save_settings()?;
        Ok(())
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

    fn save_settings(&self) -> Result<(), HTTPError> {
        let path = self.path.join(SETTINGS_NAME);
        let mut buf = BytesMut::new();
        self.settings.encode(&mut buf).map_err(|e| {
            HTTPError::internal_server_error(
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

    #[test]
    fn test_keep_settings_peristent() {
        let path = std::env::temp_dir().join("test_keep_settings_peristent");
        std::fs::create_dir_all(&path).unwrap();

        let init_settings = BucketSettings {
            max_block_size: Some(100),
            quota_type: Some(QuotaType::Fifo as i32),
            quota_size: Some(1000),
            max_block_records: Some(100),
        };

        let bucket = Bucket::new("test", &path, init_settings.clone()).unwrap();
        let settings = bucket.settings();
        assert_eq!(settings, &init_settings);

        let bucket = Bucket::restore(path).unwrap();
        let settings = bucket.settings();
        assert_eq!(settings, &init_settings);
    }

    #[test]
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
}
