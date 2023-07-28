// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use log::debug;
use prost::bytes::{Bytes, BytesMut};
use prost::Message;

use std::collections::BTreeMap;
use std::fs::remove_dir_all;
use std::io::Write;
use std::path::PathBuf;

use std::sync::{Arc, RwLock};

use crate::storage::entry::{Entry, EntrySettings, Labels};
use crate::storage::proto::BucketSettings as ProtoBucketSettings;
use crate::storage::reader::RecordReader;
use crate::storage::writer::RecordWriter;
use reduct_base::error::HttpError;
use reduct_base::msg::bucket_api::{BucketInfo, BucketSettings, FullBucketInfo, QuotaType};
use reduct_base::msg::entry_api::EntryInfo;

const DEFAULT_MAX_RECORDS: u64 = 256;
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
    ) -> Result<Bucket, HttpError> {
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
    pub fn restore(path: PathBuf) -> Result<Bucket, HttpError> {
        let buf: Vec<u8> = std::fs::read(path.join(SETTINGS_NAME))?;
        let settings = ProtoBucketSettings::decode(&mut Bytes::from(buf)).map_err(|e| {
            HttpError::internal_server_error(format!("Failed to decode settings: {}", e).as_str())
        })?;

        let settings = Self::fill_settings(settings.into(), Self::defaults());

        let mut entries = BTreeMap::new();
        for entry in std::fs::read_dir(&path)? {
            let path = entry?.path();
            if path.is_dir() {
                let entry = Entry::restore(
                    path,
                    EntrySettings {
                        max_block_size: settings.max_block_size.unwrap(),
                        max_block_records: settings.max_block_records.unwrap(),
                    },
                )?;
                entries.insert(entry.name().to_string(), entry);
            }
        }

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
    pub fn get_or_create_entry(&mut self, key: &str) -> Result<&mut Entry, HttpError> {
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
    pub fn get_entry(&self, name: &str) -> Result<&Entry, HttpError> {
        let entry = self.entries.get(name).ok_or_else(|| {
            HttpError::not_found(&format!(
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
    pub fn get_mut_entry(&mut self, name: &str) -> Result<&mut Entry, HttpError> {
        let entry = self.entries.get_mut(name).ok_or_else(|| {
            HttpError::not_found(&format!(
                "Entry '{}' not found in bucket '{}'",
                name, self.name
            ))
        })?;
        Ok(entry)
    }

    /// Return bucket stats
    pub fn info(&self) -> Result<FullBucketInfo, HttpError> {
        let mut size = 0;
        let mut oldest_record = u64::MAX;
        let mut latest_record = 0u64;
        let mut entries: Vec<EntryInfo> = vec![];
        for entry in self.entries.values() {
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
                entry_count: self.entries.len() as u64,
                oldest_record,
                latest_record,
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
    /// * `RecordWriter` - The record writer to write the record content in chunks.
    /// * `HTTPError` - The error if any.
    pub fn begin_write(
        &mut self,
        name: &str,
        time: u64,
        content_size: u64,
        content_type: String,
        labels: Labels,
    ) -> Result<Arc<RwLock<RecordWriter>>, HttpError> {
        self.keep_quota_for(content_size)?;
        let entry = self.get_or_create_entry(name)?;
        entry.begin_write(time, content_size, content_type, labels)
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
    pub fn begin_read(
        &self,
        name: &str,
        time: u64,
    ) -> Result<Arc<RwLock<RecordReader>>, HttpError> {
        let entry = self.get_entry(name)?;
        entry.begin_read(time)
    }

    /// Get the next record from the entry
    ///
    /// # Arguments
    ///
    /// * `name` - Entry name.
    /// * `time` - The timestamp of the record.
    ///
    /// # Returns
    ///
    /// * `RecordReader` - The record reader to read the record content in chunks.
    /// * `bool` - True if the record is the last one.
    /// * `HTTPError` - The error if any.
    pub fn next(
        &mut self,
        name: &str,
        time: u64,
    ) -> Result<(Arc<RwLock<RecordReader>>, bool), HttpError> {
        let entry = self.get_mut_entry(name)?;
        entry.next(time)
    }

    fn keep_quota_for(&mut self, content_size: u64) -> Result<(), HttpError> {
        match self.settings.quota_type.clone().unwrap_or(QuotaType::NONE) {
            QuotaType::NONE => Ok(()),
            QuotaType::FIFO => {
                let mut size = self.info()?.info.size + content_size;
                while size > self.settings.quota_size.unwrap_or(0) {
                    debug!(
                        "Need more space. Remove an oldest block from bucket '{}'",
                        self.name()
                    );

                    let mut candidates: Vec<&Entry> = self
                        .entries
                        .iter()
                        .map(|entry| entry.1)
                        .collect::<Vec<&Entry>>();
                    candidates.sort_by_key(|entry| match entry.info() {
                        Ok(info) => info.oldest_record,
                        Err(_) => u64::MAX, //todo: handle error
                    });

                    let candidates = candidates
                        .iter()
                        .map(|entry| entry.name().to_string())
                        .collect::<Vec<String>>();

                    let mut success = false;
                    for name in candidates {
                        debug!("Remove an oldest block from entry '{}'", name);
                        match self
                            .entries
                            .get_mut(&name)
                            .unwrap()
                            .try_remove_oldest_block()
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
                        return Err(HttpError::internal_server_error(
                            format!("Failed to keep quota of '{}'", self.name()).as_str(),
                        ));
                    }

                    size = self.info()?.info.size + content_size;
                }

                // Remove empty entries
                for name in self
                    .entries
                    .iter()
                    .filter(|entry| entry.1.info().unwrap().size == 0)
                    .map(|entry| entry.0.clone())
                    .collect::<Vec<String>>()
                {
                    debug!("Remove empty entry '{}'", name);
                    self.remove_entry(&name)?;
                }
                Ok(())
            }
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn settings(&self) -> &BucketSettings {
        &self.settings
    }

    pub fn set_settings(&mut self, settings: BucketSettings) -> Result<(), HttpError> {
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

    /// Save bucket settings to file
    fn remove_entry(&mut self, name: &str) -> Result<(), HttpError> {
        let path = self.path.join(name);
        remove_dir_all(path)?;
        self.entries.remove(name);
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

    fn save_settings(&self) -> Result<(), HttpError> {
        let path = self.path.join(SETTINGS_NAME);
        let mut buf = BytesMut::new();
        ProtoBucketSettings::from(self.settings.clone())
            .encode(&mut buf)
            .map_err(|e| {
                HttpError::internal_server_error(
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
    use crate::storage::entry::Labels;
    use crate::storage::writer::Chunk;
    use rstest::{fixture, rstest};
    use tempfile::tempdir;

    #[rstest]
    fn test_keep_settings_persistent(settings: BucketSettings, bucket: Bucket) {
        assert_eq!(bucket.settings(), &settings);

        let bucket = Bucket::restore(bucket.path.clone()).unwrap();
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
    fn test_quota_keeping(path: PathBuf) {
        let mut bucket = bucket(
            BucketSettings {
                max_block_size: Some(5),
                quota_type: Some(QuotaType::FIFO),
                quota_size: Some(10),
                max_block_records: Some(100),
            },
            path,
        );

        write(&mut bucket, "test-1", 0, b"test").unwrap();
        assert_eq!(bucket.info().unwrap().info.size, 4);

        write(&mut bucket, "test-2", 1, b"test").unwrap();
        assert_eq!(bucket.info().unwrap().info.size, 8);

        write(&mut bucket, "test-3", 2, b"test").unwrap();
        assert_eq!(bucket.info().unwrap().info.size, 8);

        assert_eq!(
            read(&mut bucket, "test-1", 0).err(),
            Some(HttpError::not_found(
                "Entry 'test-1' not found in bucket 'test'"
            ))
        );
    }

    #[rstest]
    fn test_blob_bigger_than_quota(path: PathBuf) {
        let mut bucket = bucket(
            BucketSettings {
                max_block_size: Some(5),
                quota_type: Some(QuotaType::FIFO),
                quota_size: Some(10),
                max_block_records: Some(100),
            },
            path,
        );

        write(&mut bucket, "test-1", 0, b"test").unwrap();
        assert_eq!(bucket.info().unwrap().info.size, 4);

        let result = write(&mut bucket, "test-2", 1, b"0123456789___");
        assert_eq!(
            result.err(),
            Some(HttpError::internal_server_error(
                "Failed to keep quota of 'test'"
            ))
        );
    }

    fn write(
        bucket: &mut Bucket,
        entry_name: &str,
        time: u64,
        content: &'static [u8],
    ) -> Result<(), HttpError> {
        let writer = bucket.begin_write(
            entry_name,
            time,
            content.len() as u64,
            "".to_string(),
            Labels::new(),
        )?;
        writer
            .write()
            .unwrap()
            .write(Chunk::Last(Bytes::from(content)))?;
        Ok(())
    }

    fn read(bucket: &mut Bucket, entry_name: &str, time: u64) -> Result<Vec<u8>, HttpError> {
        let reader = bucket.begin_read(entry_name, time)?;
        let data = reader.write().unwrap().read()?.unwrap();
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
}
