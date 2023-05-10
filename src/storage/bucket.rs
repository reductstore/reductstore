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

use crate::core::status::HttpError;
use crate::storage::entry::{Entry, EntrySettings, Labels};
use crate::storage::proto::bucket_settings::QuotaType;
use crate::storage::proto::{BucketInfo, BucketSettings, EntryInfo, FullBucketInfo};
use crate::storage::reader::RecordReader;
use crate::storage::writer::RecordWriter;

const DEFAULT_MAX_RECORDS: u64 = 1024;
const DEFAULT_MAX_BLOCK_SIZE: u64 = 64000000;
const SETTINGS_NAME: &str = "bucket.settings";

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
        let settings = BucketSettings::decode(&mut Bytes::from(buf)).map_err(|e| {
            HttpError::internal_server_error(format!("Failed to decode settings: {}", e).as_str())
        })?;

        let settings = Self::fill_settings(settings, Self::defaults());

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
            quota_type: Some(QuotaType::None as i32),
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
    /// * `&mut Entry` - The entry or an HTTPError
    pub fn get_entry(&mut self, name: &str) -> Result<&mut Entry, HttpError> {
        let entry = self.entries.get_mut(name).ok_or_else(|| {
            HttpError::not_found(&format!(
                "Entry '{}' not found in bucket '{}'",
                name, self.name
            ))
        })?;
        Ok(entry)
    }

    /// Remove a Bucket from disk
    ///
    /// # Returns
    ///
    /// * `Result<(), HTTPError>` - The result or an HTTPError
    pub fn remove(&self) -> Result<(), HttpError> {
        std::fs::remove_dir_all(&self.path)?;
        Ok(())
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
            info: Some(BucketInfo {
                name: self.name.clone(),
                size,
                entry_count: self.entries.len() as u64,
                oldest_record,
                latest_record,
            }),
            settings: Some(self.settings.clone()),
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
        &mut self,
        name: &str,
        time: u64,
    ) -> Result<Arc<RwLock<RecordReader>>, HttpError> {
        let entry = self.get_entry(name)?;
        entry.begin_read(time)
    }

    fn keep_quota_for(&mut self, content_size: u64) -> Result<(), HttpError> {
        match QuotaType::from_i32(self.settings.quota_type.unwrap()).unwrap() {
            QuotaType::None => Ok(()),
            QuotaType::Fifo => {
                let mut size = self.info()?.info.unwrap().size + content_size;
                while size > self.settings.quota_size() {
                    debug!(
                        "Need more space.  Try to remove an oldest block from bucket '{}'",
                        self.name()
                    );

                    let mut candidates: Vec<&mut Entry> = self
                        .entries
                        .iter_mut()
                        .map(|entry| entry.1)
                        .collect::<Vec<&mut Entry>>();
                    candidates.sort_by_key(|entry| match entry.info() {
                        Ok(info) => info.oldest_record,
                        Err(_) => u64::MAX, //todo: handle error
                    });

                    let mut success = false;
                    for entry in candidates {
                        match entry.try_remove_oldest_block() {
                            Ok(_) => {
                                success = true;
                                break;
                            }
                            Err(e) => {
                                debug!(
                                    "Failed to remove oldest block from entry '{}': {}",
                                    entry.name(),
                                    e
                                );
                            }
                        }
                    }

                    if !success {
                        return Err(HttpError::internal_server_error(
                            format!("Failed to keep quota of '{}'", self.name()).as_str(),
                        ));
                    }

                    size = self.info()?.info.unwrap().size + content_size;
                }

                // Remove empty entries
                let mut entries_to_remove: Vec<String> = Vec::new();
                for entry in self.entries.values_mut() {
                    if entry.info()?.size == 0 {
                        remove_dir_all(self.path.join(entry.name()))?;
                        entries_to_remove.push(entry.name().to_string());
                    }
                }
                self.entries
                    .retain(|_, entry| !entries_to_remove.contains(&entry.name().to_string()));

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
        self.settings = Self::fill_settings(settings, Self::defaults());
        for entry in self.entries.values_mut() {
            entry.set_settings(EntrySettings {
                max_block_size: self.settings.max_block_size.unwrap(),
                max_block_records: self.settings.max_block_records.unwrap(),
            });
        }
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

    fn save_settings(&self) -> Result<(), HttpError> {
        let path = self.path.join(SETTINGS_NAME);
        let mut buf = BytesMut::new();
        self.settings.encode(&mut buf).map_err(|e| {
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
    use tempfile::tempdir;

    #[test]
    fn test_keep_settings_persistent() {
        let (bucket, init_settings, path) = setup();

        assert_eq!(bucket.settings(), &init_settings);

        let bucket = Bucket::restore(path.join("test")).unwrap();
        assert_eq!(bucket.name(), "test");
        assert_eq!(bucket.settings(), &init_settings);
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

    #[test]
    fn test_apply_settings_to_entries() {
        let (mut bucket, init_settings, _) = setup();

        bucket.get_or_create_entry("entry-1").unwrap();
        bucket.get_or_create_entry("entry-2").unwrap();

        let mut new_settings = init_settings.clone();
        new_settings.max_block_size = Some(200);
        new_settings.max_block_records = Some(200);
        bucket.set_settings(new_settings).unwrap();

        for entry in bucket.entries.values() {
            assert_eq!(entry.settings().max_block_size, 200);
            assert_eq!(entry.settings().max_block_records, 200);
        }
    }

    #[test]
    fn test_quota_keeping() {
        let (mut bucket, _) = setup_with(BucketSettings {
            max_block_size: Some(5),
            quota_type: Some(i32::from(QuotaType::Fifo)),
            quota_size: Some(10),
            max_block_records: Some(100),
        });

        write(&mut bucket, "test-1", 0, b"test").unwrap();
        assert_eq!(bucket.info().unwrap().info.unwrap().size, 4);

        write(&mut bucket, "test-2", 1, b"test").unwrap();
        assert_eq!(bucket.info().unwrap().info.unwrap().size, 8);

        write(&mut bucket, "test-3", 2, b"test").unwrap();
        assert_eq!(bucket.info().unwrap().info.unwrap().size, 8);

        assert_eq!(
            read(&mut bucket, "test-1", 0).err(),
            Some(HttpError::not_found(
                "Entry 'test-1' not found in bucket 'test'"
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

    fn setup() -> (Bucket, BucketSettings, PathBuf) {
        let path = tempdir().unwrap().into_path();
        std::fs::create_dir_all(&path).unwrap();

        let init_settings = BucketSettings {
            max_block_size: Some(100),
            quota_type: Some(QuotaType::Fifo as i32),
            quota_size: Some(1000),
            max_block_records: Some(100),
        };

        let bucket = Bucket::new("test", &path, init_settings.clone()).unwrap();
        (bucket, init_settings, path)
    }

    fn setup_with(settings: BucketSettings) -> (Bucket, PathBuf) {
        let path = tempdir().unwrap().into_path();
        std::fs::create_dir_all(&path).unwrap();

        let bucket = Bucket::new("test", &path, settings.clone()).unwrap();
        (bucket, path)
    }
}
