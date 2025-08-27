// Copyright 2023-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::core::file_cache::FILE_CACHE;
use crate::core::thread_pool::{unique, TaskHandle};
use crate::storage::bucket::Bucket;
use crate::storage::entry::EntrySettings;
use bytes::BytesMut;
use prost::Message;
use reduct_base::error::ReductError;
use reduct_base::msg::bucket_api::{BucketSettings, QuotaType};
use reduct_base::{conflict, internal_server_error};
use std::io::{SeekFrom, Write};

pub(super) const DEFAULT_MAX_RECORDS: u64 = 1024;
pub(super) const DEFAULT_MAX_BLOCK_SIZE: u64 = 64000000;
pub(crate) const SETTINGS_NAME: &str = "bucket.settings";

impl From<BucketSettings> for crate::storage::proto::BucketSettings {
    fn from(settings: BucketSettings) -> Self {
        crate::storage::proto::BucketSettings {
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

impl Into<BucketSettings> for crate::storage::proto::BucketSettings {
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

impl Bucket {
    /// Default settings for a new bucket bucket
    pub fn defaults() -> BucketSettings {
        BucketSettings {
            max_block_size: Some(DEFAULT_MAX_BLOCK_SIZE),
            quota_type: Some(QuotaType::NONE),
            quota_size: Some(0),
            max_block_records: Some(DEFAULT_MAX_RECORDS),
        }
    }

    /// Fill in missing settings with defaults
    pub(super) fn fill_settings(
        settings: BucketSettings,
        default: BucketSettings,
    ) -> BucketSettings {
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
    pub(super) fn save_settings(&self) -> TaskHandle<Result<(), ReductError>> {
        let settings = self.settings.read().unwrap().clone();
        let path = self.path.join(SETTINGS_NAME);

        unique(&self.task_group(), "save settings", move || {
            let mut buf = BytesMut::new();
            crate::storage::proto::BucketSettings::from(settings)
                .encode(&mut buf)
                .map_err(|e| internal_server_error!("Failed to encode bucket settings: {}", e))?;

            let file = FILE_CACHE
                .write_or_create(&path, SeekFrom::Start(0))?
                .upgrade()?;
            file.write()?.write_all(&buf)?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::bucket::tests::{bucket, settings};
    use crate::storage::bucket::Bucket;
    use reduct_base::msg::bucket_api::BucketSettings;
    use rstest::rstest;

    #[rstest]
    fn test_keep_settings_persistent(settings: BucketSettings, bucket: Bucket) {
        assert_eq!(bucket.settings(), settings);

        let bucket = Bucket::restore(bucket.path.clone()).unwrap();
        assert_eq!(bucket.name(), "test");
        assert_eq!(bucket.settings(), settings);
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
    fn test_set_settings_partially(settings: BucketSettings, bucket: Bucket) {
        let new_settings = BucketSettings {
            max_block_size: Some(100),
            quota_type: None,
            quota_size: None,
            max_block_records: None,
        };

        bucket.set_settings(new_settings).wait().unwrap();
        assert_eq!(bucket.settings().max_block_size.unwrap(), 100);
        assert_eq!(bucket.settings().quota_type, settings.quota_type);
        assert_eq!(bucket.settings().quota_size, settings.quota_size);
        assert_eq!(
            bucket.settings().max_block_records,
            settings.max_block_records
        );
    }

    #[rstest]
    fn test_apply_settings_to_entries(settings: BucketSettings, bucket: Bucket) {
        bucket.get_or_create_entry("entry-1").unwrap();
        bucket.get_or_create_entry("entry-2").unwrap();

        let mut new_settings = settings.clone();
        new_settings.max_block_size = Some(200);
        new_settings.max_block_records = Some(200);
        bucket.set_settings(new_settings).wait().unwrap();

        for entry in bucket.entries.read().unwrap().values() {
            assert_eq!(entry.settings().max_block_size, 200);
            assert_eq!(entry.settings().max_block_records, 200);
        }
    }
}
