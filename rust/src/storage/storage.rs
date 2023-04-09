// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::path::PathBuf;
use std::time::{Duration, Instant};
use std::thread::sleep;

use crate::core::status::HTTPError;
use crate::storage::proto::{ServerInfo, Defaults, BucketSettings};
use crate::storage::proto::bucket_settings::QuotaType;

const DEFAULT_MAX_RECORDS: u64 = 1024;
const DEFAULT_MAX_BLOCK_SIZE: u64 = 64000000;

/// Storage is the main entry point for the storage service.
struct Storage {
    data_path: PathBuf,
    start_time: Instant,
}

impl Storage {
    /// Create a new Storage
    pub fn new(data_path: PathBuf) -> Storage {
        Storage {
            data_path,
            start_time: Instant::now(),
        }
    }

    /// Get the server info.
    ///
    /// # Returns
    ///
    /// * `ServerInfo` - The server info or an HTTPError
    pub fn info(&self) -> Result<ServerInfo, HTTPError> {
        Ok(ServerInfo {
            version: option_env!("CARGO_PKG_VERSION").unwrap_or("unknown").to_string(),
            bucket_count: 0,
            usage: 0,
            uptime: self.start_time.elapsed().as_secs(),
            oldest_record: 0,
            latest_record: 0,
            defaults: Some(Defaults {
                bucket: Some(BucketSettings {
                    quota_type: Some(QuotaType::None as i32),
                    quota_size: Some(0),
                    max_block_records: Some(DEFAULT_MAX_RECORDS),
                    max_block_size: Some(DEFAULT_MAX_BLOCK_SIZE),
                })
            }),
        })
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_info() {
        let storage = Storage::new(tempdir().unwrap().into_path());

        sleep(Duration::from_secs(1));  // uptime is 1 second

        let info = storage.info().unwrap();
        assert_eq!(info, ServerInfo {
            version: "1.4.0".to_string(),
            bucket_count: 0,
            usage: 0,
            uptime: 1,
            oldest_record: 0,
            latest_record: 0,
            defaults: Some(Defaults {
                bucket: Some(BucketSettings {
                    quota_type: Some(QuotaType::None as i32),
                    quota_size: Some(0),
                    max_block_records: Some(DEFAULT_MAX_RECORDS),
                    max_block_size: Some(DEFAULT_MAX_BLOCK_SIZE),
                })
            }),
        });
    }
}
