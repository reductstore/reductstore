// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::msg::entry_api::EntryInfo;
use serde::{Deserialize, Serialize};

/// Quota type
///
/// NONE: No quota
/// FIFO: When quota_size is reached, the oldest records are deleted
#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
pub enum QuotaType {
    #[default]
    NONE = 0,
    FIFO = 1,
}

impl From<i32> for QuotaType {
    fn from(value: i32) -> Self {
        match value {
            0 => QuotaType::NONE,
            1 => QuotaType::FIFO,
            _ => QuotaType::NONE,
        }
    }
}

/// Bucket settings
#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
pub struct BucketSettings {
    /// Quota type see QuotaType
    pub quota_type: Option<QuotaType>,
    /// Quota size in bytes
    pub quota_size: Option<u64>,
    /// Max size of a block in bytes to start a new one
    pub max_block_size: Option<u64>,
    /// Max records in a block to start a new block one
    pub max_block_records: Option<u64>,
}

/// Bucket information
#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
pub struct BucketInfo {
    /// Unique bucket name
    pub name: String,
    /// Number of records in bucket
    pub entry_count: u64,
    /// Total size of bucket in bytes
    pub size: u64,
    /// Oldest record in bucket
    pub oldest_record: u64,
    /// Latest record in bucket
    pub latest_record: u64,
}

/// Full bucket information
#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
pub struct FullBucketInfo {
    /// Bucket information
    pub info: BucketInfo,
    /// Bucket settings
    pub settings: BucketSettings,
    /// Entries in bucket
    pub entries: Vec<EntryInfo>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    fn test_enum_as_string() {
        let settings = BucketSettings {
            quota_type: Some(QuotaType::FIFO),
            quota_size: Some(100),
            max_block_size: Some(100),
            max_block_records: Some(100),
        };
        let serialized = serde_json::to_string(&settings).unwrap();

        assert_eq!(
            serialized,
            r#"{"quota_type":"FIFO","quota_size":100,"max_block_size":100,"max_block_records":100}"#
        );
    }
}
