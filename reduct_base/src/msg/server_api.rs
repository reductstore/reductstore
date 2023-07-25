// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::msg::bucket_api::{BucketInfo, BucketSettings};
use serde::{Deserialize, Serialize};

/// Information about a ReductStore instance
#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
pub struct ServerInfo {
    /// Version of ReductStore instance
    pub version: String,
    /// Number of buckets
    pub bucket_count: u64,
    /// Disk usage in bytes
    pub usage: u64,
    /// Uptime in seconds
    pub uptime: u64,
    /// Oldest record in instance
    pub oldest_record: u64,
    /// Latest record in instance
    pub latest_record: u64,
    /// Default settings
    pub defaults: Defaults,
}

/// Default settings
#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
pub struct Defaults {
    pub bucket: BucketSettings,
}

/// BucketLists
#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
pub struct BucketInfoList {
    pub buckets: Vec<BucketInfo>,
}
