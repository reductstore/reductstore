// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::msg::bucket_api::{BucketInfo, BucketSettings};
use serde::{Deserialize, Serialize};

/// Information about a ReductStore instance
#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
pub struct ServerInfo {
    pub version: String,
    pub bucket_count: u64,
    pub usage: u64,
    pub uptime: u64,
    pub oldest_record: u64,
    pub latest_record: u64,
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
