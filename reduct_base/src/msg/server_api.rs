// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::msg::bucket_api::{BucketInfo, BucketSettings};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// License information
///
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct License {
    /// Licensee usually is the company name
    pub licensee: String,
    /// Invoice number
    pub invoice: String,
    /// Expiry date
    pub expiry_date: DateTime<Utc>,
    /// Plan name
    pub plan: String,
    /// Number of devices (0 for unlimited)
    pub device_number: u32,
    /// Disk quota in TB (0 for unlimited)
    pub disk_quota: i32,
    /// Fingerprint
    #[serde(default)]
    pub fingerprint: String,
}

impl Display for License {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "\n\tLicensee: {}\n\tInvoice: {}\n\tExpiry Date: {}\n\tPlan: {}\n\tNumber of Devices: {}\n\tDisk Quota: {} TB,\n\tFingerprint: {}",
            self.licensee, self.invoice, self.expiry_date, self.plan, self.device_number, self.disk_quota, self.fingerprint
        )
    }
}

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
    /// License information if it is None, then it is BUSL-1.1
    pub license: Option<License>,
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
