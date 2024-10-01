// Copyright 2023 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
use serde::{Deserialize, Serialize};

/// Stats of entry
#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
pub struct EntryInfo {
    /// Entry name
    pub name: String,
    /// Size of entry in bytes
    pub size: u64,
    /// Number of records in entry
    pub record_count: u64,
    /// Number of blocks in entry
    pub block_count: u64,
    /// Oldest record in entry
    pub oldest_record: u64,
    /// Latest record in entry
    pub latest_record: u64,
}

/// Query Info
#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
pub struct QueryInfo {
    /// Unique query name
    pub id: u64,
}

/// Remove Query Info
#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
pub struct RemoveQueryInfo {
    /// Unique query name
    pub removed_records: u64,
}

#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
pub struct RenameEnry {
    /// New entry name
    pub new_name: String,
}
