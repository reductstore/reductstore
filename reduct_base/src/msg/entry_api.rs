use std::collections::HashMap;
use std::str::FromStr;
// Copyright 2023 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
use serde::{Deserialize, Serialize};
use serde_json::Value;

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

/// Rename Entry
#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
pub struct RenameEntry {
    /// New entry name
    pub new_name: String,
}

#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
pub enum QueryType {
    /// Query records in entry
    #[default]
    #[serde(rename = "QUERY")]
    Query = 0,
    /// Remove records in entry
    #[serde(rename = "REMOVE")]
    Remove = 1,
}

/// Query records in entry
#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
pub struct QueryEntry {
    pub query_type: QueryType,

    /// Start query from (Unix timestamp in microseconds)
    pub start: Option<u64>,
    /// Stop query at (Unix timestamp in microseconds)
    pub stop: Option<u64>,

    /// Include records with label
    pub include: Option<HashMap<String, String>>,
    /// Exclude records with label
    pub exclude: Option<HashMap<String, String>>,
    /// Return a record every S seconds
    pub each_s: Option<f64>,
    /// Return a record every N records
    pub each_n: Option<u64>,
    /// Limit the number of records returned
    pub limit: Option<u64>,

    /// TTL of query in seconds
    pub ttl: Option<u64>,
    /// Retrieve only metadata
    pub only_metadata: Option<bool>,
    /// Continue query from last result
    pub continuous: Option<bool>,

    /// Conditional query
    pub when: Option<Value>,
}
