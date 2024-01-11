// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::msg::diagnostics::Diagnostics;
use crate::Labels;
use serde::{Deserialize, Serialize};

/// Replication settings
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ReplicationSettings {
    /// Source bucket
    pub src_bucket: String,
    /// Destination bucket
    pub dst_bucket: String,
    /// Destination host URL (e.g. https://reductstore.com)
    pub dst_host: String,
    /// Destination access token
    #[serde(default)]
    pub dst_token: String,
    /// Entries to replicate. If empty, all entries are replicated. Wildcards are supported.
    #[serde(default)]
    pub entries: Vec<String>,
    /// Labels to include
    #[serde(default)]
    pub include: Labels,
    /// Labels to exclude
    #[serde(default)]
    pub exclude: Labels,
}

/// Replication info
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ReplicationInfo {
    /// Replication name
    pub name: String,
    /// Remote instance is available and replication is active
    pub is_active: bool,
    /// Replication settings
    pub is_provisioned: bool,
    /// Number of records pending replication
    pub pending_records: u64,
}

/// Replication list
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct ReplicationList {
    /// Replication list
    pub replications: Vec<ReplicationInfo>,
}

/// Replication settings
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ReplicationFullInfo {
    /// Info
    pub info: ReplicationInfo,
    /// Settings
    pub settings: ReplicationSettings,
    /// Diagnostics
    pub diagnostics: Diagnostics,
}
