// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::Labels;
use serde::{Deserialize, Serialize};

// Replication settings
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
