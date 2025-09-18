// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::msg::entry_api::QueryEntry;
use chrono::serde::ts_seconds::deserialize as as_ts;
use chrono::serde::ts_seconds::serialize as to_ts;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
/// Request to create a query link for sharing
pub struct QueryLinkCreateRequest {
    /// Expiration time
    #[serde(deserialize_with = "as_ts", serialize_with = "to_ts")]
    pub expire_at: DateTime<Utc>,
    /// Bucket name
    pub bucket: String,
    /// Entry name
    pub entry: String,
    /// Record index
    pub index: Option<u64>,
    /// Query to share
    pub query: QueryEntry,
}

#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
/// Response with created query link
pub struct QueryLinkCreateResponse {
    /// Link to access the query
    pub link: String,
}
