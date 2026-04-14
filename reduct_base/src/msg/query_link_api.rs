// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::msg::entry_api::QueryEntry;
use chrono::serde::ts_seconds::deserialize as as_ts;
use chrono::serde::ts_seconds::serialize as to_ts;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
/// Request to create a query link for sharing
pub struct QueryLinkCreateRequest {
    /// Bucket name
    pub bucket: String,
    /// Entry name (since v1.18 used for backward compatibility)
    pub entry: String,
    /// Record index
    pub index: Option<u64>,
    /// Exact record entry name for stable preview resolution (optional)
    pub record_entry: Option<String>,
    /// Exact record timestamp for stable preview resolution (optional)
    pub record_timestamp: Option<u64>,
    /// Query to share
    pub query: QueryEntry,
    /// Expiration time
    #[serde(deserialize_with = "as_ts", serialize_with = "to_ts")]
    pub expire_at: DateTime<Utc>,
    ///  Optimal base URL for the link (optional)
    pub base_url: Option<String>,
}

#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
/// Response with created query link
pub struct QueryLinkCreateResponse {
    /// Link to access the query
    pub link: String,
}
