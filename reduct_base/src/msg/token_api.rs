// Copyright 2023 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Permissions for a token
#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
pub struct Permissions {
    /// Full access to all buckets and tokens
    #[serde(default)]
    pub full_access: bool,
    /// Read access to certain buckets
    #[serde(default)]
    pub read: Vec<String>,
    /// Write access to certain buckets
    #[serde(default)]
    pub write: Vec<String>,
}

/// Token
#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
pub struct Token {
    /// Unique token name
    pub name: String,
    /// Unique token value
    pub value: String,
    /// Creation time
    pub created_at: DateTime<Utc>,
    /// Permissions
    pub permissions: Option<Permissions>,
    /// Provisioned
    pub is_provisioned: bool,
}

/// Response for created token
#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
pub struct TokenCreateResponse {
    pub value: String,
    pub created_at: DateTime<Utc>,
}

/// Token repository
#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
pub struct TokenList {
    pub tokens: Vec<Token>,
}
