// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::path::PathBuf;

include!(concat!(env!("OUT_DIR"), "/reduct.proto.api.rs"));

/// The TokenRepository trait is used to store and retrieve tokens.
struct TokenRepository {
    data_path: PathBuf,
    api_token: Option<String>,
}

impl TokenRepository {
    /// Create a new repository
    ///
    /// # Arguments
    ///
    /// * `data_path` - The path to the data directory
    /// * `api_token` - The API token
    ///
    /// # Returns
    ///
    /// The repository
    pub fn new(data_path: PathBuf, api_token: Option<String>) -> TokenRepository {
        TokenRepository {
            data_path,
            api_token,
        }
    }

    pub fn create_token(&self) -> Token {
        Token {
            name: "test".to_string(),
            value: "test".to_string(),
            created_at: Some(::prost_types::Timestamp {
                seconds: 0,
                nanos: 0,
            }),
            permissions: Some(token::Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            })
        }
    }
}

