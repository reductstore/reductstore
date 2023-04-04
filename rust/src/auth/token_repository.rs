// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::path::PathBuf;
use std::collections::HashMap;
use std::time::SystemTime;
use rand::Rng;

use bytes::Bytes;
use prost::{bytes, Message};

use crate::core::status::HTTPError;

mod proto {
    include!(concat!(env!("OUT_DIR"), "/reduct.proto.api.rs"));
}

const TOKEN_REPO_FILE_NAME: &str = ".auth";
const INIT_TOKEN_NAME: &str = "init-token";

/// The TokenRepository trait is used to store and retrieve tokens.
pub struct TokenRepository {
    config_path: PathBuf,
    init_token: Option<proto::Token>,
    repo: HashMap<String, proto::Token>,
}

impl TokenRepository {
    /// Load the token repository from the file system
    ///
    /// # Arguments
    ///
    /// * `data_path` - The path to the data directory
    /// * `api_token` - The API token with full access to the repository. If it is none, no authentication is required.
    ///
    /// # Returns
    ///
    /// The repository
    pub fn new(data_path: PathBuf, api_token: Option<String>) -> TokenRepository {
        let config_path = data_path.join(TOKEN_REPO_FILE_NAME);
        let repo = HashMap::new();

        let init_token = match api_token {
            Some(value) => {
                Some(proto::Token {
                    name: INIT_TOKEN_NAME.to_string(),
                    value,
                    created_at: ::prost_types::Timestamp::try_from(SystemTime::now()).ok(),
                    permissions: Some(proto::token::Permissions {
                        full_access: true,
                        read: vec![],
                        write: vec![],
                    }),
                })
            }
            None => {
                // No API token, no authentication
                // TODO: After C++ is removed, this should use traits and an empty implementation
                None
            }
        };

        // Load the token repository from the file system
        let mut token_repository = TokenRepository {
            config_path,
            init_token,
            repo,
        };

        match std::fs::read(&token_repository.config_path) {
            Ok(data) => {
                let toke_repository = proto::TokenRepo::decode(&mut Bytes::from(data))
                    .expect("Could not decode token repository");
                for token in toke_repository.tokens {
                    token_repository.repo.insert(token.name.clone(), token);
                }
            }
            Err(_) => {
                // Create a new token repository
                token_repository.save_repo().expect("Failed to create a new token repository");
            }
        };

        token_repository
    }

    pub fn create_token(&mut self, name: &str, permissions: proto::token::Permissions) -> Result<proto::TokenCreateResponse, HTTPError> {
        // Check if the token isn't empty
        if name.is_empty() {
            return Err(HTTPError::unprocessable_entity("Token name can't be empty"));
        }

        // Check if the token already exists
        if self.repo.contains_key(name) {
            return Err(HTTPError::conflict(format!("Token '{}' already exists", name).as_str()));
        }

        let created_at = ::prost_types::Timestamp::try_from(SystemTime::now()).ok();

        // Create a random hex string
        let mut rng = rand::thread_rng();
        let value: String = (0..32)
            .map(|_| format!("{:x}", rng.gen_range(0..16)))
            .collect();
        let value = format!("{}-{}", name, value);
        let token = proto::Token {
            name: name.to_string(),
            value: value.clone(),
            created_at: created_at.clone(),
            permissions: Some(permissions),
        };

        self.repo.insert(name.to_string(), token);
        self.save_repo()?;

        Ok(proto::TokenCreateResponse {
            value,
            created_at,
        })
    }

    /// Save the token repository to the file system
    fn save_repo(&mut self) -> Result<(), HTTPError> {
        let repo = proto::TokenRepo {
            tokens: self.repo.values().cloned().collect(),
        };
        let mut buf = Vec::new();
        repo.encode(&mut buf)
            .map_err(|_| HTTPError::internal_server_error("Could not encode token repository"))?;
        std::fs::write(&self.config_path, buf)
            .map_err(|err| HTTPError::internal_server_error(
                format!("Could not write token repository to {}: {}", self.config_path.as_path().display(), err).as_str()))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::{tempdir};
    use proto::token::Permissions;
    use super::*;

    #[test]
    pub fn test_create_empty_token() {
        let mut repo = setup();
        let token = repo.create_token("", Permissions {
            full_access: true,
            read: vec![],
            write: vec![],
        });

        assert_eq!(token, Err(HTTPError::unprocessable_entity("Token name can't be empty")));
    }

    #[test]
    pub fn test_create_existing_token() {
        let mut repo = setup();
        repo.create_token("test", Permissions {
            full_access: true,
            read: vec![],
            write: vec![],
        }).unwrap();

        let token = repo.create_token("test", Permissions {
            full_access: true,
            read: vec![],
            write: vec![],
        });

        assert_eq!(token, Err(HTTPError::conflict("Token 'test' already exists")));
    }

    #[test]
    pub fn test_create_token() {
        let mut repo = setup();
        let token = repo.create_token("test", Permissions {
            full_access: true,
            read: vec![],
            write: vec![],
        }).unwrap();

        assert_eq!(token.value.len(), 37);
        assert_eq!(token.value, "test-".to_string() + &token.value[5..]);
        assert!(token.created_at.unwrap().seconds > 0);
    }

    #[test]
    pub fn test_create_token_persistent() {
        let path = tempdir().unwrap().into_path();
        let mut repo = TokenRepository::new(path.clone(), Some("test".to_string()));
        repo.create_token("test", Permissions {
            full_access: true,
            read: vec![],
            write: vec![],
        }).unwrap();

        let repo = TokenRepository::new(path.clone(), Some("test".to_string()));
        assert!(repo.repo.contains_key("test"));
    }

    fn setup() -> TokenRepository {
        TokenRepository::new(tempdir().unwrap().into_path(), Some("test".to_string()))
    }
}