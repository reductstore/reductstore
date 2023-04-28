// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use rand::Rng;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::SystemTime;

use log::{debug, warn};
use prost::bytes::Bytes;
use prost::Message;
use prost_wkt_types::Timestamp;

use crate::auth::proto::token::Permissions;
use crate::auth::proto::{Token, TokenCreateResponse, TokenRepo};
use crate::core::status::HTTPError;

const TOKEN_REPO_FILE_NAME: &str = ".auth";
const INIT_TOKEN_NAME: &str = "init-token";

/// The TokenRepository trait is used to store and retrieve tokens.
pub struct TokenRepository {
    config_path: PathBuf,
    init_token: Option<Token>,
    // TODO: Make it non-optional after C++ is removed
    repo: HashMap<String, Token>,
}

pub fn parse_bearer_token(authorization_header: &str) -> Result<String, HTTPError> {
    if !authorization_header.starts_with("Bearer ") {
        return Err(HTTPError::unauthorized("No bearer token in request header"));
    }

    let token = authorization_header[7..].to_string();
    Ok(token)
}

impl TokenRepository {
    /// Load the token repository from the file system
    ///
    /// # Arguments
    ///
    /// * `data_path` - The path to the data directory
    /// * `api_token` - The API token with full access to the repository. If it is empty, no authentication is required.
    ///
    /// # Returns
    ///
    /// The repository
    pub fn new(data_path: PathBuf, api_token: &str) -> TokenRepository {
        let config_path = data_path.join(TOKEN_REPO_FILE_NAME);
        let repo = HashMap::new();

        let init_token = if !api_token.is_empty() {
            Some(Token {
                name: INIT_TOKEN_NAME.to_string(),
                value: api_token.to_string(),
                created_at: Timestamp::try_from(SystemTime::now()).ok(),
                permissions: Some(Permissions {
                    full_access: true,
                    read: vec![],
                    write: vec![],
                }),
            })
        } else {
            // TODO: After C++ is removed, this should use traits and an empty implementation
            warn!("API token is not set, no authentication is required");
            None
        };

        // Load the token repository from the file system
        let mut token_repository = TokenRepository {
            config_path,
            init_token,
            repo,
        };

        match std::fs::read(&token_repository.config_path) {
            Ok(data) => {
                debug!(
                    "Loading token repository from {}",
                    token_repository.config_path.as_path().display()
                );
                let toke_repository = TokenRepo::decode(&mut Bytes::from(data))
                    .expect("Could not decode token repository");
                for token in toke_repository.tokens {
                    token_repository.repo.insert(token.name.clone(), token);
                }
            }
            Err(_) => {
                debug!(
                    "Creating a new token repository {}",
                    token_repository.config_path.as_path().display()
                );
                token_repository
                    .save_repo()
                    .expect("Failed to create a new token repository");
            }
        };

        token_repository
    }

    /// Create a new token
    ///
    /// # Arguments
    ///
    /// `name` - The name of the token
    /// `permissions` - The permissions of the token
    ///
    /// # Returns
    ///
    /// token value and creation time
    pub fn create_token(
        &mut self,
        name: &str,
        permissions: Permissions,
    ) -> Result<TokenCreateResponse, HTTPError> {
        if self.init_token.is_none() {
            return Err(HTTPError::bad_request("Authentication is disabled"));
        }

        // Check if the token isn't empty
        if name.is_empty() {
            return Err(HTTPError::unprocessable_entity("Token name can't be empty"));
        }

        // Check if the token already exists
        if self.repo.contains_key(name) {
            return Err(HTTPError::conflict(
                format!("Token '{}' already exists", name).as_str(),
            ));
        }

        let created_at = Timestamp::try_from(SystemTime::now()).ok();

        // Create a random hex string
        let mut rng = rand::thread_rng();
        let value: String = (0..32)
            .map(|_| format!("{:x}", rng.gen_range(0..16)))
            .collect();
        let value = format!("{}-{}", name, value);
        let token = Token {
            name: name.to_string(),
            value: value.clone(),
            created_at: created_at.clone(),
            permissions: Some(permissions),
        };

        self.repo.insert(name.to_string(), token);
        self.save_repo()?;

        Ok(TokenCreateResponse { value, created_at })
    }

    /// Update a token
    ///
    /// # Arguments
    ///
    /// `name` - The name of the token
    /// `permissions` - The permissions of the token
    pub fn update_token(&mut self, name: &str, permissions: Permissions) -> Result<(), HTTPError> {
        if self.init_token.is_none() {
            return Err(HTTPError::bad_request("Authentication is disabled"));
        }

        debug!("Updating token '{}'", name);

        match self.repo.get(name) {
            Some(token) => {
                let mut updated_token = token.clone();
                updated_token.permissions = Some(permissions);
                self.repo.insert(name.to_string(), updated_token);
                self.save_repo()?;
                Ok(())
            }

            None => Err(HTTPError::not_found(
                format!("Token '{}' doesn't exist", name).as_str(),
            )),
        }
    }

    /// Find a token by name
    ///
    /// # Arguments
    ///
    /// `name` - The name of the token
    ///
    /// # Returns
    /// The token without value
    pub fn find_by_name(&self, name: &str) -> Result<Token, HTTPError> {
        if self.init_token.is_none() {
            return Err(HTTPError::bad_request("Authentication is disabled"));
        }

        match self.repo.get(name) {
            Some(token) => Ok(Token {
                name: token.name.clone(),
                value: "".to_string(),
                created_at: token.created_at.clone(),
                permissions: token.permissions.clone(),
            }),
            None => Err(HTTPError::not_found(
                format!("Token '{}' doesn't exist", name).as_str(),
            )),
        }
    }

    /// Get token list
    ///
    /// # Returns
    /// The token list, it the authentication is disabled, it returns an empty list
    pub fn get_token_list(&self) -> Result<Vec<Token>, HTTPError> {
        if self.init_token.is_none() {
            return Ok(vec![]);
        }

        let mut sorted: Vec<_> = self.repo.iter().collect();
        sorted.sort_by_key(|item| item.0);
        Ok(sorted
            .iter()
            .map(|item| Token {
                name: item.1.name.clone(),
                value: "".to_string(),
                created_at: item.1.created_at.clone(),
                permissions: item.1.permissions.clone(),
            })
            .collect())
    }

    /// Validate a token
    ///
    /// # Arguments
    /// `header` - The authorization header with bearer token
    ///
    /// # Returns
    ///
    /// Token with given value
    pub fn validate_token(&self, header: Option<&str>) -> Result<Token, HTTPError> {
        if self.init_token.is_none() {
            // Return placeholder
            return Ok(Token {
                name: "AUTHENTICATION-DISABLED".to_string(),
                value: "".to_string(),
                created_at: None,
                permissions: Some(Permissions {
                    full_access: true,
                    read: vec![],
                    write: vec![],
                }),
            });
        }

        let value = parse_bearer_token(header.unwrap_or(""))?;

        // Check init token first
        if let Some(init_token) = &self.init_token {
            if init_token.value == value {
                return Ok(Token {
                    name: init_token.name.clone(),
                    value: "".to_string(),
                    created_at: init_token.created_at.clone(),
                    permissions: init_token.permissions.clone(),
                });
            }
        }

        match self.repo.values().find(|token| token.value == value) {
            Some(token) => {
                // for security reasons, we don't return the value
                Ok(Token {
                    name: token.name.clone(),
                    value: "".to_string(),
                    created_at: token.created_at.clone(),
                    permissions: token.permissions.clone(),
                })
            }
            None => Err(HTTPError::unauthorized("Invalid token")),
        }
    }

    /// Remove a token
    ///
    /// # Arguments
    /// `name` - The name of the token
    ///
    /// # Returns
    ///
    /// `Ok(())` if the token was removed successfully
    pub fn remove_token(&mut self, name: &str) -> Result<(), HTTPError> {
        if self.init_token.is_none() {
            return Ok(());
        }

        if self.repo.remove(name).is_none() {
            Err(HTTPError::not_found(
                format!("Token '{}' doesn't exist", name).as_str(),
            ))
        } else {
            self.save_repo()
        }
    }

    /// Remove a bucket from all tokens and save the repository
    /// to the file system
    ///
    /// # Arguments
    /// `bucket` - The name of the bucket
    ///
    /// # Returns
    /// `Ok(())` if the bucket was removed successfully
    pub fn remove_bucket_from_tokens(&mut self, bucket: &str) -> Result<(), HTTPError> {
        for token in self.repo.values_mut() {
            if let Some(permissions) = &mut token.permissions {
                permissions.read.retain(|b| b != bucket);
                permissions.write.retain(|b| b != bucket);
            }
        }

        self.save_repo()
    }

    /// Save the token repository to the file system
    fn save_repo(&mut self) -> Result<(), HTTPError> {
        let repo = TokenRepo {
            tokens: self.repo.values().cloned().collect(),
        };
        let mut buf = Vec::new();
        repo.encode(&mut buf)
            .map_err(|_| HTTPError::internal_server_error("Could not encode token repository"))?;
        std::fs::write(&self.config_path, buf).map_err(|err| {
            HTTPError::internal_server_error(
                format!(
                    "Could not write token repository to {}: {}",
                    self.config_path.as_path().display(),
                    err
                )
                .as_str(),
            )
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    pub fn test_init_token() {
        let repo = setup();

        let token = repo.validate_token("Bearer test").unwrap();
        assert_eq!(token.name, "init-token");
        assert_eq!(token.value, "");
    }

    //------------
    // create_token tests
    //------------
    #[test]
    pub fn test_create_empty_token() {
        let mut repo = setup();
        let token = repo.create_token(
            "",
            Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            },
        );

        assert_eq!(
            token,
            Err(HTTPError::unprocessable_entity("Token name can't be empty"))
        );
    }

    #[test]
    pub fn test_create_existing_token() {
        let mut repo = setup();
        repo.create_token(
            "test",
            Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            },
        )
        .unwrap();

        let token = repo.create_token(
            "test",
            Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            },
        );

        assert_eq!(
            token,
            Err(HTTPError::conflict("Token 'test' already exists"))
        );
    }

    #[test]
    pub fn test_create_token() {
        let mut repo = setup();
        let token = repo
            .create_token(
                "test",
                Permissions {
                    full_access: true,
                    read: vec![],
                    write: vec![],
                },
            )
            .unwrap();

        assert_eq!(token.value.len(), 37);
        assert_eq!(token.value, "test-".to_string() + &token.value[5..]);
        assert!(token.created_at.unwrap().seconds > 0);
    }

    #[test]
    pub fn test_create_token_persistent() {
        let path = tempdir().unwrap().into_path();
        let mut repo = TokenRepository::new(path.clone(), Some("test".to_string()));
        repo.create_token(
            "test",
            Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            },
        )
        .unwrap();

        let repo = TokenRepository::new(path.clone(), Some("test".to_string()));
        assert!(repo.repo.contains_key("test"));
    }

    #[test]
    pub fn test_create_token_no_init_token() {
        let mut repo = TokenRepository::new(tempdir().unwrap().into_path(), None);
        let token = repo.create_token(
            "test",
            Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            },
        );

        assert_eq!(
            token,
            Err(HTTPError::bad_request("Authentication is disabled"))
        );
    }

    //------------
    // update_token tests
    //------------
    #[test]
    pub fn test_update_token_ok() {
        let mut repo = setup();
        repo.create_token(
            "test",
            Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            },
        )
        .unwrap();

        let token = repo
            .update_token(
                "test",
                Permissions {
                    full_access: false,
                    read: vec!["test".to_string()],
                    write: vec![],
                },
            )
            .unwrap();

        assert_eq!(token, ());

        let token = repo.find_by_name("test").unwrap();

        assert_eq!(token.name, "test");

        let permissions = token.permissions.unwrap();
        assert_eq!(permissions.full_access, false);
        assert_eq!(permissions.read, vec!["test".to_string()]);
    }

    #[test]
    pub fn test_update_token_not_found() {
        let mut repo = setup();
        let token = repo.update_token(
            "test",
            Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            },
        );

        assert_eq!(
            token,
            Err(HTTPError::not_found("Token 'test' doesn't exist"))
        );
    }

    #[test]
    pub fn test_update_token_persistent() {
        let path = tempdir().unwrap().into_path();
        let mut repo = TokenRepository::new(path.clone(), Some("test".to_string()));
        repo.create_token(
            "test",
            Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            },
        )
        .unwrap();

        repo.update_token(
            "test",
            Permissions {
                full_access: false,
                read: vec!["test".to_string()],
                write: vec![],
            },
        )
        .unwrap();

        let repo = TokenRepository::new(path.clone(), Some("test".to_string()));
        let token = repo.find_by_name("test").unwrap();

        assert_eq!(token.name, "test");

        let permissions = token.permissions.unwrap();
        assert_eq!(permissions.full_access, false);
        assert_eq!(permissions.read, vec!["test".to_string()]);
    }

    #[test]
    pub fn test_update_token_no_init_token() {
        let mut repo = TokenRepository::new(tempdir().unwrap().into_path(), None);
        let token = repo.update_token(
            "test",
            Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            },
        );

        assert_eq!(
            token,
            Err(HTTPError::bad_request("Authentication is disabled"))
        );
    }

    //----------------
    // find_by_name tests
    //----------------
    #[test]
    pub fn test_find_by_name() {
        let mut repo = setup();
        repo.create_token(
            "test",
            Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            },
        )
        .unwrap();

        let token = repo.find_by_name("test").unwrap();

        assert_eq!(token.name, "test");
        assert_eq!(token.value, "");
    }

    #[test]
    pub fn test_find_by_name_not_found() {
        let repo = setup();
        let token = repo.find_by_name("test");

        assert_eq!(
            token,
            Err(HTTPError::not_found("Token 'test' doesn't exist"))
        );
    }

    #[test]
    pub fn test_find_by_name_no_init_token() {
        let repo = TokenRepository::new(tempdir().unwrap().into_path(), None);
        let token = repo.find_by_name("test");

        assert_eq!(
            token,
            Err(HTTPError::bad_request("Authentication is disabled"))
        );
    }

    //------------
    // get_token_list tests
    //------------
    #[test]
    pub fn test_get_token_list() {
        let mut repo = setup();
        repo.create_token(
            "test",
            Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            },
        )
        .unwrap();

        let token_list = repo.get_token_list().unwrap();

        assert_eq!(token_list.len(), 1);
        assert_eq!(token_list[0].name, "test");
        assert_eq!(token_list[0].value, "");
    }

    #[test]
    pub fn test_get_token_list_no_init_token() {
        let repo = TokenRepository::new(tempdir().unwrap().into_path(), None);
        let token_list = repo.get_token_list().unwrap();

        assert_eq!(token_list, vec![]);
    }

    //------------
    // validate_token tests
    //------------
    #[test]
    pub fn test_validate_token() {
        let mut repo = setup();
        let value = repo
            .create_token(
                "test",
                Permissions {
                    full_access: true,
                    read: vec!["bucket-1".to_string()],
                    write: vec!["bucket-2".to_string()],
                },
            )
            .unwrap()
            .value;

        let token = repo
            .validate_token(format!("Bearer {}", value).as_str())
            .unwrap();

        assert_eq!(
            token,
            Token {
                name: "test".to_string(),
                created_at: token.created_at.clone(),
                value: "".to_string(),
                permissions: Some(Permissions {
                    full_access: true,
                    read: vec!["bucket-1".to_string()],
                    write: vec!["bucket-2".to_string()],
                }),
            }
        );
    }

    #[test]
    pub fn test_validate_token_not_found() {
        let repo = setup();
        let token = repo.validate_token("Bearer invalid-value");

        assert_eq!(token, Err(HTTPError::unauthorized("Invalid token")));
    }

    #[test]
    pub fn test_validate_token_no_init_token() {
        let repo = TokenRepository::new(tempdir().unwrap().into_path(), None);
        let placeholder = repo.validate_token("invalid-value").unwrap();

        assert_eq!(placeholder.name, "AUTHENTICATION-DISABLED");
        assert_eq!(placeholder.value, "");
        assert_eq!(placeholder.permissions.unwrap().full_access, true);
    }

    //------------
    // remove_token tests
    //------------
    #[test]
    pub fn test_remove_token() {
        let mut repo = setup();
        repo.create_token(
            "test",
            Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            },
        )
        .unwrap();

        let token = repo.remove_token("test").unwrap();

        assert_eq!(token, ());
    }

    #[test]
    pub fn test_remove_token_not_found() {
        let mut repo = setup();
        let token = repo.remove_token("test");

        assert_eq!(
            token,
            Err(HTTPError::not_found("Token 'test' doesn't exist"))
        );
    }

    #[test]
    pub fn test_remove_token_persistent() {
        let path = tempdir().unwrap().into_path();
        let mut repo = TokenRepository::new(path.clone(), Some("test".to_string()));
        repo.create_token(
            "test",
            Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            },
        )
        .unwrap();

        repo.remove_token("test").unwrap();

        let repo = TokenRepository::new(path.clone(), Some("test".to_string()));
        let token = repo.find_by_name("test");

        assert_eq!(
            token,
            Err(HTTPError::not_found("Token 'test' doesn't exist"))
        );
    }

    #[test]
    pub fn test_remove_token_no_init_token() {
        let mut repo = TokenRepository::new(tempdir().unwrap().into_path(), None);
        let token = repo.remove_token("test");

        assert_eq!(token, Ok(()));
    }

    fn setup() -> TokenRepository {
        TokenRepository::new(tempdir().unwrap().into_path(), Some("test".to_string()))
    }
}
