// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use rand::Rng;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::SystemTime;

use log::debug;
use prost::bytes::Bytes;
use prost::Message;
use prost_wkt_types::Timestamp;

use crate::auth::proto::token::Permissions;
use crate::auth::proto::{Token, TokenCreateResponse, TokenRepo};
use crate::core::status::HttpError;

const TOKEN_REPO_FILE_NAME: &str = ".auth";
const INIT_TOKEN_NAME: &str = "init-token";

pub trait ManageTokens {
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
    fn create_token(
        &mut self,
        name: &str,
        permissions: Permissions,
    ) -> Result<TokenCreateResponse, HttpError>;

    /// Update a token
    ///
    /// # Arguments
    ///
    /// `name` - The name of the token
    /// `permissions` - The permissions of the token
    fn update_token(&mut self, name: &str, permissions: Permissions) -> Result<(), HttpError>;

    /// Find a token by name
    ///
    /// # Arguments
    ///
    /// `name` - The name of the token
    ///
    /// # Returns
    /// The token without value
    fn find_by_name(&self, name: &str) -> Result<Token, HttpError>;

    /// Get token list
    ///
    /// # Returns
    /// The token list, it the authentication is disabled, it returns an empty list
    fn get_token_list(&self) -> Result<Vec<Token>, HttpError>;

    /// Validate a token
    ///
    /// # Arguments
    /// `header` - The authorization header with bearer token
    ///
    /// # Returns
    ///
    /// Token with given value
    fn validate_token(&self, header: Option<&str>) -> Result<Token, HttpError>;

    /// Remove a token
    ///
    /// # Arguments
    /// `name` - The name of the token
    ///
    /// # Returns
    ///
    /// `Ok(())` if the token was removed successfully
    fn remove_token(&mut self, name: &str) -> Result<(), HttpError>;
}

/// The TokenRepository trait is used to store and retrieve tokens.
struct TokenRepository {
    config_path: PathBuf,
    repo: HashMap<String, Token>,
}

pub fn parse_bearer_token(authorization_header: &str) -> Result<String, HttpError> {
    if !authorization_header.starts_with("Bearer ") {
        return Err(HttpError::unauthorized("No bearer token in request header"));
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

        if api_token.is_empty() {
            panic!("API must be set");
        }

        // Load the token repository from the file system
        let mut token_repository = TokenRepository { config_path, repo };

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

        let init_token = Token {
            name: INIT_TOKEN_NAME.to_string(),
            value: api_token.to_string(),
            created_at: Timestamp::try_from(SystemTime::now()).ok(),
            permissions: Some(Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            }),
        };

        token_repository
            .repo
            .insert(init_token.name.clone(), init_token);
        token_repository
    }

    /// Remove a bucket from all tokens and save the repository
    /// to the file system
    ///
    /// # Arguments
    /// `bucket` - The name of the bucket
    ///
    /// # Returns
    /// `Ok(())` if the bucket was removed successfully
    pub fn remove_bucket_from_tokens(&mut self, bucket: &str) -> Result<(), HttpError> {
        for token in self.repo.values_mut() {
            if let Some(permissions) = &mut token.permissions {
                permissions.read.retain(|b| b != bucket);
                permissions.write.retain(|b| b != bucket);
            }
        }

        self.save_repo()
    }

    /// Save the token repository to the file system
    fn save_repo(&mut self) -> Result<(), HttpError> {
        let repo = TokenRepo {
            tokens: self.repo.values().cloned().collect(),
        };
        let mut buf = Vec::new();
        repo.encode(&mut buf)
            .map_err(|_| HttpError::internal_server_error("Could not encode token repository"))?;
        std::fs::write(&self.config_path, buf).map_err(|err| {
            HttpError::internal_server_error(
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

impl ManageTokens for TokenRepository {
    fn create_token(
        &mut self,
        name: &str,
        permissions: Permissions,
    ) -> Result<TokenCreateResponse, HttpError> {
        // Check if the token isn't empty
        if name.is_empty() {
            return Err(HttpError::unprocessable_entity("Token name can't be empty"));
        }

        // Check if the token already exists
        if self.repo.contains_key(name) {
            return Err(HttpError::conflict(
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

    fn update_token(&mut self, name: &str, permissions: Permissions) -> Result<(), HttpError> {
        debug!("Updating token '{}'", name);

        match self.repo.get(name) {
            Some(token) => {
                let mut updated_token = token.clone();
                updated_token.permissions = Some(permissions);
                self.repo.insert(name.to_string(), updated_token);
                self.save_repo()?;
                Ok(())
            }

            None => Err(HttpError::not_found(
                format!("Token '{}' doesn't exist", name).as_str(),
            )),
        }
    }

    fn find_by_name(&self, name: &str) -> Result<Token, HttpError> {
        match self.repo.get(name) {
            Some(token) => Ok(Token {
                name: token.name.clone(),
                value: "".to_string(),
                created_at: token.created_at.clone(),
                permissions: token.permissions.clone(),
            }),
            None => Err(HttpError::not_found(
                format!("Token '{}' doesn't exist", name).as_str(),
            )),
        }
    }

    fn get_token_list(&self) -> Result<Vec<Token>, HttpError> {
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

    fn validate_token(&self, header: Option<&str>) -> Result<Token, HttpError> {
        let value = parse_bearer_token(header.unwrap_or(""))?;

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
            None => Err(HttpError::unauthorized("Invalid token")),
        }
    }

    fn remove_token(&mut self, name: &str) -> Result<(), HttpError> {
        if name == INIT_TOKEN_NAME {
            return Err(HttpError::bad_request("Cannot remove init token"));
        }

        if self.repo.remove(name).is_none() {
            Err(HttpError::not_found(
                format!("Token '{}' doesn't exist", name).as_str(),
            ))
        } else {
            self.save_repo()
        }
    }
}

/// A repository that doesn't require authentication
struct NoAuthRepository {}

impl NoAuthRepository {
    fn new() -> Self {
        Self {}
    }
}

impl ManageTokens for NoAuthRepository {
    fn create_token(
        &mut self,
        _name: &str,
        _permissions: Permissions,
    ) -> Result<TokenCreateResponse, HttpError> {
        Err(HttpError::bad_request("Authentication is disabled"))
    }

    fn update_token(&mut self, _name: &str, _permissions: Permissions) -> Result<(), HttpError> {
        Err(HttpError::bad_request("Authentication is disabled"))
    }

    fn find_by_name(&self, _name: &str) -> Result<Token, HttpError> {
        Err(HttpError::bad_request("Authentication is disabled"))
    }

    fn get_token_list(&self) -> Result<Vec<Token>, HttpError> {
        Ok(vec![])
    }

    fn validate_token(&self, _header: Option<&str>) -> Result<Token, HttpError> {
        Ok(Token {
            name: "AUTHENTICATION-DISABLED".to_string(),
            value: "".to_string(),
            created_at: None,
            permissions: Some(Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            }),
        })
    }

    fn remove_token(&mut self, _name: &str) -> Result<(), HttpError> {
        Ok(())
    }
}

/// Creates a token repository
///
/// If `init_token` is empty, the repository will be stubbed and authentication will be disabled.
pub fn create_token_repository(
    path: PathBuf,
    init_token: &str,
) -> Box<dyn ManageTokens + Send + Sync> {
    if init_token.is_empty() {
        Box::new(NoAuthRepository::new())
    } else {
        Box::new(TokenRepository::new(path, init_token))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    pub fn test_init_token() {
        let repo = setup("init-token");

        let token = repo.validate_token(Some("Bearer init-token")).unwrap();
        assert_eq!(token.name, "init-token");
        assert_eq!(token.value, "");

        let token_list = repo.get_token_list().unwrap();
        assert_eq!(token_list.len(), 1);
        assert_eq!(token_list[0].name, "init-token");
    }

    //------------
    // create_token tests
    //------------
    #[test]
    pub fn test_create_empty_token() {
        let mut repo = setup("init-token");
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
            Err(HttpError::unprocessable_entity("Token name can't be empty"))
        );
    }

    #[test]
    pub fn test_create_existing_token() {
        let mut repo = setup("init-token");
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
            Err(HttpError::conflict("Token 'test' already exists"))
        );
    }

    #[test]
    pub fn test_create_token() {
        let mut repo = setup("init-token");
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
        let mut repo = TokenRepository::new(path.clone(), "test");
        repo.create_token(
            "test",
            Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            },
        )
        .unwrap();

        let repo = TokenRepository::new(path.clone(), "test");
        assert!(repo.repo.contains_key("test"));
    }

    #[test]
    pub fn test_create_token_no_init_token() {
        let mut repo = setup("");
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
            Err(HttpError::bad_request("Authentication is disabled"))
        );
    }

    //------------
    // update_token tests
    //------------
    #[test]
    pub fn test_update_token_ok() {
        let mut repo = setup("init-token");
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
        let mut repo = setup("init-token");
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
            Err(HttpError::not_found("Token 'test' doesn't exist"))
        );
    }

    #[test]
    pub fn test_update_token_persistent() {
        let path = tempdir().unwrap().into_path();
        let mut repo = TokenRepository::new(path.clone(), "test");
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

        let repo = TokenRepository::new(path.clone(), "test");
        let token = repo.find_by_name("test").unwrap();

        assert_eq!(token.name, "test");

        let permissions = token.permissions.unwrap();
        assert_eq!(permissions.full_access, false);
        assert_eq!(permissions.read, vec!["test".to_string()]);
    }

    #[test]
    pub fn test_update_token_no_init_token() {
        let mut repo = setup("");
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
            Err(HttpError::bad_request("Authentication is disabled"))
        );
    }

    //----------------
    // find_by_name tests
    //----------------
    #[test]
    pub fn test_find_by_name() {
        let mut repo = setup("init-token");
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
        let repo = setup("init-token");
        let token = repo.find_by_name("test");

        assert_eq!(
            token,
            Err(HttpError::not_found("Token 'test' doesn't exist"))
        );
    }

    #[test]
    pub fn test_find_by_name_no_init_token() {
        let repo = setup("");
        let token = repo.find_by_name("test");

        assert_eq!(
            token,
            Err(HttpError::bad_request("Authentication is disabled"))
        );
    }

    //------------
    // get_token_list tests
    //------------
    #[test]
    pub fn test_get_token_list() {
        let mut repo = setup("init-token");
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

        assert_eq!(token_list.len(), 2);
        assert_eq!(token_list[1].name, "test");
        assert_eq!(token_list[1].value, "");
    }

    #[test]
    pub fn test_get_token_list_no_init_token() {
        let repo = setup("");
        let token_list = repo.get_token_list().unwrap();

        assert_eq!(token_list, vec![]);
    }

    //------------
    // validate_token tests
    //------------
    #[test]
    pub fn test_validate_token() {
        let mut repo = setup("init-token");
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
            .validate_token(Some(&format!("Bearer {}", value)))
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
        let repo = setup("init-token");
        let token = repo.validate_token(Some("Bearer invalid-value"));

        assert_eq!(token, Err(HttpError::unauthorized("Invalid token")));
    }

    #[test]
    pub fn test_validate_token_no_init_token() {
        let repo = setup("");
        let placeholder = repo.validate_token(Some("invalid-value")).unwrap();

        assert_eq!(placeholder.name, "AUTHENTICATION-DISABLED");
        assert_eq!(placeholder.value, "");
        assert_eq!(placeholder.permissions.unwrap().full_access, true);
    }

    //------------
    // remove_token tests
    //------------
    #[test]
    pub fn test_remove_token() {
        let mut repo = setup("init-token");
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
    pub fn test_remove_init_token() {
        let mut repo = setup("init-token");
        let token = repo.remove_token("init-token");

        assert_eq!(
            token,
            Err(HttpError::bad_request("Cannot remove init token"))
        );
    }

    #[test]
    pub fn test_remove_token_not_found() {
        let mut repo = setup("init-token");
        let token = repo.remove_token("test");

        assert_eq!(
            token,
            Err(HttpError::not_found("Token 'test' doesn't exist"))
        );
    }

    #[test]
    pub fn test_remove_token_persistent() {
        let path = tempdir().unwrap().into_path();
        let mut repo = TokenRepository::new(path.clone(), "test");
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

        let repo = TokenRepository::new(path.clone(), "test");
        let token = repo.find_by_name("test");

        assert_eq!(
            token,
            Err(HttpError::not_found("Token 'test' doesn't exist"))
        );
    }

    #[test]
    pub fn test_remove_token_no_init_token() {
        let mut repo = setup("");
        let token = repo.remove_token("test");

        assert_eq!(token, Ok(()));
    }

    fn setup(init_token: &str) -> Box<dyn ManageTokens> {
        create_token_repository(tempdir().unwrap().into_path(), init_token)
    }
}
