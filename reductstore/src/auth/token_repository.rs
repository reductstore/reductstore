// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use chrono::{DateTime, Utc};
use rand::Rng;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use log::{debug, warn};
use prost::bytes::Bytes;
use prost::Message;
use prost_wkt_types::Timestamp;
use tempfile::tempdir;

use crate::auth::proto::token::Permissions as ProtoPermissions;
use crate::auth::proto::{Token as ProtoToken, TokenRepo as ProtoTokenRepo, TokenRepo};
use reduct_base::error::ReductError;

use reduct_base::msg::token_api::{Permissions, Token, TokenCreateResponse};

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
    fn generate_token(
        &mut self,
        name: &str,
        permissions: Permissions,
    ) -> Result<TokenCreateResponse, ReductError>;

    /// Update a token
    ///
    /// # Arguments
    ///
    /// `name` - The name of the token
    /// `permissions` - The permissions of the token
    fn update_token(&mut self, name: &str, permissions: Permissions) -> Result<(), ReductError>;

    /// Get a token by name
    ///
    /// # Arguments
    ///
    /// `name` - The name of the token
    ///
    /// # Returns
    /// The token without value
    fn get_token(&self, name: &str) -> Result<&Token, ReductError>;

    /// Get a token by name (mutable)
    ///
    /// # Arguments
    ///
    /// `name` - The name of the token
    ///
    /// # Returns
    /// The token without value
    fn get_mut_token(&mut self, name: &str) -> Result<&mut Token, ReductError>;

    /// Get token list
    ///
    /// # Returns
    /// The token list, it the authentication is disabled, it returns an empty list
    fn get_token_list(&self) -> Result<Vec<Token>, ReductError>;

    /// Validate a token
    ///
    /// # Arguments
    /// `header` - The authorization header with bearer token
    ///
    /// # Returns
    ///
    /// Token with given value
    fn validate_token(&self, header: Option<&str>) -> Result<Token, ReductError>;

    /// Remove a token
    ///
    /// # Arguments
    /// `name` - The name of the token
    ///
    /// # Returns
    ///
    /// `Ok(())` if the token was removed successfully
    fn remove_token(&mut self, name: &str) -> Result<(), ReductError>;

    /// Remove a bucket from all tokens and save the repository
    /// to the file system
    ///
    /// # Arguments
    /// `bucket` - The name of the bucket
    ///
    /// # Returns
    /// `Ok(())` if the bucket was removed successfully
    fn remove_bucket_from_tokens(&mut self, bucket: &str) -> Result<(), ReductError>;
}

/// The TokenRepository trait is used to store and retrieve tokens.
struct TokenRepository {
    config_path: PathBuf,
    repo: HashMap<String, Token>,
}

pub fn parse_bearer_token(authorization_header: &str) -> Result<String, ReductError> {
    if !authorization_header.starts_with("Bearer ") {
        return Err(ReductError::unauthorized(
            "No bearer token in request header",
        ));
    }

    let token = authorization_header[7..].to_string();
    Ok(token)
}

impl From<Token> for ProtoToken {
    fn from(token: Token) -> Self {
        let permissions = if let Some(perm) = token.permissions {
            Some(ProtoPermissions {
                full_access: perm.full_access,
                read: perm.read,
                write: perm.write,
            })
        } else {
            None
        };

        ProtoToken {
            name: token.name,
            value: token.value,
            created_at: Some(Timestamp {
                seconds: token.created_at.timestamp(),
                nanos: token.created_at.timestamp_subsec_nanos() as i32,
            }),
            permissions: permissions,
        }
    }
}

impl Into<Token> for ProtoToken {
    fn into(self) -> Token {
        let permissions = if let Some(perm) = self.permissions {
            Some(Permissions {
                full_access: perm.full_access,
                read: perm.read,
                write: perm.write,
            })
        } else {
            None
        };

        let created_at = if let Some(ts) = self.created_at {
            let since_epoch = Duration::new(ts.seconds as u64, ts.nanos as u32);
            DateTime::<Utc>::from(UNIX_EPOCH + since_epoch)
        } else {
            warn!("Token has no creation time");
            Utc::now()
        };

        Token {
            name: self.name,
            value: self.value,
            created_at,
            permissions,
            is_provisioned: false,
        }
    }
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
                let toke_repository = ProtoTokenRepo::decode(&mut Bytes::from(data))
                    .expect("Could not decode token repository");
                for token in toke_repository.tokens {
                    token_repository
                        .repo
                        .insert(token.name.clone(), token.into());
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
            created_at: DateTime::<chrono::Utc>::from(SystemTime::now()),
            permissions: Some(Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            }),
            is_provisioned: false,
        };

        token_repository
            .repo
            .insert(init_token.name.clone(), init_token);
        token_repository
    }

    /// Save the token repository to the file system
    fn save_repo(&mut self) -> Result<(), ReductError> {
        let repo = TokenRepo {
            tokens: self
                .repo
                .iter()
                .map(|(_, token)| token.clone().into())
                .collect(),
        };
        let mut buf = Vec::new();
        repo.encode(&mut buf)
            .map_err(|_| ReductError::internal_server_error("Could not encode token repository"))?;
        std::fs::write(&self.config_path, buf).map_err(|err| {
            ReductError::internal_server_error(
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
    fn generate_token(
        &mut self,
        name: &str,
        permissions: Permissions,
    ) -> Result<TokenCreateResponse, ReductError> {
        // Check if the token isn't empty
        if name.is_empty() {
            return Err(ReductError::unprocessable_entity(
                "Token name can't be empty",
            ));
        }

        // Check if the token already exists
        if self.repo.contains_key(name) {
            return Err(ReductError::conflict(
                format!("Token '{}' already exists", name).as_str(),
            ));
        }

        let created_at = DateTime::<Utc>::from(SystemTime::now());

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
            is_provisioned: false,
        };

        self.repo.insert(name.to_string(), token);
        self.save_repo()?;

        Ok(TokenCreateResponse { value, created_at })
    }

    fn update_token(&mut self, name: &str, permissions: Permissions) -> Result<(), ReductError> {
        match self.repo.get(name) {
            Some(token) => {
                if token.is_provisioned {
                    Err(ReductError::conflict(
                        format!("Can't update provisioned token '{}'", name).as_str(),
                    ))
                } else {
                    let mut updated_token = token.clone();
                    updated_token.permissions = Some(permissions);
                    self.repo.insert(name.to_string(), updated_token);
                    self.save_repo()?;
                    Ok(())
                }
            }

            None => Err(ReductError::not_found(
                format!("Token '{}' doesn't exist", name).as_str(),
            )),
        }
    }

    fn get_token(&self, name: &str) -> Result<&Token, ReductError> {
        match self.repo.get(name) {
            Some(token) => Ok(token),
            None => Err(ReductError::not_found(
                format!("Token '{}' doesn't exist", name).as_str(),
            )),
        }
    }

    fn get_mut_token(&mut self, name: &str) -> Result<&mut Token, ReductError> {
        match self.repo.get_mut(name) {
            Some(token) => Ok(token),
            None => Err(ReductError::not_found(
                format!("Token '{}' doesn't exist", name).as_str(),
            )),
        }
    }

    fn get_token_list(&self) -> Result<Vec<Token>, ReductError> {
        let mut sorted: Vec<_> = self.repo.iter().collect();
        sorted.sort_by_key(|item| item.0);
        Ok(sorted
            .iter()
            .map(|item| {
                // for security reasons, we don't return the value
                let mut token = item.1.clone();
                token.value = "".to_string();
                token
            })
            .collect())
    }

    fn validate_token(&self, header: Option<&str>) -> Result<Token, ReductError> {
        let value = parse_bearer_token(header.unwrap_or(""))?;

        match self.repo.values().find(|token| token.value == value) {
            Some(token) => {
                // for security reasons, we don't return the value
                let mut token = token.clone();
                token.value = "".to_string();
                Ok(token)
            }
            None => Err(ReductError::unauthorized("Invalid token")),
        }
    }

    fn remove_token(&mut self, name: &str) -> Result<(), ReductError> {
        if name == INIT_TOKEN_NAME {
            return Err(ReductError::bad_request("Cannot remove init token"));
        }

        if let Some(token) = self.repo.get(name) {
            if token.is_provisioned {
                return Err(ReductError::conflict(
                    format!("Can't remove provisioned token '{}'", name).as_str(),
                ));
            }
        }

        if self.repo.remove(name).is_none() {
            Err(ReductError::not_found(
                format!("Token '{}' doesn't exist", name).as_str(),
            ))
        } else {
            self.save_repo()
        }
    }

    fn remove_bucket_from_tokens(&mut self, bucket: &str) -> Result<(), ReductError> {
        for token in self.repo.values_mut() {
            if let Some(permissions) = &mut token.permissions {
                permissions.read.retain(|b| b != bucket);
                permissions.write.retain(|b| b != bucket);
            }
        }

        self.save_repo()
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
    fn generate_token(
        &mut self,
        _name: &str,
        _permissions: Permissions,
    ) -> Result<TokenCreateResponse, ReductError> {
        Err(ReductError::bad_request("Authentication is disabled"))
    }

    fn update_token(&mut self, _name: &str, _permissions: Permissions) -> Result<(), ReductError> {
        Err(ReductError::bad_request("Authentication is disabled"))
    }

    fn get_token(&self, _name: &str) -> Result<&Token, ReductError> {
        Err(ReductError::bad_request("Authentication is disabled"))
    }

    fn get_mut_token(&mut self, _name: &str) -> Result<&mut Token, ReductError> {
        Err(ReductError::bad_request("Authentication is disabled"))
    }

    fn get_token_list(&self) -> Result<Vec<Token>, ReductError> {
        Ok(vec![])
    }

    fn validate_token(&self, _header: Option<&str>) -> Result<Token, ReductError> {
        Ok(Token {
            name: "AUTHENTICATION-DISABLED".to_string(),
            value: "".to_string(),
            created_at: DateTime::<Utc>::from(SystemTime::now()),
            permissions: Some(Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            }),
            is_provisioned: false,
        })
    }

    fn remove_token(&mut self, _name: &str) -> Result<(), ReductError> {
        Ok(())
    }

    fn remove_bucket_from_tokens(&mut self, _bucket: &str) -> Result<(), ReductError> {
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
    use chrono::Timelike;
    use rstest::{fixture, rstest};
    use tempfile::tempdir;

    #[rstest]
    fn test_init_token(repo: Box<dyn ManageTokens>) {
        let token = repo.validate_token(Some("Bearer init-token")).unwrap();
        assert_eq!(token.name, "init-token");
        assert_eq!(token.value, "");

        let token_list = repo.get_token_list().unwrap();
        assert_eq!(token_list.len(), 2);
        assert_eq!(token_list[0].name, "init-token");
    }

    //------------
    // create_token tests
    //------------
    #[rstest]
    fn test_create_empty_token(mut repo: Box<dyn ManageTokens>) {
        let token = repo.generate_token(
            "",
            Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            },
        );

        assert_eq!(
            token,
            Err(ReductError::unprocessable_entity(
                "Token name can't be empty"
            ))
        );
    }

    #[rstest]
    fn test_create_existing_token(mut repo: Box<dyn ManageTokens>) {
        let token = repo.generate_token(
            "test",
            Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            },
        );

        assert_eq!(
            token,
            Err(ReductError::conflict("Token 'test' already exists"))
        );
    }

    #[rstest]
    fn test_create_token(mut repo: Box<dyn ManageTokens>) {
        let token = repo
            .generate_token(
                "test-1",
                Permissions {
                    full_access: true,
                    read: vec![],
                    write: vec![],
                },
            )
            .unwrap();

        assert_eq!(token.value.len(), 39);
        assert_eq!(token.value, "test-1-".to_string() + &token.value[7..]);
        assert!(token.created_at.second() > 0);
    }

    #[rstest]
    fn test_create_token_persistent(path: PathBuf) {
        let mut repo = create_token_repository(path.clone(), "test");
        repo.generate_token(
            "test",
            Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            },
        )
        .unwrap();

        let repo = create_token_repository(path.clone(), "test");
        assert_eq!(repo.get_token("test").unwrap().name, "test");
    }

    #[rstest]
    fn test_create_token_no_init_token(mut disabled_repo: Box<dyn ManageTokens>) {
        let token = disabled_repo.generate_token(
            "test",
            Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            },
        );

        assert_eq!(
            token,
            Err(ReductError::bad_request("Authentication is disabled"))
        );
    }

    //------------
    // update_token tests
    //------------
    #[rstest]
    fn test_update_token_ok(mut repo: Box<dyn ManageTokens>) {
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

        let token = repo.get_token("test").unwrap().clone();

        assert_eq!(token.name, "test");

        let permissions = token.permissions.unwrap();
        assert_eq!(permissions.full_access, false);
        assert_eq!(permissions.read, vec!["test".to_string()]);
    }

    #[rstest]
    fn test_update_token_not_found(mut repo: Box<dyn ManageTokens>) {
        let token = repo.update_token(
            "test-1",
            Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            },
        );

        assert_eq!(
            token,
            Err(ReductError::not_found("Token 'test-1' doesn't exist"))
        );
    }

    #[rstest]
    fn test_update_token_persistent(path: PathBuf) {
        let mut repo = create_token_repository(path.clone(), "test");
        repo.generate_token(
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

        let repo = create_token_repository(path.clone(), "test");
        let token = repo.get_token("test").unwrap().clone();

        assert_eq!(token.name, "test");

        let permissions = token.permissions.unwrap();
        assert_eq!(permissions.full_access, false);
        assert_eq!(permissions.read, vec!["test".to_string()]);
    }

    #[rstest]
    fn test_update_provisioned_token(mut repo: Box<dyn ManageTokens>) {
        let mut token = repo.get_mut_token("test").unwrap();
        token.is_provisioned = true;

        let token = repo.update_token("test", Permissions::default());

        assert_eq!(
            token,
            Err(ReductError::conflict(
                "Can't update provisioned token 'test'"
            ))
        );
    }

    #[rstest]
    fn test_update_token_no_init_token(mut disabled_repo: Box<dyn ManageTokens>) {
        let token = disabled_repo.update_token(
            "test",
            Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            },
        );

        assert_eq!(
            token,
            Err(ReductError::bad_request("Authentication is disabled"))
        );
    }

    //----------------
    // find_by_name tests
    //----------------
    #[rstest]
    fn test_find_by_name(mut repo: Box<dyn ManageTokens>) {
        let token = repo.get_token("test").unwrap();
        assert_eq!(token.name, "test");
        assert!(token.value.starts_with("test-"));
    }

    #[rstest]
    fn test_find_by_name_not_found(mut repo: Box<dyn ManageTokens>) {
        let token = repo.get_token("test-1");
        assert_eq!(
            token,
            Err(ReductError::not_found("Token 'test-1' doesn't exist"))
        );
    }

    #[rstest]
    fn test_find_by_name_no_init_token(disabled_repo: Box<dyn ManageTokens>) {
        let token = disabled_repo.get_token("test");
        assert_eq!(
            token,
            Err(ReductError::bad_request("Authentication is disabled"))
        );
    }

    //------------
    // get_token_list tests
    //------------
    #[rstest]
    fn test_get_token_list(mut repo: Box<dyn ManageTokens>) {
        let token_list = repo.get_token_list().unwrap();

        assert_eq!(token_list.len(), 2);
        assert_eq!(token_list[1].name, "test");
        assert_eq!(token_list[1].value, "");
        assert_eq!(
            token_list[1].permissions,
            Some(Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            })
        );
    }

    #[rstest]
    fn test_get_token_list_no_init_token(disabled_repo: Box<dyn ManageTokens>) {
        let token_list = disabled_repo.get_token_list().unwrap();
        assert_eq!(token_list, vec![]);
    }

    //------------
    // validate_token tests
    //------------
    #[rstest]
    fn test_validate_token(mut repo: Box<dyn ManageTokens>) {
        let value = repo
            .generate_token(
                "test-1",
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
                name: "test-1".to_string(),
                created_at: token.created_at.clone(),
                value: "".to_string(),
                permissions: Some(Permissions {
                    full_access: true,
                    read: vec!["bucket-1".to_string()],
                    write: vec!["bucket-2".to_string()],
                }),
                is_provisioned: false,
            }
        );
    }

    #[rstest]
    fn test_validate_token_not_found(repo: Box<dyn ManageTokens>) {
        let token = repo.validate_token(Some("Bearer invalid-value"));
        assert_eq!(token, Err(ReductError::unauthorized("Invalid token")));
    }

    #[rstest]
    fn test_validate_token_no_init_token(disabled_repo: Box<dyn ManageTokens>) {
        let placeholder = disabled_repo.validate_token(Some("invalid-value")).unwrap();

        assert_eq!(placeholder.name, "AUTHENTICATION-DISABLED");
        assert_eq!(placeholder.value, "");
        assert_eq!(placeholder.permissions.unwrap().full_access, true);
    }

    //------------
    // remove_token tests
    //------------
    #[rstest]
    fn test_remove_token(mut repo: Box<dyn ManageTokens>) {
        let token = repo.remove_token("test").unwrap();
        assert_eq!(token, ());
    }

    #[rstest]
    fn test_remove_init_token(mut repo: Box<dyn ManageTokens>) {
        let token = repo.remove_token("init-token");
        assert_eq!(
            token,
            Err(ReductError::bad_request("Cannot remove init token"))
        );
    }

    #[rstest]
    fn test_remove_token_not_found(mut repo: Box<dyn ManageTokens>) {
        let token = repo.remove_token("test-1");
        assert_eq!(
            token,
            Err(ReductError::not_found("Token 'test-1' doesn't exist"))
        );
    }

    #[rstest]
    fn test_remove_token_persistent(path: PathBuf, init_token: &str) {
        let mut repo = create_token_repository(path.clone(), init_token);
        repo.generate_token(
            "test",
            Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            },
        )
        .unwrap();

        repo.remove_token("test").unwrap();

        let repo = create_token_repository(path, init_token);
        let token = repo.get_token("test");

        assert_eq!(
            token,
            Err(ReductError::not_found("Token 'test' doesn't exist"))
        );
    }

    #[rstest]
    fn test_remove_token_no_init_token(mut disabled_repo: Box<dyn ManageTokens>) {
        let token = disabled_repo.remove_token("test");
        assert_eq!(token, Ok(()));
    }

    #[rstest]
    fn test_remove_provisioned_token(mut repo: Box<dyn ManageTokens>) {
        let mut token = repo.get_mut_token("test").unwrap();
        token.is_provisioned = true;

        let err = repo.remove_token("test").err().unwrap();
        assert_eq!(
            err,
            ReductError::conflict("Can't remove provisioned token 'test'")
        )
    }

    #[fixture]
    fn path() -> PathBuf {
        tempdir().unwrap().into_path()
    }

    #[fixture]
    fn init_token() -> &'static str {
        "init-token"
    }

    #[fixture]
    fn repo(path: PathBuf, init_token: &str) -> Box<dyn ManageTokens> {
        let mut repo = create_token_repository(path, init_token);
        repo.generate_token(
            "test",
            Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            },
        )
        .unwrap();
        repo
    }

    #[fixture]
    fn disabled_repo(path: PathBuf) -> Box<dyn ManageTokens> {
        create_token_repository(path, "")
    }
}
