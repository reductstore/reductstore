// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

mod disabled;
mod read_only;
mod repo;

use crate::auth::token_repository::disabled::NoAuthRepository;
use crate::auth::token_repository::read_only::ReadOnlyTokenRepository;
use crate::auth::token_repository::repo::TokenRepository;
use crate::auth::token_secret::verify_token_secret;
use crate::cfg::{Cfg, InstanceRole};
use crate::storage::engine::StorageEngine;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use log::warn;
use prost_wkt_types::Timestamp;
use reduct_base::error::ReductError;
use reduct_base::msg::token_api::{Permissions, Token, TokenCreateRequest, TokenCreateResponse};
use reduct_base::{not_found, unauthorized};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

const TOKEN_REPO_FILE_NAME: &str = ".auth";
pub(crate) const INIT_TOKEN_NAME: &str = "init-token";

pub(crate) fn parse_bearer_token(authorization_header: &str) -> Result<String, ReductError> {
    if !authorization_header.starts_with("Bearer ") {
        return Err(ReductError::unauthorized(
            "No bearer token in request header",
        ));
    }

    let token = authorization_header[7..].to_string();
    Ok(token)
}

#[inline]
pub(super) fn resolve_last_access_from_cache(
    cache: &HashMap<String, u64>,
    token_name: &str,
) -> Option<DateTime<Utc>> {
    let suffix = format!("/{}", token_name);
    cache
        .iter()
        .filter_map(|(entry_name, timestamp)| {
            if entry_name == token_name || entry_name.ends_with(&suffix) {
                Some(*timestamp)
            } else {
                None
            }
        })
        .max()
        .and_then(|timestamp| DateTime::<Utc>::from_timestamp_micros(timestamp as i64))
}

#[inline]
fn datetime_to_proto_timestamp(ts: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: ts.timestamp(),
        nanos: ts.timestamp_subsec_nanos() as i32,
    }
}

#[inline]
fn proto_timestamp_to_datetime(ts: Timestamp) -> DateTime<Utc> {
    let since_epoch = std::time::Duration::new(ts.seconds as u64, ts.nanos as u32);
    DateTime::<Utc>::from(UNIX_EPOCH + since_epoch)
}

impl From<Token> for crate::auth::proto::Token {
    fn from(token: Token) -> Self {
        let permissions = if let Some(perm) = token.permissions {
            Some(crate::auth::proto::token::Permissions {
                full_access: perm.full_access,
                read: perm.read,
                write: perm.write,
            })
        } else {
            None
        };

        crate::auth::proto::Token {
            name: token.name,
            value: token.value,
            created_at: Some(datetime_to_proto_timestamp(token.created_at)),
            expires_at: token.expires_at.map(datetime_to_proto_timestamp),
            permissions,
            is_provisioned: token.is_provisioned,
        }
    }
}

impl Into<Token> for crate::auth::proto::Token {
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

        let created_at = self.created_at.map_or_else(
            || {
                warn!("Token has no creation time");
                Utc::now()
            },
            proto_timestamp_to_datetime,
        );
        let expires_at = self.expires_at.map(proto_timestamp_to_datetime);

        Token {
            name: self.name,
            value: self.value,
            created_at,
            permissions,
            is_provisioned: self.is_provisioned,
            expires_at,
            last_access: None,
        }
    }
}

#[async_trait]
pub(crate) trait ManageTokens {
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
    async fn generate_token(
        &mut self,
        name: &str,
        request: TokenCreateRequest,
    ) -> Result<TokenCreateResponse, ReductError>;

    /// Get a token by name
    ///
    /// # Arguments
    ///
    /// `name` - The name of the token
    ///
    /// # Returns
    /// The token without value
    async fn get_token(&mut self, name: &str) -> Result<&Token, ReductError>;

    /// Get a token by name with the last-access timestamp resolved from audit cache.
    async fn get_token_with_last_access(&mut self, name: &str) -> Result<Token, ReductError>;

    /// Replace an existing token and persist the repository if needed.
    ///
    /// # Arguments
    ///
    /// `token` - The token to replace
    async fn update_token(&mut self, token: Token) -> Result<(), ReductError>;

    /// Get token list
    ///
    /// # Returns
    /// The token list, it the authentication is disabled, it returns an empty list
    async fn get_token_list(&mut self) -> Result<Vec<Token>, ReductError>;

    /// Get token list with last-access timestamps resolved from audit cache.
    async fn get_token_list_with_last_access(&mut self) -> Result<Vec<Token>, ReductError>;

    /// Validate a token
    ///
    /// # Arguments
    /// `header` - The authorization header with bearer token
    ///
    /// # Returns
    ///
    /// Token with given value
    async fn validate_token(&mut self, header: Option<&str>) -> Result<Token, ReductError>;

    /// Validate a token and resolve its last-access timestamp from audit cache.
    async fn validate_token_with_last_access(
        &mut self,
        header: Option<&str>,
    ) -> Result<Token, ReductError>;

    /// Remove a token
    ///
    /// # Arguments
    /// `name` - The name of the token
    ///
    /// # Returns
    ///
    /// `Ok(())` if the token was removed successfully
    async fn remove_token(&mut self, name: &str) -> Result<(), ReductError>;

    /// Rotate an existing token and return a new token value.
    ///
    /// # Arguments
    /// `name` - The name of the token
    ///
    /// # Returns
    /// New token value and creation time.
    async fn rotate_token(&mut self, name: &str) -> Result<TokenCreateResponse, ReductError>;

    /// Remove a bucket from all tokens and save the repository
    /// to the file system
    ///
    /// # Arguments
    /// `bucket` - The name of the bucket
    ///
    /// # Returns
    /// `Ok(())` if the bucket was removed successfully
    async fn remove_bucket_from_tokens(&mut self, bucket: &str) -> Result<(), ReductError>;

    /// Rename a bucket in all tokens
    ///
    /// # Arguments
    ///
    /// `old_name` - The old name of the bucket
    /// `new_name` - The new name of the bucket
    ///
    /// # Returns
    ///
    /// `Ok(())` if the bucket was renamed successfully
    async fn rename_bucket(&mut self, old_name: &str, new_name: &str) -> Result<(), ReductError>;
}

pub(super) trait AccessTokens {
    fn repo(&self) -> &std::collections::HashMap<String, Token>;

    fn get_token(&mut self, name: &str) -> Result<&Token, ReductError> {
        self.repo()
            .get(name)
            .ok_or_else(|| not_found!("Token '{}' doesn't exist", name))
    }

    fn get_token_list(&mut self) -> Result<Vec<Token>, ReductError> {
        let mut sorted: Vec<_> = self.repo().iter().collect();
        sorted.sort_by_key(|item| item.0);
        Ok(sorted
            .iter()
            .map(|item| {
                let mut token = item.1.clone();
                token.value = "".to_string();
                token
            })
            .collect())
    }

    fn validate_token(&mut self, header: Option<&str>) -> Result<Token, ReductError> {
        let value = parse_bearer_token(header.unwrap_or(""))?;
        let token = self
            .repo()
            .values()
            .find(|token| verify_token_secret(&token.value, &value))
            .cloned()
            .ok_or_else(|| unauthorized!("Invalid token"))?;
        check_token_lifetime(&token)?;
        Ok(token)
    }
}

pub(super) fn check_token_lifetime(token: &Token) -> Result<(), ReductError> {
    if let Some(expiry) = token.expires_at {
        if Utc::now() >= expiry {
            return Err(unauthorized!("Token has expired"));
        }
    }
    Ok(())
}

pub(crate) type BoxedTokenRepository = Box<dyn ManageTokens + Send + Sync>;

pub(crate) struct TokenRepositoryBuilder {
    cfg: Cfg,
}

impl TokenRepositoryBuilder {
    pub fn new(cfg: Cfg) -> Self {
        Self { cfg }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn build(self, config_path: PathBuf) -> BoxedTokenRepository {
        self.build_internal(config_path, None).await
    }

    pub async fn build_with_storage(
        self,
        config_path: PathBuf,
        storage: Arc<StorageEngine>,
    ) -> BoxedTokenRepository {
        self.build_internal(config_path, Some(storage)).await
    }

    async fn build_internal(
        self,
        config_path: PathBuf,
        storage: Option<Arc<StorageEngine>>,
    ) -> BoxedTokenRepository {
        if self.cfg.role == InstanceRole::Replica {
            return Box::new(
                ReadOnlyTokenRepository::new(config_path, self.cfg.clone(), storage).await,
            ) as BoxedTokenRepository;
        }

        if !self.cfg.api_token.is_empty() {
            Box::new(TokenRepository::new(config_path, self.cfg.api_token, storage).await)
                as BoxedTokenRepository
        } else {
            Box::new(NoAuthRepository::new()) as BoxedTokenRepository
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use reduct_base::{forbidden, unauthorized};
    use tempfile::tempdir;

    #[test]
    fn test_check_token_lifetime_expired() {
        let token = Token {
            name: "expired".to_string(),
            value: "expired".to_string(),
            created_at: Utc::now(),
            permissions: None,
            is_provisioned: false,
            expires_at: Some(Utc::now() - Duration::seconds(1)),
            last_access: None,
        };

        assert_eq!(
            check_token_lifetime(&token).err().unwrap(),
            unauthorized!("Token has expired")
        );
    }

    #[test]
    fn test_check_token_lifetime_no_expiry() {
        let token = Token {
            name: "no-expiry".to_string(),
            value: "no-expiry".to_string(),
            created_at: Utc::now(),
            permissions: None,
            is_provisioned: false,
            expires_at: None,
            last_access: None,
        };

        assert!(check_token_lifetime(&token).is_ok());
    }

    #[tokio::test]
    async fn test_token_repository_builder_replica_read_only() {
        let mut cfg = Cfg::default();
        cfg.role = InstanceRole::Replica;
        let path = tempdir().unwrap().keep();

        let mut repo = TokenRepositoryBuilder::new(cfg).build(path).await;
        let err = repo
            .generate_token(
                "test",
                TokenCreateRequest {
                    permissions: Permissions::default(),
                    expires_at: None,
                },
            )
            .await
            .err()
            .unwrap();
        assert_eq!(err, forbidden!("Cannot generate token in read-only mode"));
    }
}
