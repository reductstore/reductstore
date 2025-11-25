// Copyright 2023-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

mod disabled;
mod read_only;
mod repo;

use crate::auth::token_repository::disabled::NoAuthRepository;
use crate::auth::token_repository::read_only::ReadOnlyTokenRepository;
use crate::auth::token_repository::repo::TokenRepository;
use crate::cfg::{Cfg, InstanceRole};
use chrono::{DateTime, Utc};
use log::warn;
use prost_wkt_types::Timestamp;
use reduct_base::error::ReductError;
use reduct_base::msg::token_api::{Permissions, Token, TokenCreateResponse};
use reduct_base::{not_found, unauthorized};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::time::{Duration, UNIX_EPOCH};

const TOKEN_REPO_FILE_NAME: &str = ".auth";
const INIT_TOKEN_NAME: &str = "init-token";

pub(crate) fn parse_bearer_token(authorization_header: &str) -> Result<String, ReductError> {
    if !authorization_header.starts_with("Bearer ") {
        return Err(ReductError::unauthorized(
            "No bearer token in request header",
        ));
    }

    let token = authorization_header[7..].to_string();
    Ok(token)
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
            created_at: Some(Timestamp {
                seconds: token.created_at.timestamp(),
                nanos: token.created_at.timestamp_subsec_nanos() as i32,
            }),
            permissions,
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
    fn generate_token(
        &mut self,
        name: &str,
        permissions: Permissions,
    ) -> Result<TokenCreateResponse, ReductError>;

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
    fn rename_bucket(&mut self, old_name: &str, new_name: &str) -> Result<(), ReductError>;
}

pub(crate) trait TokenRepoCommon {
    fn repo(&self) -> &std::collections::HashMap<String, Token>;

    fn get_token(&self, name: &str) -> Result<&Token, ReductError> {
        self.repo()
            .get(name)
            .ok_or_else(|| not_found!("Token '{}' doesn't exist", name))
    }

    fn get_token_list(&self) -> Result<Vec<Token>, ReductError> {
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

    fn validate_token(&self, header: Option<&str>) -> Result<Token, ReductError> {
        let value = parse_bearer_token(header.unwrap_or(""))?;
        self.repo()
            .values()
            .find(|token| token.value == value)
            .cloned()
            .ok_or_else(|| unauthorized!("Invalid token"))
    }
}

pub(crate) type BoxedTokenRepository = Box<dyn ManageTokens + Send + Sync>;

pub(crate) struct TokenRepositoryBuilder {
    cfg: Cfg,
}

impl TokenRepositoryBuilder {
    pub fn new(cfg: Cfg) -> Self {
        Self { cfg }
    }

    pub fn build(self, config_path: PathBuf) -> BoxedTokenRepository {
        if self.cfg.role == InstanceRole::ReadOnly {
            return Box::new(ReadOnlyTokenRepository::new(
                config_path,
                self.cfg.api_token,
            ));
        }

        if !self.cfg.api_token.is_empty() {
            Box::new(TokenRepository::new(config_path, self.cfg.api_token))
        } else {
            Box::new(NoAuthRepository::new())
        }
    }
}
