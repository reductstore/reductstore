// Copyright 2023-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

mod disabled;
mod read_only;
mod repo;

use crate::auth::token_repository::disabled::NoAuthRepository;
use crate::auth::token_repository::read_only::ReadOnlyTokenRepository;
use crate::auth::token_repository::repo::TokenRepository;
use crate::cfg::{Cfg, InstanceRole};
use async_trait::async_trait;
use chrono::{DateTime, Months, Utc};
use log::warn;
use prost_wkt_types::Timestamp;
use reduct_base::error::ReductError;
use reduct_base::msg::token_api::{Permissions, Token, TokenCreateRequest, TokenCreateResponse};
use reduct_base::{not_found, unauthorized, unprocessable_entity};
use std::path::PathBuf;
use std::time::UNIX_EPOCH;

const TOKEN_REPO_FILE_NAME: &str = ".auth";
const INIT_TOKEN_NAME: &str = "init-token";

pub(crate) fn parse_token_expiry_duration(
    expires_in: &str,
    from: DateTime<Utc>,
) -> Result<DateTime<Utc>, ReductError> {
    let normalized: String = expires_in.split_whitespace().collect();
    if normalized.len() < 2 {
        return Err(unprocessable_entity!(
            "Token expiration duration must be in format '<number><unit>', got '{}'",
            expires_in
        ));
    }

    let (value, unit) = normalized.split_at(normalized.len() - 1);
    let value = value.parse::<u64>().map_err(|_| {
        unprocessable_entity!(
            "Token expiration duration must start with an unsigned integer, got '{}'",
            expires_in
        )
    })?;

    let overflow_err =
        || unprocessable_entity!("Token expiration duration '{}' is too large", expires_in);

    let result = match unit {
        "s" | "S" => from.checked_add_signed(chrono::Duration::seconds(
            i64::try_from(value).map_err(|_| overflow_err())?,
        )),
        "m" => from.checked_add_signed(chrono::Duration::minutes(
            i64::try_from(value).map_err(|_| overflow_err())?,
        )),
        "h" | "H" => from.checked_add_signed(chrono::Duration::hours(
            i64::try_from(value).map_err(|_| overflow_err())?,
        )),
        "d" | "D" => from.checked_add_signed(chrono::Duration::days(
            i64::try_from(value).map_err(|_| overflow_err())?,
        )),
        "w" | "W" => from.checked_add_signed(chrono::Duration::weeks(
            i64::try_from(value).map_err(|_| overflow_err())?,
        )),
        "M" => from.checked_add_months(Months::new(
            u32::try_from(value).map_err(|_| overflow_err())?,
        )),
        "y" | "Y" => {
            let months = value.checked_mul(12).ok_or_else(overflow_err)?;
            from.checked_add_months(Months::new(
                u32::try_from(months).map_err(|_| overflow_err())?,
            ))
        }
        _ => {
            return Err(unprocessable_entity!(
                "Unsupported token expiration unit '{}' in '{}'. Use one of s,m,h,d,w,M,y",
                unit,
                expires_in
            ));
        }
    };

    result.ok_or_else(overflow_err)
}

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
            expires_at: token.expires_at.map(|ts| Timestamp {
                seconds: ts.timestamp(),
                nanos: ts.timestamp_subsec_nanos() as i32,
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
            let since_epoch = std::time::Duration::new(ts.seconds as u64, ts.nanos as u32);
            DateTime::<Utc>::from(UNIX_EPOCH + since_epoch)
        } else {
            warn!("Token has no creation time");
            Utc::now()
        };

        let expires_at = self.expires_at.map(|ts| {
            let since_epoch = std::time::Duration::new(ts.seconds as u64, ts.nanos as u32);
            DateTime::<Utc>::from(UNIX_EPOCH + since_epoch)
        });

        Token {
            name: self.name,
            value: self.value,
            created_at,
            permissions,
            is_provisioned: false,
            expires_at,
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

    /// Get a token by name (mutable)
    ///
    /// # Arguments
    ///
    /// `name` - The name of the token
    ///
    /// # Returns
    /// The token without value
    async fn get_mut_token(&mut self, name: &str) -> Result<&mut Token, ReductError>;

    /// Get token list
    ///
    /// # Returns
    /// The token list, it the authentication is disabled, it returns an empty list
    async fn get_token_list(&mut self) -> Result<Vec<Token>, ReductError>;

    /// Validate a token
    ///
    /// # Arguments
    /// `header` - The authorization header with bearer token
    ///
    /// # Returns
    ///
    /// Token with given value
    async fn validate_token(&mut self, header: Option<&str>) -> Result<Token, ReductError>;

    /// Remove a token
    ///
    /// # Arguments
    /// `name` - The name of the token
    ///
    /// # Returns
    ///
    /// `Ok(())` if the token was removed successfully
    async fn remove_token(&mut self, name: &str) -> Result<(), ReductError>;

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
            .find(|token| token.value == value)
            .cloned()
            .ok_or_else(|| unauthorized!("Invalid token"))?;
        check_token_lifetime(&token)?;
        Ok(token)
    }
}

fn check_token_lifetime(token: &Token) -> Result<(), ReductError> {
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

    pub async fn build(self, config_path: PathBuf) -> BoxedTokenRepository {
        if self.cfg.role == InstanceRole::Replica {
            return Box::new(ReadOnlyTokenRepository::new(config_path, self.cfg.clone()).await)
                as BoxedTokenRepository;
        }

        if !self.cfg.api_token.is_empty() {
            Box::new(TokenRepository::new(config_path, self.cfg.api_token).await)
                as BoxedTokenRepository
        } else {
            Box::new(NoAuthRepository::new()) as BoxedTokenRepository
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reduct_base::unprocessable_entity;
    use rstest::rstest;

    #[rstest]
    #[case("5D", "2026-02-22T00:00:00+00:00")]
    #[case("10M", "2026-12-17T00:00:00+00:00")]
    #[case("3h", "2026-02-17T03:00:00+00:00")]
    fn test_parse_token_expiry_duration(#[case] expires_in: &str, #[case] expected: &str) {
        let from = DateTime::parse_from_rfc3339("2026-02-17T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let expected = DateTime::parse_from_rfc3339(expected)
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(
            parse_token_expiry_duration(expires_in, from).unwrap(),
            expected
        );
    }

    #[rstest]
    #[case(
        "",
        "Token expiration duration must be in format '<number><unit>', got ''"
    )]
    #[case(
        "5",
        "Token expiration duration must be in format '<number><unit>', got '5'"
    )]
    #[case(
        "XD",
        "Token expiration duration must start with an unsigned integer, got 'XD'"
    )]
    #[case(
        "5Q",
        "Unsupported token expiration unit 'Q' in '5Q'. Use one of s,m,h,d,w,M,y"
    )]
    fn test_parse_token_expiry_duration_invalid(#[case] expires_in: &str, #[case] message: &str) {
        let from = DateTime::parse_from_rfc3339("2026-02-17T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(
            parse_token_expiry_duration(expires_in, from).err().unwrap(),
            unprocessable_entity!(message)
        );
    }
}
