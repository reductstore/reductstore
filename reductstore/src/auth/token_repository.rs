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
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use log::warn;
use prost_wkt_types::Timestamp;
use reduct_base::error::ReductError;
use reduct_base::msg::token_api::{Permissions, Token, TokenCreateRequest, TokenCreateResponse};
use reduct_base::{not_found, unauthorized};
use std::net::IpAddr;
use std::path::PathBuf;
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

impl From<Token> for crate::auth::proto::Token {
    fn from(token: Token) -> Self {
        let permissions = if let Some(perm) = token.permissions {
            Some(crate::auth::proto::token::Permissions {
                full_access: perm.full_access,
                read: perm.read,
                write: perm.write,
                ip_allowlist: perm.ip_allowlist,
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
                ip_allowlist: perm.ip_allowlist,
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
            is_provisioned: self.is_provisioned,
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

    /// Validate a token
    ///
    /// # Arguments
    /// `header` - The authorization header with bearer token
    ///
    /// # Returns
    ///
    /// Token with given value
    async fn validate_token(
        &mut self,
        header: Option<&str>,
        client_ip: Option<IpAddr>,
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

    fn validate_token(
        &mut self,
        header: Option<&str>,
        client_ip: Option<IpAddr>,
    ) -> Result<Token, ReductError> {
        let value = parse_bearer_token(header.unwrap_or(""))?;
        let token = self
            .repo()
            .values()
            .find(|token| verify_token_secret(&token.value, &value))
            .cloned()
            .ok_or_else(|| unauthorized!("Invalid token"))?;
        check_token_lifetime(&token)?;
        check_token_ip_allowlist(&token, client_ip)?;
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

fn check_token_ip_allowlist(token: &Token, client_ip: Option<IpAddr>) -> Result<(), ReductError> {
    let Some(permissions) = token.permissions.as_ref() else {
        return Ok(());
    };

    if permissions.ip_allowlist.is_empty() {
        return Ok(());
    }

    let client_ip =
        client_ip.ok_or_else(|| unauthorized!("Client IP is required for this token"))?;

    if permissions
        .ip_allowlist
        .iter()
        .any(|entry| ip_allowlist_entry_matches(entry, client_ip))
    {
        Ok(())
    } else {
        Err(unauthorized!(
            "Token is not allowed for client IP {}",
            client_ip
        ))
    }
}

fn ip_allowlist_entry_matches(entry: &str, client_ip: IpAddr) -> bool {
    match entry.split_once('/') {
        Some((network, prefix)) => cidr_matches(network, prefix, client_ip),
        None => entry.parse::<IpAddr>().ok() == Some(client_ip),
    }
}

fn cidr_matches(network: &str, prefix: &str, client_ip: IpAddr) -> bool {
    let Ok(prefix_len) = prefix.parse::<u8>() else {
        return false;
    };

    let Ok(network_ip) = network.parse::<IpAddr>() else {
        return false;
    };

    match (network_ip, client_ip) {
        (IpAddr::V4(net), IpAddr::V4(client)) => {
            if prefix_len > 32 {
                return false;
            }
            let net = u32::from(net);
            let client = u32::from(client);
            let mask = if prefix_len == 0 {
                0
            } else {
                u32::MAX << (32 - prefix_len)
            };
            (net & mask) == (client & mask)
        }
        (IpAddr::V6(net), IpAddr::V6(client)) => {
            if prefix_len > 128 {
                return false;
            }
            let net = u128::from(net);
            let client = u128::from(client);
            let mask = if prefix_len == 0 {
                0
            } else {
                u128::MAX << (128 - prefix_len)
            };
            (net & mask) == (client & mask)
        }
        _ => false,
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
        };

        assert!(check_token_lifetime(&token).is_ok());
    }

    #[test]
    fn test_cidr_matches_ipv4() {
        assert!(cidr_matches("10.1.2.0", "24", "10.1.2.42".parse().unwrap()));
        assert!(!cidr_matches("10.1.2.0", "24", "10.1.3.1".parse().unwrap()));
    }

    #[test]
    fn test_cidr_matches_ipv6() {
        assert!(cidr_matches(
            "2001:db8::",
            "32",
            "2001:db8::1234".parse().unwrap()
        ));
        assert!(!cidr_matches(
            "2001:db8::",
            "32",
            "2001:db9::1".parse().unwrap()
        ));
    }

    #[test]
    fn test_check_token_ip_allowlist_accepts_exact_or_cidr() {
        let token = Token {
            name: "t".to_string(),
            value: "v".to_string(),
            created_at: Utc::now(),
            permissions: Some(Permissions {
                full_access: false,
                read: vec![],
                write: vec![],
                ip_allowlist: vec!["203.0.113.5".to_string(), "10.10.0.0/16".to_string()],
            }),
            is_provisioned: false,
            expires_at: None,
        };

        assert!(check_token_ip_allowlist(&token, Some("203.0.113.5".parse().unwrap())).is_ok());
        assert!(check_token_ip_allowlist(&token, Some("10.10.12.34".parse().unwrap())).is_ok());
        assert!(check_token_ip_allowlist(&token, Some("10.11.0.1".parse().unwrap())).is_err());
    }

    #[test]
    fn test_check_token_ip_allowlist_requires_client_ip_when_restricted() {
        let token = Token {
            name: "t".to_string(),
            value: "v".to_string(),
            created_at: Utc::now(),
            permissions: Some(Permissions {
                full_access: false,
                read: vec![],
                write: vec![],
                ip_allowlist: vec!["203.0.113.5".to_string()],
            }),
            is_provisioned: false,
            expires_at: None,
        };

        let err = check_token_ip_allowlist(&token, None).err().unwrap();
        assert_eq!(err, unauthorized!("Client IP is required for this token"));
    }

    #[test]
    fn test_ip_allowlist_entry_matches_invalid_entries() {
        assert!(!ip_allowlist_entry_matches(
            "not-an-ip",
            "203.0.113.5".parse().unwrap()
        ));
        assert!(!ip_allowlist_entry_matches(
            "10.0.0.0/abc",
            "10.0.0.1".parse().unwrap()
        ));
        assert!(!ip_allowlist_entry_matches(
            "10.0.0.0/33",
            "10.0.0.1".parse().unwrap()
        ));
        assert!(!ip_allowlist_entry_matches(
            "2001:db8::/129",
            "2001:db8::1".parse().unwrap()
        ));
    }

    #[test]
    fn test_ip_allowlist_entry_matches_family_mismatch() {
        assert!(!ip_allowlist_entry_matches(
            "10.0.0.0/24",
            "2001:db8::1".parse().unwrap()
        ));
        assert!(!ip_allowlist_entry_matches(
            "2001:db8::/32",
            "10.0.0.1".parse().unwrap()
        ));
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
