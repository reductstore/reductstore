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
use std::net::IpAddr;
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
        let permissions = token
            .permissions
            .map(|perm| crate::auth::proto::token::Permissions {
                full_access: perm.full_access,
                read: perm.read,
                write: perm.write,
            });

        crate::auth::proto::Token {
            name: token.name,
            value: token.value,
            created_at: Some(datetime_to_proto_timestamp(token.created_at)),
            expires_at: token.expires_at.map(datetime_to_proto_timestamp),
            permissions,
            is_provisioned: token.is_provisioned,
            ttl: token.ttl.unwrap_or_default(),
            ip_allowlist: token.ip_allowlist,
        }
    }
}

impl Into<Token> for crate::auth::proto::Token {
    fn into(self) -> Token {
        let permissions = self.permissions.map(|perm| Permissions {
            full_access: perm.full_access,
            read: perm.read,
            write: perm.write,
        });

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
            ttl: if self.ttl == 0 { None } else { Some(self.ttl) },
            last_access: None,
            ip_allowlist: self.ip_allowlist,
            is_expired: false,
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
    async fn validate_token(
        &mut self,
        header: Option<&str>,
        client_ip: Option<IpAddr>,
    ) -> Result<Token, ReductError>;

    /// Validate a token and resolve its last-access timestamp from audit cache.
    async fn validate_token_with_last_access(
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

pub(crate) fn token_is_expired(token: &Token, now: DateTime<Utc>) -> bool {
    if let Some(expiry) = token.expires_at {
        if now >= expiry {
            return true;
        }
    }

    if let Some(ttl) = token.ttl {
        let last_access = token.last_access.unwrap_or(token.created_at);
        if (now - last_access).num_seconds() > ttl as i64 {
            return true;
        }
    }

    false
}

pub(super) fn check_token_lifetime(token: &Token) -> Result<(), ReductError> {
    if token_is_expired(token, Utc::now()) {
        return Err(unauthorized!("Token has expired"));
    }
    Ok(())
}

fn check_token_ip_allowlist(token: &Token, client_ip: Option<IpAddr>) -> Result<(), ReductError> {
    if token.ip_allowlist.is_empty() {
        return Ok(());
    }

    let client_ip =
        client_ip.ok_or_else(|| unauthorized!("Client IP is required for this token"))?;

    if token
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
    use std::collections::HashMap;
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
            ttl: None,
            last_access: None,
            ip_allowlist: vec![],
            is_expired: false,
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
            ttl: None,
            last_access: None,
            ip_allowlist: vec![],
            is_expired: false,
        };

        assert!(check_token_lifetime(&token).is_ok());
    }

    #[test]
    fn test_token_is_expired_by_inactivity_ttl() {
        let now = Utc::now();
        let token = Token {
            name: "ttl-expired".to_string(),
            value: "ttl-expired".to_string(),
            created_at: now - Duration::seconds(20),
            permissions: None,
            is_provisioned: false,
            expires_at: None,
            ttl: Some(5),
            last_access: Some(now - Duration::seconds(10)),
            ip_allowlist: vec![],
            is_expired: false,
        };

        assert!(token_is_expired(&token, now));
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
            }),
            is_provisioned: false,
            expires_at: None,
            ttl: None,
            last_access: None,
            ip_allowlist: vec!["203.0.113.5".to_string(), "10.10.0.0/16".to_string()],
            is_expired: false,
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
            }),
            is_provisioned: false,
            expires_at: None,
            ttl: None,
            last_access: None,
            ip_allowlist: vec!["203.0.113.5".to_string()],
            is_expired: false,
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
                    ..Default::default()
                },
            )
            .await
            .err()
            .unwrap();
        assert_eq!(err, forbidden!("Cannot generate token in read-only mode"));
    }

    #[test]
    fn test_resolve_last_access_from_cache_exact_name() {
        let mut cache = HashMap::new();
        cache.insert("token-a".to_string(), 1_000_000);

        let ts = resolve_last_access_from_cache(&cache, "token-a").unwrap();
        assert_eq!(
            ts,
            DateTime::<Utc>::from_timestamp_micros(1_000_000).unwrap()
        );
    }

    #[test]
    fn test_resolve_last_access_from_cache_instance_prefixed_name() {
        let mut cache = HashMap::new();
        cache.insert("instance-a/token-a".to_string(), 2_000_000);

        let ts = resolve_last_access_from_cache(&cache, "token-a").unwrap();
        assert_eq!(
            ts,
            DateTime::<Utc>::from_timestamp_micros(2_000_000).unwrap()
        );
    }

    #[test]
    fn test_resolve_last_access_from_cache_prefers_latest_match() {
        let mut cache = HashMap::new();
        cache.insert("token-a".to_string(), 1_000_000);
        cache.insert("instance-a/token-a".to_string(), 3_000_000);
        cache.insert("instance-b/token-a".to_string(), 2_000_000);

        let ts = resolve_last_access_from_cache(&cache, "token-a").unwrap();
        assert_eq!(
            ts,
            DateTime::<Utc>::from_timestamp_micros(3_000_000).unwrap()
        );
    }

    #[test]
    fn test_resolve_last_access_from_cache_no_match() {
        let mut cache = HashMap::new();
        cache.insert("instance-a/other-token".to_string(), 1_000_000);

        assert!(resolve_last_access_from_cache(&cache, "token-a").is_none());
    }
}
