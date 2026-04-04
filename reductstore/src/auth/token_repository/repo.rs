// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::audit::AUDIT_BUCKET_NAME;
use crate::auth::proto::TokenRepo;
use crate::auth::token_repository::AccessTokens;
use crate::auth::token_repository::{
    check_token_lifetime, parse_bearer_token, resolve_last_access_from_cache, ManageTokens,
    INIT_TOKEN_NAME, TOKEN_REPO_FILE_NAME,
};
use crate::auth::token_secret::{
    hash_token_secret, is_hashed_token_secret, matched_hashed_token_secret,
};
use crate::core::cache::Cache;
use crate::core::file_cache::FILE_CACHE;
use crate::storage::engine::StorageEngine;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use log::debug;
use prost::Message;
use rand::Rng;
use reduct_base::error::ErrorCode;
use reduct_base::error::ReductError;
use reduct_base::msg::token_api::{Permissions, Token, TokenCreateRequest, TokenCreateResponse};
use reduct_base::{conflict, not_found, unprocessable_entity};
use regex::Regex;
use std::collections::HashMap;
use std::io::{Read, SeekFrom, Write};
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::time::{Duration, Instant};

const TOKEN_LAST_ACCESS_CACHE_TTL: Duration = Duration::from_secs(10);

/// The TokenRepository trait is used to store and retrieve tokens.
pub(super) struct TokenRepository {
    config_path: PathBuf,
    repo: HashMap<String, Token>,
    permission_regex: Regex,
    storage: Option<Arc<StorageEngine>>,
    last_access_cache: HashMap<String, u64>,
    last_access_cache_updated_at: Option<Instant>,
    auth_cache: Cache<String, Token>,
}

const AUTH_CACHE_SIZE: usize = 1024;
const AUTH_CACHE_TTL: Duration = Duration::MAX;

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
    pub async fn new(
        data_path: PathBuf,
        api_token: String,
        storage: Option<Arc<StorageEngine>>,
    ) -> TokenRepository {
        let config_path = data_path.join(TOKEN_REPO_FILE_NAME);
        let repo = HashMap::new();

        // Load the token repository from the file system
        let permission_regex =
            Regex::new(r"^[*a-zA-Z0-9_\-]+$").expect("Invalid regex for permissions");
        let mut token_repository = TokenRepository {
            config_path,
            repo,
            permission_regex,
            storage,
            last_access_cache: HashMap::new(),
            last_access_cache_updated_at: None,
            auth_cache: Cache::new(AUTH_CACHE_SIZE, AUTH_CACHE_TTL),
        };

        let file = FILE_CACHE
            .read(&token_repository.config_path, SeekFrom::Start(0))
            .await;
        match file {
            Ok(mut file) => {
                debug!(
                    "Loading token repository from {}",
                    token_repository.config_path.as_path().display()
                );

                let mut buf = Vec::new();
                file.read_to_end(&mut buf)
                    .expect("Could not read token repository");

                let proto_repo = TokenRepo::decode(&mut Bytes::from(buf))
                    .expect("Could not decode token repository");
                for token in proto_repo.tokens {
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
                    .await
                    .expect("Failed to create a new token repository");
            }
        };

        let mut migrated = false;
        for token in token_repository.repo.values_mut() {
            if Self::ensure_hashed_token_secret(token).expect("Failed to hash token secret") {
                migrated = true;
            }
        }

        let full_access_permissions = Some(Permissions {
            full_access: true,
            read: vec![],
            write: vec![],
        });
        let existing_init_token = token_repository.repo.get(INIT_TOKEN_NAME).cloned();
        let init_token_value = token_repository
            .repo
            .get(INIT_TOKEN_NAME)
            .and_then(|token| matched_hashed_token_secret(&token.value, &api_token))
            .map(|secret| secret.to_string())
            .unwrap_or_else(|| {
                hash_token_secret(&api_token).expect("Failed to hash init token secret")
            });

        let init_token = Token {
            name: INIT_TOKEN_NAME.to_string(),
            value: init_token_value,
            created_at: existing_init_token
                .as_ref()
                .map(|token| token.created_at)
                .unwrap_or_else(|| DateTime::<Utc>::from(SystemTime::now())),
            permissions: full_access_permissions.clone(),
            is_provisioned: true,
            expires_at: None,
            ttl: None,
            last_access: None,
            ip_allowlist: vec![],
            is_expired: false,
        };

        token_repository
            .repo
            .insert(init_token.name.clone(), init_token);
        if migrated {
            token_repository
                .save_repo()
                .await
                .expect("Failed to persist token repository");
        }
        token_repository
    }

    async fn refresh_last_access_cache_if_needed(&mut self) {
        let now = Instant::now();
        if self
            .last_access_cache_updated_at
            .is_some_and(|ts| now.duration_since(ts) < TOKEN_LAST_ACCESS_CACHE_TTL)
        {
            return;
        }

        let Some(storage) = self.storage.as_ref() else {
            return;
        };

        let bucket = match storage.get_bucket(AUDIT_BUCKET_NAME).await {
            Ok(bucket) => bucket.upgrade_and_unwrap(),
            Err(err) if err.status == ErrorCode::NotFound => {
                self.last_access_cache.clear();
                self.last_access_cache_updated_at = Some(now);
                return;
            }
            Err(err) => {
                debug!("Failed to get audit bucket for token last access: {}", err);
                return;
            }
        };

        match bucket.info().await {
            Ok(bucket_info) => {
                self.last_access_cache = bucket_info
                    .entries
                    .into_iter()
                    .filter(|entry| entry.record_count > 0)
                    .map(|entry| (entry.name, entry.latest_record))
                    .collect();
                self.last_access_cache_updated_at = Some(now);
            }
            Err(err) => {
                debug!(
                    "Failed to read audit bucket info for token last access: {}",
                    err
                );
            }
        }
    }

    async fn populate_token_last_access(&mut self, token: &mut Token) {
        self.refresh_last_access_cache_if_needed().await;
        token.last_access = resolve_last_access_from_cache(&self.last_access_cache, &token.name);
    }

    /// Save the token repository to the file system
    async fn save_repo(&mut self) -> Result<(), ReductError> {
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

        let mut file = FILE_CACHE
            .write_or_create(&self.config_path, SeekFrom::Start(0))
            .await?;
        file.set_len(0)?;
        file.write_all(&buf)?;
        file.sync_all().await?;
        Ok(())
    }

    fn ensure_hashed_token_secret(token: &mut Token) -> Result<bool, ReductError> {
        if is_hashed_token_secret(&token.value) {
            return Ok(false);
        }

        token.value = hash_token_secret(&token.value)?;
        Ok(true)
    }

    fn clear_auth_cache(&mut self) {
        self.auth_cache.clear();
    }
}

impl AccessTokens for TokenRepository {
    fn repo(&self) -> &HashMap<String, Token> {
        &self.repo
    }
}

#[async_trait]
impl ManageTokens for TokenRepository {
    async fn generate_token(
        &mut self,
        name: &str,
        request: TokenCreateRequest,
    ) -> Result<TokenCreateResponse, ReductError> {
        let TokenCreateRequest {
            permissions,
            expires_at,
            ttl,
            ip_allowlist,
        } = request;

        // Check if the token isn't empty
        if name.is_empty() {
            return Err(unprocessable_entity!("Token name can't be empty"));
        }

        // Check if the token already exists
        if self.repo.contains_key(name) {
            return Err(conflict!("Token '{}' already exists", name));
        }

        for entry in permissions.read.iter().chain(&permissions.write) {
            if !entry.starts_with('$') && !self.permission_regex.is_match(entry) {
                return Err(unprocessable_entity!(
                    "Permission can contain only bucket names or wildcard '*', got '{}'",
                    entry
                ));
            }
        }

        let created_at = DateTime::<Utc>::from(SystemTime::now());
        if matches!(ttl, Some(0)) {
            return Err(unprocessable_entity!("Token TTL must be greater than zero"));
        }

        if ttl.is_some() && expires_at.is_some() {
            return Err(unprocessable_entity!(
                "Only one lifetime policy is allowed: either 'ttl' or 'expires_at'"
            ));
        }

        let expires_at = expires_at
            .map(|expires_at| {
                if expires_at < created_at {
                    Err(unprocessable_entity!(
                        "Token expiration date must not be in the past"
                    ))
                } else {
                    Ok(expires_at)
                }
            })
            .transpose()?;

        // Create a random hex string
        let (value, token) = {
            let mut rng = rand::rng();
            let value: String = (0..32)
                .map(|_| format!("{:x}", rng.random_range(0..16)))
                .collect();
            let value = format!("{}-{}", name, value);
            let secret = hash_token_secret(&value)?;
            (
                value.clone(),
                Token {
                    name: name.to_string(),
                    value: secret,
                    created_at: created_at.clone(),
                    permissions: Some(permissions),
                    is_provisioned: false,
                    expires_at,
                    ttl,
                    last_access: None,
                    ip_allowlist,
                    is_expired: false,
                },
            )
        };

        self.repo.insert(name.to_string(), token);
        self.save_repo().await?;
        self.clear_auth_cache();

        Ok(TokenCreateResponse { value, created_at })
    }

    async fn get_token(&mut self, name: &str) -> Result<&Token, ReductError> {
        AccessTokens::get_token(self, name)
    }

    async fn get_token_with_last_access(&mut self, name: &str) -> Result<Token, ReductError> {
        let mut token = AccessTokens::get_token(self, name)?.clone();
        self.populate_token_last_access(&mut token).await;
        Ok(token)
    }

    async fn update_token(&mut self, mut token: Token) -> Result<(), ReductError> {
        if !self.repo.contains_key(&token.name) {
            return Err(not_found!("Token '{}' doesn't exist", token.name));
        }

        Self::ensure_hashed_token_secret(&mut token)?;
        self.repo.insert(token.name.clone(), token);
        self.save_repo().await?;
        self.clear_auth_cache();
        Ok(())
    }

    async fn get_token_list(&mut self) -> Result<Vec<Token>, ReductError> {
        AccessTokens::get_token_list(self)
    }

    async fn get_token_list_with_last_access(&mut self) -> Result<Vec<Token>, ReductError> {
        let mut tokens = AccessTokens::get_token_list(self)?;
        for token in tokens.iter_mut() {
            self.populate_token_last_access(token).await;
        }
        Ok(tokens)
    }

    async fn validate_token(
        &mut self,
        header: Option<&str>,
        client_ip: Option<IpAddr>,
    ) -> Result<Token, ReductError> {
        let value = parse_bearer_token(header.unwrap_or(""))?;

        if let Some(mut token) = self.auth_cache.get(&value).cloned() {
            if token.ttl.is_some() {
                self.populate_token_last_access(&mut token).await;
            }

            if let Err(err) = check_token_lifetime(&token) {
                self.auth_cache.remove(&value);
                return Err(err);
            }
            if let Err(err) = super::check_token_ip_allowlist(&token, client_ip) {
                self.auth_cache.remove(&value);
                return Err(err);
            }
            return Ok(token);
        }

        let mut token = self
            .repo
            .values()
            .find(|token| crate::auth::token_secret::verify_token_secret(&token.value, &value))
            .cloned()
            .ok_or_else(|| reduct_base::unauthorized!("Invalid token"))?;

        if token.ttl.is_some() {
            self.populate_token_last_access(&mut token).await;
        }

        check_token_lifetime(&token)?;
        super::check_token_ip_allowlist(&token, client_ip)?;
        self.auth_cache.insert(value, token.clone());
        Ok(token)
    }

    async fn validate_token_with_last_access(
        &mut self,
        header: Option<&str>,
        client_ip: Option<IpAddr>,
    ) -> Result<Token, ReductError> {
        let mut token = ManageTokens::validate_token(self, header, client_ip).await?;
        self.populate_token_last_access(&mut token).await;
        Ok(token)
    }

    async fn remove_token(&mut self, name: &str) -> Result<(), ReductError> {
        if let Some(token) = self.repo.get(name) {
            if token.is_provisioned {
                return Err(conflict!("Can't remove provisioned token '{}'", name));
            }
        }

        if self.repo.remove(name).is_none() {
            Err(not_found!("Token '{}' doesn't exist", name))
        } else {
            self.save_repo().await?;
            self.clear_auth_cache();
            Ok(())
        }
    }

    async fn rotate_token(&mut self, name: &str) -> Result<TokenCreateResponse, ReductError> {
        if name == INIT_TOKEN_NAME {
            return Err(conflict!("Can't rotate init token"));
        }

        let token = self
            .repo
            .get_mut(name)
            .ok_or_else(|| not_found!("Token '{}' doesn't exist", name))?;

        if token.is_provisioned {
            return Err(conflict!("Can't rotate provisioned token '{}'", name));
        }

        let created_at = DateTime::<Utc>::from(SystemTime::now());
        let value = {
            let mut rng = rand::rng();
            let value: String = (0..32)
                .map(|_| format!("{:x}", rng.random_range(0..16)))
                .collect();
            format!("{}-{}", name, value)
        };

        token.value = value.clone();
        token.created_at = created_at;

        self.save_repo().await?;

        Ok(TokenCreateResponse { value, created_at })
    }

    async fn remove_bucket_from_tokens(&mut self, bucket: &str) -> Result<(), ReductError> {
        for token in self.repo.values_mut() {
            if let Some(permissions) = &mut token.permissions {
                permissions.read.retain(|b| b != bucket);
                permissions.write.retain(|b| b != bucket);
            }
        }

        self.save_repo().await?;
        self.clear_auth_cache();
        Ok(())
    }

    async fn rename_bucket(&mut self, old_name: &str, new_name: &str) -> Result<(), ReductError> {
        for token in self.repo.values_mut() {
            if let Some(permissions) = &mut token.permissions {
                permissions
                    .read
                    .iter_mut()
                    .filter(|b| *b == old_name)
                    .for_each(|b| {
                        *b = new_name.to_string();
                    });

                permissions
                    .write
                    .iter_mut()
                    .filter(|b| *b == old_name)
                    .for_each(|b| {
                        *b = new_name.to_string();
                    });
            }
        }

        self.save_repo().await?;
        self.clear_auth_cache();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::token_repository::{BoxedTokenRepository, TokenRepositoryBuilder};
    use crate::auth::token_secret::{is_hashed_token_secret, verify_token_secret};

    use crate::cfg::Cfg;
    use reduct_base::{conflict, unauthorized, unprocessable_entity};
    use rstest::{fixture, rstest};
    use tempfile::tempdir;

    #[rstest]
    #[tokio::test]
    async fn test_init_token(#[future] repo: BoxedTokenRepository) {
        let mut repo = repo.await;
        let token = repo
            .validate_token(Some("Bearer init-token"), None)
            .await
            .unwrap();
        assert_eq!(token.name, "init-token");
        assert!(is_hashed_token_secret(&token.value));
        assert!(verify_token_secret(&token.value, "init-token"));
        assert!(token.is_provisioned);

        let token_list = repo.get_token_list().await.unwrap();
        assert_eq!(token_list.len(), 2);
        assert_eq!(token_list[0].name, "init-token");
    }

    mod create_token {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_create_empty_token(#[future] repo: BoxedTokenRepository) {
            let mut repo = repo.await;
            let token = repo
                .generate_token(
                    "",
                    TokenCreateRequest {
                        permissions: Permissions {
                            full_access: true,
                            read: vec![],
                            write: vec![],
                        },
                        expires_at: None,
                        ttl: None,
                        ip_allowlist: vec![],
                    },
                )
                .await;

            assert_eq!(
                token,
                Err(unprocessable_entity!("Token name can't be empty"))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_create_existing_token(#[future] repo: BoxedTokenRepository) {
            let mut repo = repo.await;
            let token = repo
                .generate_token(
                    "test",
                    TokenCreateRequest {
                        permissions: Permissions {
                            full_access: true,
                            read: vec![],
                            write: vec![],
                        },
                        expires_at: None,
                        ttl: None,
                        ip_allowlist: vec![],
                    },
                )
                .await;

            assert_eq!(token, Err(conflict!("Token 'test' already exists")));
        }

        #[rstest]
        #[tokio::test]
        async fn test_create_token(#[future] repo: BoxedTokenRepository) {
            let mut repo = repo.await;
            let token = repo
                .generate_token(
                    "test-1",
                    TokenCreateRequest {
                        permissions: Permissions {
                            full_access: true,
                            read: vec![],
                            write: vec![],
                        },
                        expires_at: None,
                        ttl: None,
                        ip_allowlist: vec![],
                    },
                )
                .await
                .unwrap();

            assert_eq!(token.value.len(), 39);
            assert_eq!(token.value, "test-1-".to_string() + &token.value[7..]);
            assert!(token.created_at.timestamp() > 0);
        }

        #[rstest]
        #[tokio::test]
        async fn test_create_token_with_expires_at(#[future] repo: BoxedTokenRepository) {
            let mut repo = repo.await;
            let expires_at = chrono::Utc::now() + chrono::Duration::days(5);
            let created = repo
                .generate_token(
                    "test-exp",
                    TokenCreateRequest {
                        permissions: Permissions::default(),
                        expires_at: Some(expires_at),
                        ttl: None,
                        ip_allowlist: vec![],
                    },
                )
                .await
                .unwrap();

            let token = repo.get_token("test-exp").await.unwrap();
            assert_eq!(token.expires_at, Some(expires_at));
            assert!(created.created_at.timestamp() > 0);
        }

        #[rstest]
        #[tokio::test]
        async fn test_create_token_with_past_expires_at(#[future] repo: BoxedTokenRepository) {
            let mut repo = repo.await;
            let token = repo
                .generate_token(
                    "test-exp",
                    TokenCreateRequest {
                        permissions: Permissions::default(),
                        expires_at: Some(chrono::Utc::now() - chrono::Duration::days(5)),
                        ttl: None,
                        ip_allowlist: vec![],
                    },
                )
                .await;

            assert_eq!(
                token.err().unwrap(),
                unprocessable_entity!("Token expiration date must not be in the past")
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_create_token_rejects_ttl_and_expires_at_together(
            #[future] repo: BoxedTokenRepository,
        ) {
            let mut repo = repo.await;
            let token = repo
                .generate_token(
                    "test-exp-ttl",
                    TokenCreateRequest {
                        permissions: Permissions::default(),
                        expires_at: Some(chrono::Utc::now() + chrono::Duration::days(1)),
                        ttl: Some(3600),
                        ip_allowlist: vec![],
                    },
                )
                .await;

            assert_eq!(
                token.err().unwrap(),
                unprocessable_entity!(
                    "Only one lifetime policy is allowed: either 'ttl' or 'expires_at'"
                )
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_create_token_persistent(path: PathBuf, init_token: &str) {
            let cfg = Cfg {
                api_token: init_token.to_string(),
                ..Default::default()
            };

            let mut repo = build_repo_at(&path, &cfg).await;
            repo.generate_token(
                "test",
                TokenCreateRequest {
                    permissions: Permissions {
                        full_access: true,
                        read: vec![],
                        write: vec![],
                    },
                    expires_at: None,
                    ttl: None,
                    ip_allowlist: vec![],
                },
            )
            .await
            .unwrap();

            let mut repo = build_repo_at(&path, &cfg).await;
            assert_eq!(repo.get_token("test").await.unwrap().name, "test");
        }

        #[rstest]
        #[tokio::test]
        async fn test_migrate_plaintext_token_on_startup(path: PathBuf, init_token: &str) {
            let cfg = Cfg {
                api_token: init_token.to_string(),
                ..Default::default()
            };

            let repo_path = path.join(TOKEN_REPO_FILE_NAME);
            let legacy_token = Token {
                name: "legacy".to_string(),
                value: "legacy-secret".to_string(),
                created_at: chrono::Utc::now(),
                permissions: Some(Permissions::default()),
                is_provisioned: false,
                expires_at: None,
                ttl: None,
                last_access: None,
                ip_allowlist: vec![],
                is_expired: false,
            };
            let mut buf = Vec::new();
            TokenRepo {
                tokens: vec![legacy_token.into()],
            }
            .encode(&mut buf)
            .unwrap();
            std::fs::write(repo_path, buf).unwrap();

            let mut repo = build_repo_at(&path, &cfg).await;
            let token = repo.get_token("legacy").await.unwrap().clone();
            assert!(is_hashed_token_secret(&token.value));
            assert!(verify_token_secret(&token.value, "legacy-secret"));

            let validated = repo
                .validate_token(Some("Bearer legacy-secret"), None)
                .await
                .unwrap();
            assert_eq!(validated.name, "legacy");
        }

        #[rstest]
        #[tokio::test]
        async fn test_create_token_expiry_persistent(path: PathBuf, init_token: &str) {
            let cfg = Cfg {
                api_token: init_token.to_string(),
                ..Default::default()
            };

            let mut repo = build_repo_at(&path, &cfg).await;
            let expires_at = chrono::Utc::now() + chrono::Duration::days(5);
            let created = repo
                .generate_token(
                    "test-exp-persistent",
                    TokenCreateRequest {
                        permissions: Permissions::default(),
                        expires_at: Some(expires_at),
                        ttl: None,
                        ip_allowlist: vec![],
                    },
                )
                .await
                .unwrap();

            let mut repo = build_repo_at(&path, &cfg).await;
            let token = repo.get_token("test-exp-persistent").await.unwrap();
            assert_eq!(token.expires_at, Some(expires_at));
            assert!(created.created_at.timestamp() > 0);
        }

        #[rstest]
        #[tokio::test]
        #[case("*", None)]
        #[case("$audit", None)]
        #[case("$system", None)]
        #[case("bucket_1", None)]
        #[case("bucket_2", None)]
        #[case("bucket-*", None)]
        #[case(
            "%!",
            Some(unprocessable_entity!(
                "Permission can contain only bucket names or wildcard '*', got '%!'"
            ))
        )]
        async fn test_create_token_check_format_read(
            #[future] repo: BoxedTokenRepository,
            #[case] bucket: &str,
            #[case] expected: Option<ReductError>,
        ) {
            let mut repo = repo.await;
            let token = repo
                .generate_token(
                    "test-1",
                    TokenCreateRequest {
                        permissions: Permissions {
                            full_access: true,
                            read: vec![bucket.to_string()],
                            write: vec![],
                        },
                        expires_at: None,
                        ttl: None,
                        ip_allowlist: vec![],
                    },
                )
                .await;

            assert_eq!(token.err(), expected);
        }

        #[rstest]
        #[tokio::test]
        #[case("*", None)]
        #[case("$audit", None)]
        #[case("$system", None)]
        #[case("bucket_1", None)]
        #[case("bucket_2", None)]
        #[case("bucket-*", None)]
        #[case(
            "%!",
            Some(unprocessable_entity!(
                "Permission can contain only bucket names or wildcard '*', got '%!'"
            ))
        )]
        async fn test_create_token_check_format_write(
            #[future] repo: BoxedTokenRepository,
            #[case] bucket: &str,
            #[case] expected: Option<ReductError>,
        ) {
            let mut repo = repo.await;
            let token = repo
                .generate_token(
                    "test-1",
                    TokenCreateRequest {
                        permissions: Permissions {
                            full_access: true,
                            read: vec![],
                            write: vec![bucket.to_string()],
                        },
                        expires_at: None,
                        ttl: None,
                        ip_allowlist: vec![],
                    },
                )
                .await;

            assert_eq!(token.err(), expected);
        }
    }

    mod find_token {
        use super::*;
        #[rstest]
        #[tokio::test]
        async fn test_find_by_name(#[future] repo: BoxedTokenRepository) {
            let mut repo = repo.await;
            let token = repo.get_token("test").await.unwrap();
            assert_eq!(token.name, "test");
            assert!(is_hashed_token_secret(&token.value));
        }

        #[rstest]
        #[tokio::test]
        async fn test_find_by_name_not_found(#[future] repo: BoxedTokenRepository) {
            let mut repo = repo.await;
            let token = repo.get_token("test-1").await;
            assert_eq!(token, Err(not_found!("Token 'test-1' doesn't exist")));
        }

        #[rstest]
        #[tokio::test]
        async fn test_update_token_persistent(path: PathBuf, init_token: &str) {
            let cfg = Cfg {
                api_token: init_token.to_string(),
                ..Default::default()
            };

            let mut repo = build_repo_at(&path, &cfg).await;
            repo.generate_token(
                "test",
                TokenCreateRequest {
                    permissions: Permissions {
                        full_access: true,
                        read: vec![],
                        write: vec![],
                    },
                    expires_at: None,
                    ttl: None,
                    ip_allowlist: vec![],
                },
            )
            .await
            .unwrap();

            let created_at = repo.get_token("test").await.unwrap().created_at;
            repo.update_token(Token {
                name: "test".to_string(),
                value: "updated-token".to_string(),
                created_at,
                permissions: Some(Permissions {
                    full_access: false,
                    read: vec!["bucket-1".to_string()],
                    write: vec![],
                }),
                is_provisioned: true,
                expires_at: None,
                ttl: None,
                last_access: None,
                ip_allowlist: vec![],
                is_expired: false,
            })
            .await
            .unwrap();

            let mut reloaded_repo = build_repo_at(&path, &cfg).await;
            let token = reloaded_repo.get_token("test").await.unwrap();
            assert!(is_hashed_token_secret(&token.value));
            assert!(verify_token_secret(&token.value, "updated-token"));
            assert!(token.is_provisioned);
            assert_eq!(
                token.permissions,
                Some(Permissions {
                    full_access: false,
                    read: vec!["bucket-1".to_string()],
                    write: vec![],
                })
            );
        }
    }

    mod token_list {
        use super::*;
        #[rstest]
        #[tokio::test]
        async fn test_get_token_list(#[future] repo: BoxedTokenRepository) {
            let mut repo = repo.await;
            let token_list = repo.get_token_list().await.unwrap();

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
    }

    mod validate_token {
        use super::*;
        #[rstest]
        #[tokio::test]
        async fn test_validate_token(#[future] repo: BoxedTokenRepository) {
            let mut repo = repo.await;
            let value = repo
                .generate_token(
                    "test-1",
                    TokenCreateRequest {
                        permissions: Permissions {
                            full_access: true,
                            read: vec!["bucket-1".to_string()],
                            write: vec!["bucket-2".to_string()],
                        },
                        expires_at: None,
                        ttl: None,
                        ip_allowlist: vec![],
                    },
                )
                .await
                .unwrap()
                .value;

            let token = repo
                .validate_token(Some(&format!("Bearer {}", value)), None)
                .await
                .unwrap();

            assert_eq!(
                token,
                Token {
                    name: "test-1".to_string(),
                    created_at: token.created_at.clone(),
                    value: token.value.clone(),
                    permissions: Some(Permissions {
                        full_access: true,
                        read: vec!["bucket-1".to_string()],
                        write: vec!["bucket-2".to_string()],
                    }),
                    is_provisioned: false,
                    expires_at: None,
                    ttl: None,
                    last_access: None,
                    ip_allowlist: vec![],
                    is_expired: false,
                }
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_validate_token_not_found(#[future] repo: BoxedTokenRepository) {
            let mut repo = repo.await;
            let token = repo
                .validate_token(Some("Bearer invalid-value"), None)
                .await;
            assert_eq!(token, Err(unauthorized!("Invalid token")));
        }

        #[rstest]
        #[tokio::test]
        async fn test_validate_token_expired(#[future] repo: BoxedTokenRepository) {
            let mut repo = repo.await;
            let value = repo
                .generate_token(
                    "test-expired",
                    TokenCreateRequest {
                        permissions: Permissions::default(),
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
                .value;

            let mut token = repo.get_token("test-expired").await.unwrap().clone();
            token.expires_at = Some(chrono::Utc::now() - chrono::Duration::seconds(1));
            repo.update_token(token).await.unwrap();

            let err = repo
                .validate_token(Some(&format!("Bearer {}", value)), None)
                .await
                .err()
                .unwrap();
            assert_eq!(err, unauthorized!("Token has expired"));
        }

        #[rstest]
        #[tokio::test]
        async fn test_validate_token_cache_invalidation_on_update(path: PathBuf, init_token: &str) {
            let cfg = Cfg {
                api_token: init_token.to_string(),
                ..Default::default()
            };

            let mut repo = build_repo_at(&path, &cfg).await;
            let old_value = repo
                .generate_token(
                    "cache-token",
                    TokenCreateRequest {
                        permissions: Permissions::default(),
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
                .value;

            repo.validate_token(Some(&format!("Bearer {}", old_value)), None)
                .await
                .unwrap();

            let mut token = repo.get_token("cache-token").await.unwrap().clone();
            token.value = "cache-token-new-secret".to_string();
            repo.update_token(token).await.unwrap();

            let err = repo
                .validate_token(Some(&format!("Bearer {}", old_value)), None)
                .await
                .err()
                .unwrap();
            assert_eq!(err, unauthorized!("Invalid token"));

            let token = repo
                .validate_token(Some("Bearer cache-token-new-secret"), None)
                .await
                .unwrap();
            assert_eq!(token.name, "cache-token");
        }
    }

    mod remove_token {
        use super::*;
        #[rstest]
        #[tokio::test]
        async fn test_remove_token(#[future] repo: BoxedTokenRepository) {
            let mut repo = repo.await;
            let token = repo.remove_token("test").await.unwrap();
            assert_eq!(token, ());
        }

        #[rstest]
        #[tokio::test]
        async fn test_remove_init_token(#[future] repo: BoxedTokenRepository) {
            let mut repo = repo.await;
            let token = repo.remove_token("init-token").await;
            assert_eq!(
                token,
                Err(conflict!("Can't remove provisioned token 'init-token'"))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_remove_token_not_found(#[future] repo: BoxedTokenRepository) {
            let mut repo = repo.await;
            let token = repo.remove_token("test-1").await;
            assert_eq!(token, Err(not_found!("Token 'test-1' doesn't exist")));
        }

        #[rstest]
        #[tokio::test]
        async fn test_remove_token_persistent(path: PathBuf, init_token: &str, cfg: Cfg) {
            let mut repo = build_repo_at(&path, &cfg).await;
            let _ = repo
                .generate_token(
                    init_token,
                    TokenCreateRequest {
                        permissions: Permissions::default(),
                        ..Default::default()
                    },
                )
                .await;
            repo.generate_token(
                "test",
                TokenCreateRequest {
                    permissions: Permissions {
                        full_access: true,
                        read: vec![],
                        write: vec![],
                    },
                    expires_at: None,
                    ttl: None,
                    ip_allowlist: vec![],
                },
            )
            .await
            .unwrap();

            repo.remove_token("test").await.unwrap();

            let mut repo = build_repo_at(&path, &cfg).await;
            let token = repo.get_token("test").await;

            assert_eq!(token, Err(not_found!("Token 'test' doesn't exist")));
        }

        #[rstest]
        #[tokio::test]
        async fn test_remove_provisioned_token(#[future] repo: BoxedTokenRepository) {
            let mut repo = repo.await;
            let mut token = repo.get_token("test").await.unwrap().clone();
            token.is_provisioned = true;
            repo.update_token(token).await.unwrap();

            let err = repo.remove_token("test").await.err().unwrap();
            assert_eq!(err, conflict!("Can't remove provisioned token 'test'"))
        }
    }

    mod rotate_token {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_rotate_token(#[future] repo: BoxedTokenRepository) {
            let mut repo = repo.await;
            let old_value = repo.get_token("test").await.unwrap().value.clone();

            let rotated = repo.rotate_token("test").await.unwrap();

            assert_ne!(rotated.value, old_value);
            assert!(rotated.value.starts_with("test-"));

            let token = repo.get_token("test").await.unwrap();
            assert_eq!(token.value, rotated.value);
            assert_eq!(token.created_at, rotated.created_at);
        }

        #[rstest]
        #[tokio::test]
        async fn test_rotate_token_not_found(#[future] repo: BoxedTokenRepository) {
            let mut repo = repo.await;
            let err = repo.rotate_token("missing").await.err().unwrap();
            assert_eq!(err, not_found!("Token 'missing' doesn't exist"));
        }

        #[rstest]
        #[tokio::test]
        async fn test_rotate_init_token(#[future] repo: BoxedTokenRepository) {
            let mut repo = repo.await;
            let err = repo.rotate_token(INIT_TOKEN_NAME).await.err().unwrap();
            assert_eq!(err, conflict!("Can't rotate init token"));
        }

        #[rstest]
        #[tokio::test]
        async fn test_rotate_provisioned_token(#[future] repo: BoxedTokenRepository) {
            let mut repo = repo.await;
            let mut token = repo.get_token("test").await.unwrap().clone();
            token.is_provisioned = true;
            repo.update_token(token).await.unwrap();

            let err = repo.rotate_token("test").await.err().unwrap();
            assert_eq!(err, conflict!("Can't rotate provisioned token 'test'"));
        }
    }

    mod rename_bucket {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_rename_bucket(#[future] repo: BoxedTokenRepository) {
            let mut repo = repo.await;
            repo.generate_token(
                "test-2",
                TokenCreateRequest {
                    permissions: Permissions {
                        full_access: true,
                        read: vec!["bucket-1".to_string()],
                        write: vec!["bucket-1".to_string()],
                    },
                    expires_at: None,
                    ttl: None,
                    ip_allowlist: vec![],
                },
            )
            .await
            .expect("Failed to generate token");

            repo.rename_bucket("bucket-1", "bucket-2").await.unwrap();

            let token = repo.get_token("test-2").await.unwrap();
            let permissions = token.permissions.as_ref().unwrap();

            assert_eq!(permissions.read, vec!["bucket-2".to_string()]);
            assert_eq!(permissions.write, vec!["bucket-2".to_string()]);
        }

        #[rstest]
        #[tokio::test]
        async fn test_rename_bucket_not_exit(#[future] repo: BoxedTokenRepository) {
            let mut repo = repo.await;
            repo.rename_bucket("bucket-1", "bucket-2").await.unwrap();

            let token = repo.get_token("test").await.unwrap();
            let permissions = token.permissions.as_ref().unwrap();

            assert!(permissions.read.is_empty());
            assert!(permissions.write.is_empty());
        }

        #[rstest]
        #[tokio::test]
        async fn test_rename_bucket_persistent(path: PathBuf, init_token: &str, cfg: Cfg) {
            let mut repo = build_repo_at(&path, &cfg).await;
            let _ = repo
                .generate_token(
                    init_token,
                    TokenCreateRequest {
                        permissions: Permissions::default(),
                        ..Default::default()
                    },
                )
                .await;
            repo.generate_token(
                "test-2",
                TokenCreateRequest {
                    permissions: Permissions {
                        full_access: true,
                        read: vec!["bucket-1".to_string()],
                        write: vec!["bucket-1".to_string()],
                    },
                    expires_at: None,
                    ttl: None,
                    ip_allowlist: vec![],
                },
            )
            .await
            .expect("Failed to generate token");

            repo.rename_bucket("bucket-1", "bucket-2").await.unwrap();

            let mut repo = build_repo_at(&path, &cfg).await;
            let token = repo.get_token("test-2").await.unwrap();
            let permissions = token.permissions.as_ref().unwrap();

            assert_eq!(permissions.read, vec!["bucket-2".to_string()]);
            assert_eq!(permissions.write, vec!["bucket-2".to_string()]);
        }
    }

    #[fixture]
    fn path() -> PathBuf {
        tempdir().unwrap().keep()
    }

    #[fixture]
    fn init_token() -> &'static str {
        "init-token"
    }

    #[fixture]
    fn cfg(init_token: &str) -> Cfg {
        Cfg {
            api_token: init_token.to_string(),
            ..Default::default()
        }
    }

    #[fixture]
    async fn repo(path: PathBuf, cfg: Cfg) -> BoxedTokenRepository {
        let mut repo = build_repo_at(&path, &cfg).await;

        let _ = repo
            .generate_token(
                cfg.api_token.as_str(),
                TokenCreateRequest {
                    permissions: Permissions {
                        full_access: true,
                        read: vec![],
                        write: vec![],
                    },
                    expires_at: None,
                    ttl: None,
                    ip_allowlist: vec![],
                },
            )
            .await;

        let _ = repo
            .generate_token(
                "test",
                TokenCreateRequest {
                    permissions: Permissions {
                        full_access: true,
                        read: vec![],
                        write: vec![],
                    },
                    expires_at: None,
                    ttl: None,
                    ip_allowlist: vec![],
                },
            )
            .await;

        repo
    }

    async fn build_repo_at(path: &PathBuf, cfg: &Cfg) -> BoxedTokenRepository {
        TokenRepositoryBuilder::new(cfg.clone())
            .build(path.clone())
            .await
    }
}
