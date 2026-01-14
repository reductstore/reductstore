// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::auth::proto::TokenRepo;
use crate::auth::token_repository::AccessTokens;
use crate::auth::token_repository::{ManageTokens, INIT_TOKEN_NAME, TOKEN_REPO_FILE_NAME};
use crate::core::file_cache::FILE_CACHE;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use log::debug;
use prost::Message;
use rand::Rng;
use reduct_base::error::ReductError;
use reduct_base::msg::token_api::{Permissions, Token, TokenCreateResponse};
use reduct_base::{conflict, not_found, unprocessable_entity};
use regex::Regex;
use std::collections::HashMap;
use std::io::{Read, SeekFrom, Write};
use std::path::PathBuf;
use std::time::SystemTime;

/// The TokenRepository trait is used to store and retrieve tokens.
pub(super) struct TokenRepository {
    config_path: PathBuf,
    repo: HashMap<String, Token>,
    permission_regex: Regex,
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
    pub async fn new(data_path: PathBuf, api_token: String) -> TokenRepository {
        let config_path = data_path.join(TOKEN_REPO_FILE_NAME);
        let repo = HashMap::new();

        // Load the token repository from the file system
        let permission_regex =
            Regex::new(r"^[*a-zA-Z0-9_\-]+$").expect("Invalid regex for permissions");
        let mut token_repository = TokenRepository {
            config_path,
            repo,
            permission_regex,
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

        let init_token = Token {
            name: INIT_TOKEN_NAME.to_string(),
            value: api_token.to_string(),
            created_at: DateTime::<Utc>::from(SystemTime::now()),
            permissions: Some(Permissions {
                full_access: true,
                read: vec![],
                write: vec![],
            }),
            is_provisioned: true,
        };

        token_repository
            .repo
            .insert(init_token.name.clone(), init_token);
        token_repository
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
        permissions: Permissions,
    ) -> Result<TokenCreateResponse, ReductError> {
        // Check if the token isn't empty
        if name.is_empty() {
            return Err(unprocessable_entity!("Token name can't be empty"));
        }

        // Check if the token already exists
        if self.repo.contains_key(name) {
            return Err(conflict!("Token '{}' already exists", name));
        }

        for entry in permissions.read.iter().chain(&permissions.write) {
            if !self.permission_regex.is_match(entry) {
                return Err(unprocessable_entity!(
                    "Permission can contain only bucket names or wildcard '*', got '{}'",
                    entry
                ));
            }
        }

        let created_at = DateTime::<Utc>::from(SystemTime::now());

        // Create a random hex string
        let (value, token) = {
            let mut rng = rand::rng();
            let value: String = (0..32)
                .map(|_| format!("{:x}", rng.random_range(0..16)))
                .collect();
            let value = format!("{}-{}", name, value);
            (
                value.clone(),
                Token {
                    name: name.to_string(),
                    value,
                    created_at: created_at.clone(),
                    permissions: Some(permissions),
                    is_provisioned: false,
                },
            )
        };

        self.repo.insert(name.to_string(), token);
        self.save_repo().await?;

        Ok(TokenCreateResponse { value, created_at })
    }

    async fn get_token(&mut self, name: &str) -> Result<&Token, ReductError> {
        AccessTokens::get_token(self, name)
    }

    async fn get_mut_token(&mut self, name: &str) -> Result<&mut Token, ReductError> {
        match self.repo.get_mut(name) {
            Some(token) => Ok(token),
            None => Err(not_found!("Token '{}' doesn't exist", name)),
        }
    }

    async fn get_token_list(&mut self) -> Result<Vec<Token>, ReductError> {
        AccessTokens::get_token_list(self)
    }

    async fn validate_token(&mut self, header: Option<&str>) -> Result<Token, ReductError> {
        AccessTokens::validate_token(self, header)
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
            self.save_repo().await
        }
    }

    async fn remove_bucket_from_tokens(&mut self, bucket: &str) -> Result<(), ReductError> {
        for token in self.repo.values_mut() {
            if let Some(permissions) = &mut token.permissions {
                permissions.read.retain(|b| b != bucket);
                permissions.write.retain(|b| b != bucket);
            }
        }

        self.save_repo().await
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

        self.save_repo().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::token_repository::{BoxedTokenRepository, TokenRepositoryBuilder};
    use crate::backend::Backend;
    use crate::cfg::Cfg;
    use reduct_base::{conflict, unauthorized, unprocessable_entity};
    use rstest::{fixture, rstest};
    use tempfile::tempdir;

    #[rstest]
    #[tokio::test]
    async fn test_init_token(#[future] repo: BoxedTokenRepository) {
        let mut repo = repo.await;
        let token = repo
            .validate_token(Some("Bearer init-token"))
            .await
            .unwrap();
        assert_eq!(token.name, "init-token");
        assert_eq!(token.value, "init-token");
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
                    Permissions {
                        full_access: true,
                        read: vec![],
                        write: vec![],
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
                    Permissions {
                        full_access: true,
                        read: vec![],
                        write: vec![],
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
                    Permissions {
                        full_access: true,
                        read: vec![],
                        write: vec![],
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
        async fn test_create_token_persistent(path: PathBuf, init_token: &str) {
            let cfg = Cfg {
                api_token: init_token.to_string(),
                ..Default::default()
            };

            let mut repo = build_repo_at(&path, &cfg).await;
            repo.generate_token(
                "test",
                Permissions {
                    full_access: true,
                    read: vec![],
                    write: vec![],
                },
            )
            .await
            .unwrap();

            let mut repo = build_repo_at(&path, &cfg).await;
            assert_eq!(repo.get_token("test").await.unwrap().name, "test");
        }

        #[rstest]
        #[tokio::test]
        #[case("*", None)]
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
                    Permissions {
                        full_access: true,
                        read: vec![bucket.to_string()],
                        write: vec![],
                    },
                )
                .await;

            assert_eq!(token.err(), expected);
        }

        #[rstest]
        #[tokio::test]
        #[case("*", None)]
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
                    Permissions {
                        full_access: true,
                        read: vec![],
                        write: vec![bucket.to_string()],
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
            assert!(token.value.starts_with("test-"));
        }

        #[rstest]
        #[tokio::test]
        async fn test_find_by_name_not_found(#[future] repo: BoxedTokenRepository) {
            let mut repo = repo.await;
            let token = repo.get_token("test-1").await;
            assert_eq!(token, Err(not_found!("Token 'test-1' doesn't exist")));
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
                    Permissions {
                        full_access: true,
                        read: vec!["bucket-1".to_string()],
                        write: vec!["bucket-2".to_string()],
                    },
                )
                .await
                .unwrap()
                .value;

            let token = repo
                .validate_token(Some(&format!("Bearer {}", value)))
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
                }
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_validate_token_not_found(#[future] repo: BoxedTokenRepository) {
            let mut repo = repo.await;
            let token = repo.validate_token(Some("Bearer invalid-value")).await;
            assert_eq!(token, Err(unauthorized!("Invalid token")));
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
                .generate_token(init_token, Permissions::default())
                .await;
            repo.generate_token(
                "test",
                Permissions {
                    full_access: true,
                    read: vec![],
                    write: vec![],
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
            let token = repo.get_mut_token("test").await.unwrap();
            token.is_provisioned = true;

            let err = repo.remove_token("test").await.err().unwrap();
            assert_eq!(err, conflict!("Can't remove provisioned token 'test'"))
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
                Permissions {
                    full_access: true,
                    read: vec!["bucket-1".to_string()],
                    write: vec!["bucket-1".to_string()],
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
                .generate_token(init_token, Permissions::default())
                .await;
            repo.generate_token(
                "test-2",
                Permissions {
                    full_access: true,
                    read: vec!["bucket-1".to_string()],
                    write: vec!["bucket-1".to_string()],
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
                Permissions {
                    full_access: true,
                    read: vec![],
                    write: vec![],
                },
            )
            .await;

        let _ = repo
            .generate_token(
                "test",
                Permissions {
                    full_access: true,
                    read: vec![],
                    write: vec![],
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
