// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::auth::proto::TokenRepo;
use crate::auth::token_repository::AccessTokens;
use crate::auth::token_repository::{ManageTokens, INIT_TOKEN_NAME, TOKEN_REPO_FILE_NAME};
use crate::cfg::{Cfg, InstanceRole};
use crate::core::file_cache::FILE_CACHE;
use crate::core::sync::RwLock;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use log::{debug, error};
use prost::Message;
use reduct_base::error::ReductError;
use reduct_base::msg::token_api::{Permissions, Token, TokenCreateResponse};
use reduct_base::{forbidden, internal_server_error};
use std::collections::HashMap;
use std::io::{Read, SeekFrom};
use std::path::PathBuf;
use std::time::SystemTime;
use tokio::time::Instant;

pub(super) struct ReadOnlyTokenRepository {
    config_path: PathBuf,
    repo: HashMap<String, Token>,
    cfg: Cfg,
    last_replica_sync: RwLock<Instant>,
}

impl ReadOnlyTokenRepository {
    /// Load the token repository from the given path in read-only mode
    ///
    /// # Arguments
    ///
    /// * `data_path` - The path to the data directory
    /// * `cfg` - The configuration
    ///
    /// # Returns
    ///
    /// The repository
    pub fn new(data_path: PathBuf, cfg: Cfg) -> Self {
        let config_path = data_path.join(TOKEN_REPO_FILE_NAME);

        let mut token_repository = Self {
            config_path,
            repo: HashMap::new(),
            last_replica_sync: RwLock::new(Instant::now()),
            cfg,
        };
        let repo = token_repository
            .load_repo()
            .expect("Could not load token repository");

        token_repository.repo = repo;
        token_repository
    }

    fn load_repo(&self) -> Result<HashMap<String, Token>, ReductError> {
        let api_token = self.cfg.api_token.clone();

        FILE_CACHE.discard_recursive(&self.config_path)?; // ensure we update it from backend
        let lock = FILE_CACHE.read(&self.config_path, SeekFrom::Start(0));
        let mut repo = HashMap::new();
        match lock {
            Ok(lock) => {
                debug!(
                    "Loading token repository from {}",
                    self.config_path.as_path().display()
                );

                let mut buf = Vec::new();
                lock.upgrade()?.write()?.read_to_end(&mut buf)?;

                let proto_repo = TokenRepo::decode(&mut Bytes::from(buf)).map_err(|e| {
                    internal_server_error!("Could not decode token repository: {}", e)
                })?;

                for token in proto_repo.tokens {
                    repo.insert(token.name.clone(), token.into());
                }
            }
            Err(_) => error!("Token repository not found at {:?}", self.config_path),
        };

        if !api_token.is_empty() {
            let init_token = Token {
                name: INIT_TOKEN_NAME.to_string(),
                value: api_token.to_string(),
                created_at: DateTime::<Utc>::from(SystemTime::now()),
                permissions: Some(Permissions {
                    full_access: false,
                    read: vec!["*".to_string()],
                    write: vec![],
                }),
                is_provisioned: true,
            };

            repo.insert(init_token.name.clone(), init_token);
        }

        Ok(repo)
    }

    fn update_repo(&mut self) -> Result<(), ReductError> {
        let mut last_sync = self.last_replica_sync.write()?;
        if self.cfg.role != InstanceRole::Replica
            || last_sync.elapsed() < self.cfg.engine_config.replica_update_interval
        {
            // Only read-only instances need to update bucket list from backend
            return Ok(());
        }

        *last_sync = Instant::now();
        self.repo = self.load_repo()?;

        Ok(())
    }
}

impl AccessTokens for ReadOnlyTokenRepository {
    fn repo(&self) -> &HashMap<String, Token> {
        &self.repo
    }
}

impl ManageTokens for ReadOnlyTokenRepository {
    fn generate_token(
        &mut self,
        _name: &str,
        _permissions: Permissions,
    ) -> Result<TokenCreateResponse, ReductError> {
        Err(forbidden!("Cannot generate token in read-only mode"))
    }

    fn get_token(&mut self, name: &str) -> Result<&Token, ReductError> {
        self.update_repo()?;
        AccessTokens::get_token(self, name)
    }

    fn get_mut_token(&mut self, _name: &str) -> Result<&mut Token, ReductError> {
        Err(forbidden!("Cannot mutate token in read-only mode"))
    }

    fn get_token_list(&mut self) -> Result<Vec<Token>, ReductError> {
        self.update_repo()?;

        AccessTokens::get_token_list(self)
    }

    fn validate_token(&mut self, header: Option<&str>) -> Result<Token, ReductError> {
        self.update_repo()?;

        AccessTokens::validate_token(self, header)
    }

    fn remove_token(&mut self, _name: &str) -> Result<(), ReductError> {
        Err(forbidden!("Cannot remove token in read-only mode"))
    }

    fn remove_bucket_from_tokens(&mut self, _bucket: &str) -> Result<(), ReductError> {
        Err(forbidden!(
            "Cannot remove bucket from token in read-only mode"
        ))
    }

    fn rename_bucket(&mut self, _old_name: &str, _new_name: &str) -> Result<(), ReductError> {
        Err(forbidden!(
            "Cannot rename bucket in token in read-only mode"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::token_repository::{BoxedTokenRepository, INIT_TOKEN_NAME};
    use crate::backend::Backend;
    use crate::cfg::{Cfg, InstanceRole};
    use reduct_base::msg::token_api::Permissions;
    use rstest::{fixture, rstest};
    use std::path::PathBuf;
    use tempfile::tempdir;

    mod repo_methods {
        use super::*;
        use std::io::Write;
        use std::thread::sleep;
        use std::time::Duration;

        #[rstest]
        fn test_new_loads_tokens(mut repo: BoxedTokenRepository) {
            let token_list = repo.get_token_list().unwrap();
            assert_eq!(token_list.len(), 2); // one from file, one from cfg

            assert!(token_list.iter().any(|t| t.name == "file_token"));
            assert!(token_list.iter().any(|t| t.name == INIT_TOKEN_NAME));

            #[rstest]
            fn test_load_repo_with_api_token(mut repo: BoxedTokenRepository, cfg_fixture: Cfg) {
                let token = repo.get_token(INIT_TOKEN_NAME).unwrap();
                assert_eq!(token.value, cfg_fixture.api_token);
                assert!(token.is_provisioned);
            }

            #[rstest]
            fn test_reload_from_from_file(repo: BoxedTokenRepository, path: PathBuf) {
                let mut repo = repo;

                // Modify the token file to add a new token
                let new_token = Token {
                    name: "new_file_token".to_string(),
                    value: "new_file_value".to_string(),
                    created_at: DateTime::<Utc>::from(SystemTime::now()),
                    permissions: Some(Permissions {
                        full_access: true,
                        read: vec![],
                        write: vec![],
                    }),
                    is_provisioned: true,
                };

                write_token_to_file(&path, &new_token);

                // Force reload
                sleep(Duration::from_millis(200));

                // Check if the new token is loaded
                assert!(repo.get_token("new_file_token").is_ok());
            }
        }

        mod manage_tokens {
            use super::*;
            use reduct_base::{not_found, unauthorized};

            #[rstest]
            fn test_generate_token_forbidden(mut repo: BoxedTokenRepository) {
                let perms = Permissions {
                    full_access: true,
                    read: vec![],
                    write: vec![],
                };
                let res = repo.generate_token("test", perms);
                assert_eq!(
                    res.err().unwrap(),
                    forbidden!("Cannot generate token in read-only mode")
                );
            }

            #[rstest]
            fn test_get_token_existing(mut repo: BoxedTokenRepository, cfg_fixture: Cfg) {
                let token = repo.get_token(INIT_TOKEN_NAME).unwrap().clone();
                assert_eq!(
                    token,
                    Token {
                        name: INIT_TOKEN_NAME.to_string(),
                        value: cfg_fixture.api_token.clone(),
                        created_at: token.created_at,
                        permissions: Some(Permissions {
                            full_access: false,
                            read: vec!["*".to_string()],
                            write: vec![],
                        }),
                        is_provisioned: true,
                    }
                );
            }

            #[rstest]
            fn test_get_token_missing(mut repo: BoxedTokenRepository) {
                let token = repo.get_token("missing");
                assert_eq!(
                    token.err().unwrap(),
                    not_found!("Token 'missing' doesn't exist")
                );
            }

            #[rstest]
            fn test_get_mut_token_forbidden(mut repo: BoxedTokenRepository) {
                let res = repo.get_mut_token(INIT_TOKEN_NAME);
                assert_eq!(
                    res.err().unwrap(),
                    forbidden!("Cannot mutate token in read-only mode")
                );
            }

            #[rstest]
            fn test_get_token_list(mut repo: BoxedTokenRepository) {
                let list = repo.get_token_list().unwrap();
                assert_eq!(list.len(), 2);

                assert!(list.iter().any(|t| t.name == "file_token"));
                assert!(list.iter().any(|t| t.name == INIT_TOKEN_NAME));
            }

            #[rstest]
            fn test_validate_token_valid(mut repo: BoxedTokenRepository, cfg_fixture: Cfg) {
                let header = format!("Bearer {}", cfg_fixture.api_token);
                let res = repo.validate_token(Some(header.as_str())).unwrap();
                assert_eq!(
                    res,
                    Token {
                        name: INIT_TOKEN_NAME.to_string(),
                        value: cfg_fixture.api_token.clone(),
                        created_at: res.created_at,
                        permissions: Some(Permissions {
                            full_access: false,
                            read: vec!["*".to_string()],
                            write: vec![],
                        }),
                        is_provisioned: true,
                    }
                );
            }

            #[rstest]
            fn test_validate_token_invalid(mut repo: BoxedTokenRepository) {
                let header = Some("Bearer invalid_token");
                let res = repo.validate_token(header);
                assert_eq!(res.err().unwrap(), unauthorized!("Invalid token"));
            }

            #[rstest]
            fn test_remove_token_forbidden(mut repo: BoxedTokenRepository) {
                let res = repo.remove_token(INIT_TOKEN_NAME);
                assert_eq!(
                    res.err().unwrap(),
                    forbidden!("Cannot remove token in read-only mode")
                );
            }

            #[rstest]
            fn test_remove_bucket_from_tokens_forbidden(mut repo: BoxedTokenRepository) {
                let res = repo.remove_bucket_from_tokens("bucket");
                assert_eq!(
                    res.err().unwrap(),
                    forbidden!("Cannot remove bucket from token in read-only mode")
                );
            }

            #[rstest]
            fn test_rename_bucket_forbidden(mut repo: BoxedTokenRepository) {
                let res = repo.rename_bucket("old", "new");
                assert_eq!(
                    res.err().unwrap(),
                    forbidden!("Cannot rename bucket in token in read-only mode")
                );
            }
        }

        // Fixtures and helpers
        #[fixture]
        fn path() -> PathBuf {
            tempdir().unwrap().keep()
        }

        #[fixture]
        fn cfg_fixture() -> Cfg {
            let mut cfg = Cfg::default();
            cfg.api_token = "test_token".to_string();
            cfg.role = InstanceRole::Replica;
            cfg.engine_config.replica_update_interval = std::time::Duration::from_millis(100);
            cfg
        }

        #[fixture]
        fn repo(cfg_fixture: Cfg, path: PathBuf) -> BoxedTokenRepository {
            FILE_CACHE.set_storage_backend(
                Backend::builder()
                    .local_data_path(path.clone())
                    .try_build()
                    .unwrap(),
            );

            let token = Token {
                name: "file_token".to_string(),
                value: "file_value".to_string(),
                created_at: DateTime::<Utc>::from(SystemTime::now()),
                permissions: Some(Permissions {
                    full_access: true,
                    read: vec![],
                    write: vec![],
                }),
                is_provisioned: true,
            };

            write_token_to_file(&path, &token);
            Box::new(ReadOnlyTokenRepository::new(path, cfg_fixture))
        }

        // Helper to write a token to the token repo file
        fn write_token_to_file(path: &PathBuf, new_token: &Token) {
            let mut token_repo = TokenRepo::default();
            token_repo.tokens.push(new_token.clone().into());
            let mut buf = Vec::new();
            token_repo.encode(&mut buf).unwrap();

            FILE_CACHE
                .write_or_create(&path.join(TOKEN_REPO_FILE_NAME), SeekFrom::Start(0))
                .unwrap()
                .upgrade()
                .unwrap()
                .write()
                .unwrap()
                .write_all(&buf)
                .unwrap();
        }
    }
}
