// Copyright 2023-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::auth::proto::TokenRepo;
use crate::auth::token_repository::TokenRepoCommon;
use crate::auth::token_repository::{ManageTokens, INIT_TOKEN_NAME, TOKEN_REPO_FILE_NAME};
use crate::cfg::{Cfg, InstanceRole};
use crate::core::file_cache::FILE_CACHE;
use crate::core::sync::RwLock;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use log::{debug, error};
use prost::Message;
use reduct_base::error::ReductError;
use reduct_base::forbidden;
use reduct_base::msg::token_api::{Permissions, Token, TokenCreateResponse};
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
                    ReductError::internal_server_error(&format!(
                        "Could not decode token repository: {}",
                        e
                    ))
                })?;
                for token in proto_repo.tokens {
                    repo.insert(token.name.clone(), token.into());
                }
            }
            Err(_) => {
                error!(
                    "Token repository not found at {}, creating",
                    self.config_path.as_path().display()
                );
            }
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
        if self.cfg.role != InstanceRole::ReadOnly
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

impl TokenRepoCommon for ReadOnlyTokenRepository {
    fn repo(&self) -> &HashMap<String, Token> {
        &self.repo
    }
}

impl ManageTokens for ReadOnlyTokenRepository {
    fn generate_token(
        &mut self,
        name: &str,
        permissions: Permissions,
    ) -> Result<TokenCreateResponse, ReductError> {
        Err(forbidden!("Cannot generate token in read-only mode"))
    }

    fn get_token(&mut self, name: &str) -> Result<&Token, ReductError> {
        self.update_repo()?;
        TokenRepoCommon::get_token(self, name)
    }

    fn get_mut_token(&mut self, name: &str) -> Result<&mut Token, ReductError> {
        Err(forbidden!("Cannot generate token in read-only mode"))
    }

    fn get_token_list(&mut self) -> Result<Vec<Token>, ReductError> {
        self.update_repo()?;

        TokenRepoCommon::get_token_list(self)
    }

    fn validate_token(&mut self, header: Option<&str>) -> Result<Token, ReductError> {
        self.update_repo()?;

        TokenRepoCommon::validate_token(self, header)
    }

    fn remove_token(&mut self, name: &str) -> Result<(), ReductError> {
        Err(forbidden!("Cannot generate token in read-only mode"))
    }

    fn remove_bucket_from_tokens(&mut self, bucket: &str) -> Result<(), ReductError> {
        Err(forbidden!("Cannot generate token in read-only mode"))
    }

    fn rename_bucket(&mut self, old_name: &str, new_name: &str) -> Result<(), ReductError> {
        Err(forbidden!("Cannot generate token in read-only mode"))
    }
}
