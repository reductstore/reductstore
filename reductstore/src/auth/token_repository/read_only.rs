// Copyright 2023-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::auth::proto::TokenRepo;
use crate::auth::token_repository::repo::TokenRepository;
use crate::auth::token_repository::TokenRepoCommon;
use crate::auth::token_repository::{
    parse_bearer_token, ManageTokens, INIT_TOKEN_NAME, TOKEN_REPO_FILE_NAME,
};
use crate::core::file_cache::FILE_CACHE;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use log::{debug, error};
use prost::Message;
use rand::Rng;
use reduct_base::error::ReductError;
use reduct_base::msg::token_api::{Permissions, Token, TokenCreateResponse};
use reduct_base::{conflict, forbidden, not_found, unauthorized, unprocessable_entity};
use regex::Regex;
use std::collections::HashMap;
use std::io::{Read, SeekFrom};
use std::path::PathBuf;
use std::time::SystemTime;

pub(super) struct ReadOnlyTokenRepository {
    config_path: PathBuf,
    repo: HashMap<String, Token>,
}

impl ReadOnlyTokenRepository {
    /// Load the token repository from the given path in read-only mode
    ///
    /// # Arguments
    ///
    /// * `data_path` - The path to the data directory
    /// * `api_token` - The API token with read access to all buckets
    ///
    /// # Returns
    ///
    /// The repository
    pub fn new(data_path: PathBuf, api_token: String) -> Self {
        let config_path = data_path.join(TOKEN_REPO_FILE_NAME);
        let repo = HashMap::new();

        let mut token_repository = Self { config_path, repo };

        let lock = FILE_CACHE.read(&token_repository.config_path, SeekFrom::Start(0));
        match lock {
            Ok(lock) => {
                debug!(
                    "Loading token repository from {}",
                    token_repository.config_path.as_path().display()
                );

                let mut buf = Vec::new();
                lock.upgrade()
                    .unwrap()
                    .write()
                    .unwrap()
                    .read_to_end(&mut buf)
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
                error!(
                    "Token repository not found at {}, creating",
                    token_repository.config_path.as_path().display()
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

            token_repository
                .repo
                .insert(init_token.name.clone(), init_token);
        }
        token_repository
    }
}

impl TokenRepoCommon for ReadOnlyTokenRepository {
    fn repo(&self) -> &std::collections::HashMap<String, Token> {
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

    fn get_token(&self, name: &str) -> Result<&Token, ReductError> {
        TokenRepoCommon::get_token(self, name)
    }

    fn get_mut_token(&mut self, name: &str) -> Result<&mut Token, ReductError> {
        Err(forbidden!("Cannot generate token in read-only mode"))
    }

    fn get_token_list(&self) -> Result<Vec<Token>, ReductError> {
        TokenRepoCommon::get_token_list(self)
    }

    fn validate_token(&self, header: Option<&str>) -> Result<Token, ReductError> {
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
