// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::asset::asset_manager::ZipAssetManager;
use crate::auth::token_auth::TokenAuthorization;
use crate::auth::token_repository::create_token_repository;
use crate::core::env::Env;
use crate::http_frontend::Componentes;
use crate::storage::bucket::Bucket;
use crate::storage::storage::Storage;
use bytesize::ByteSize;
use futures_util::future::err;
use log::{error, info, log, warn};
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::msg::bucket_api::{BucketSettings, QuotaType};
use reduct_base::msg::token_api::{Permissions, Token};
use std::collections::HashMap;
use std::fmt::{Display, Formatter, Pointer};
use std::path::PathBuf;
use std::str::FromStr;
use tokio::sync::RwLock;

/// Database configuration
pub struct Cfg {
    pub log_level: String,
    pub host: String,
    pub port: i32,
    pub api_base_path: String,
    pub data_path: String,
    pub api_token: String,
    pub cert_path: String,
    pub cert_key_path: String,
    pub buckets: HashMap<String, BucketSettings>,
    pub tokens: HashMap<String, Token>,

    env: Env,
}

impl Cfg {
    pub fn from_env() -> Self {
        let mut env = Env::new();
        let mut cfg = Cfg {
            log_level: env.get("RS_LOG_LEVEL", "INFO".to_string()),
            host: env.get("RS_HOST", "0.0.0.0".to_string()),
            port: env.get("RS_PORT", 8383),
            api_base_path: env.get("RS_API_BASE_PATH", "/".to_string()),
            data_path: env.get("RS_DATA_PATH", "/data".to_string()),
            api_token: env.get_masked("RS_API_TOKEN", "".to_string()),
            cert_path: env.get_masked("RS_CERT_PATH", "".to_string()),
            cert_key_path: env.get_masked("RS_CERT_KEY_PATH", "".to_string()),
            buckets: Self::parse_buckets(&mut env),
            tokens: Self::parse_tokens(&mut env),

            env,
        };

        cfg
    }

    pub async fn build(&self) -> Componentes {
        let storage = self.provision_storage().await;
        let token_repo = RwLock::new(create_token_repository(
            PathBuf::from(self.data_path.clone()),
            &self.api_token,
        ));

        for (name, token) in &self.tokens {
            let mut token_repo = token_repo.write().await;
            let permissions = match token_repo
                .generate_token(&name, token.permissions.clone().unwrap_or_default())
            {
                Ok(_) => {
                    // replace the generated token
                    token_repo.get_mut_token(&name).unwrap().clone_from(token);
                    Ok(token.permissions.clone().unwrap_or_default())
                }
                Err(e) => {
                    if e.status() == ErrorCode::Conflict {
                        // replace the existing token
                        token_repo.get_mut_token(&name).unwrap().clone_from(token);
                        Ok(token.permissions.clone().unwrap_or_default())
                    } else {
                        Err(e)
                    }
                }
            };

            if let (Ok(permissions)) = permissions {
                info!("Provisioned token '{}' with {:?}", token.name, permissions);
            } else {
                error!(
                    "Failed to provision token '{}': {}",
                    name,
                    permissions.err().unwrap()
                );
            }
        }

        Componentes {
            storage,
            token_repo,
            auth: TokenAuthorization::new(&self.api_token),
            console: ZipAssetManager::new(include_bytes!(concat!(env!("OUT_DIR"), "/console.zip"))),
            base_path: self.api_base_path.clone(),
        }
    }

    async fn provision_storage(&self) -> RwLock<Storage> {
        let mut storage = RwLock::new(Storage::new(PathBuf::from(self.data_path.clone())));
        for (name, settings) in &self.buckets {
            let mut storage = storage.write().await;
            let settings = match storage.create_bucket(&name, settings.clone()) {
                Ok(bucket) => Ok(bucket.settings().clone()),
                Err(e) => {
                    if e.status() == ErrorCode::Conflict {
                        let mut bucket = storage.get_mut_bucket(&name).unwrap();
                        bucket.set_settings(settings.clone()).unwrap();
                        Ok(bucket.settings().clone())
                    } else {
                        Err(e)
                    }
                }
            };

            if let (Ok(settings)) = settings {
                info!("Provisioned bucket '{}' with: {:?}", name, settings);
            } else {
                error!(
                    "Failed to provision bucket '{}': {}",
                    name,
                    settings.err().unwrap()
                );
            }
        }
        storage
    }

    fn parse_buckets(env: &mut Env) -> HashMap<String, BucketSettings> {
        let mut buckets = HashMap::<String, (String, BucketSettings)>::new();
        for (id, name) in env.matches("RS_BUCKET_(.*)_NAME") {
            buckets.insert(id, (name, BucketSettings::default()));
        }

        for (id, bucket) in &mut buckets {
            let mut settings = &mut bucket.1;
            settings.quota_type = env.get_optional(&format!("RS_BUCKET_{}_QUOTA_TYPE", id));

            settings.quota_size = env
                .get_optional::<ByteSize>(&format!("RS_BUCKET_{}_QUOTA_SIZE", id))
                .map(|s| s.as_u64());
            settings.max_block_size = env
                .get_optional::<ByteSize>(&format!("RS_BUCKET_{}_MAX_BLOCK_SIZE", id))
                .map(|s| s.as_u64());
            settings.max_block_records =
                env.get_optional(&format!("RS_BUCKET_{}_MAX_BLOCK_RECORDS", id));
        }

        buckets
            .into_iter()
            .map(|(id, (name, settings))| (name, settings))
            .collect()
    }

    fn parse_tokens(env: &mut Env) -> HashMap<String, Token> {
        let mut tokens = HashMap::<String, Token>::new();
        for (id, name) in env.matches("RS_TOKEN_(.*)_NAME") {
            let token = Token {
                name,
                created_at: chrono::Utc::now(),
                ..Token::default()
            };
            tokens.insert(id, token);
        }

        for (id, token) in &mut tokens {
            token.value =
                env.get_masked::<String>(&format!("RS_TOKEN_{}_VALUE", id), "".to_string());
        }

        tokens.retain(|_, token| {
            if token.value.is_empty() {
                warn!("Token '{}' has no value. Drop it.", token.name);
                false
            } else {
                true
            }
        });

        let mut parse_list_env = |env: &mut Env, name: String| -> Vec<String> {
            env.get_optional::<String>(&name)
                .unwrap_or_default()
                .split(",")
                .map(|s| s.to_string())
                .collect()
        };

        // Parse permissions
        for (id, token) in &mut tokens {
            let read = parse_list_env(env, format!("RS_TOKEN_{}_READ", id));
            let write = parse_list_env(env, format!("RS_TOKEN_{}_WRITE", id));
            let permissions = Permissions {
                full_access: env
                    .get_optional(&format!("RS_TOKEN_{}_FULL_ACCESS", id))
                    .unwrap_or_default(),
                read,
                write,
            };

            token.permissions = Some(permissions);
        }

        tokens
    }
}

impl Display for Cfg {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.env.message())
    }
}
