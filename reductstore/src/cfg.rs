// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

/*
 let host = env.get::<String>("RS_HOST", "0.0.0.0".to_string(), false);
   let port = env.get::<i32>("RS_PORT", 8383, false);
   let api_base_path = env.get::<String>("RS_API_BASE_PATH", "/".to_string(), false);
   let data_path = env.get::<String>("RS_DATA_PATH", "/data".to_string(), false);
   let api_token = env.get::<String>("RS_API_TOKEN", "".to_string(), true);
   let cert_path = env.get::<String>("RS_CERT_PATH", "".to_string(), true);
   let cert_key_path = env.get::<String>("RS_CERT_KEY_PATH", "".to_string(), true);
*/
use crate::core::env::Env;
use log::{log, warn};
use reduct_base::msg::bucket_api::{BucketSettings, QuotaType};
use reduct_base::msg::token_api::{Permissions, Token};
use std::collections::HashMap;
use std::str::FromStr;

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
    pub buckets: HashMap<String, Bucket>,
    pub tokens: HashMap<String, Token>,
}

/// Provisioned bucket
pub struct Bucket {
    pub name: String,
    pub settings: BucketSettings,
}

impl Cfg {
    pub fn from_env(env: &mut Env) -> Self {
        let mut cfg = Cfg {
            log_level: env.get("RS_LOG_LEVEL", "INFO".to_string()),
            host: env.get("RS_HOST", "0.0.0.0".to_string()),
            port: env.get("RS_PORT", 8383),
            api_base_path: env.get("RS_API_BASE_PATH", "/".to_string()),
            data_path: env.get("RS_DATA_PATH", "/data".to_string()),
            api_token: env.get_masked("RS_API_TOKEN", "".to_string()),
            cert_path: env.get_masked("RS_CERT_PATH", "".to_string()),
            cert_key_path: env.get_masked("RS_CERT_KEY_PATH", "".to_string()),
            buckets: Self::parse_buckets(env),
            tokens: Self::parse_tokens(env),
        };

        cfg
    }

    fn parse_buckets(env: &mut Env) -> HashMap<String, Bucket> {
        let mut buckets = HashMap::<String, (String, BucketSettings)>::new();
        for (id, name) in env.matches("RS_BUCKET_(.*)_NAME") {
            buckets.insert(id, (name, BucketSettings::default()));
        }

        for bucket in buckets.values_mut() {
            bucket.1.quota_type = env.get_optional(&format!("RS_BUCKET_{}_QUOTA_TYPE", bucket.0));
            bucket.1.quota_size = env.get_optional(&format!("RS_BUCKET_{}_QUOTA_SIZE", bucket.0));
            bucket.1.max_block_size =
                env.get_optional(&format!("RS_BUCKET_{}_MAX_BLOCK_SIZE", bucket.0));
            bucket.1.max_block_records =
                env.get_optional(&format!("RS_BUCKET_{}_MAX_BLOCK_RECORDS", bucket.0));
        }
        // parse_settings!("BUCKET", quota_type, bucket, "QUOTA_TYPE", QuotaType);
        // parse_settings!("BUCKET", quota_size, bucket, "QUOTA_SIZE", u64);
        // parse_settings!("BUCKET", max_block_size, bucket, "MAX_BLOCK_SIZE", u64);
        // parse_settings!(
        //     "BUCKET",
        //     max_block_records,
        //     buckets,
        //     "MAX_BLOCK_RECORDS",
        //     u64
        // );

        buckets
            .into_iter()
            .map(|(id, (name, settings))| (id, Bucket { name, settings }))
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
