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
use reduct_base::msg::token_api::Permissions;
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

/// Provisioned token
pub struct Token {
    pub name: String,
    pub permissions: Permissions,
}

impl Cfg {
    pub fn from_env(env: &mut Env) -> Self {
        let mut cfg = Cfg {
            log_level: env.get("RS_LOG_LEVEL", "INFO".to_string(), false),
            host: env.get("RS_HOST", "0.0.0.0".to_string(), false),
            port: env.get("RS_PORT", 8383, false),
            api_base_path: env.get("RS_API_BASE_PATH", "/".to_string(), false),
            data_path: env.get("RS_DATA_PATH", "/data".to_string(), false),
            api_token: env.get("RS_API_TOKEN", "".to_string(), true),
            cert_path: env.get("RS_CERT_PATH", "".to_string(), true),
            cert_key_path: env.get("RS_CERT_KEY_PATH", "".to_string(), true),
            buckets: HashMap::new(),
            tokens: HashMap::new(),
        };

        let mut buckets = HashMap::<String, (String, BucketSettings)>::new();
        for (id, name) in env.matches("RS_BUCKET_(.*)_NAME", false) {
            buckets.insert(id, (name, BucketSettings::default()));
        }

        macro_rules! parse_settings {
            ($resource:literal, $name:ident, $buckets:ident, $key:literal, $type:ty) => {
                for (id, value) in
                    env.matches::<String>(&format!("RS_{}_(.*)_{}", $resource, $key), false)
                {
                    if let Some(bucket) = $buckets.get_mut(&id) {
                        let (name, settings) = bucket;
                        if let Ok($name) = <$type>::from_str(&value) {
                            settings.$name = Some($name);
                        } else {
                            warn!("Invalid {} {} for {} '{}'", $key, value, $resource, name);
                        }
                    } else {
                        warn!(
                            "No name found for provision RS_{}_{}_{}: ignored",
                            $resource, id, $key
                        );
                    }
                }
            };
        }

        parse_settings!("BUCKET", quota_type, buckets, "QUOTA_TYPE", QuotaType);
        parse_settings!("BUCKET", quota_size, buckets, "QUOTA_SIZE", u64);
        parse_settings!("BUCKET", max_block_size, buckets, "MAX_BLOCK_SIZE", u64);
        parse_settings!(
            "BUCKET",
            max_block_records,
            buckets,
            "MAX_BLOCK_RECORDS",
            u64
        );

        // let mut tokens = HashMap::<String, (String, Permissions)>::new();
        // for (id, name) in env.matches("RS_TOKEN_(.*)_NAME", false) {
        //     tokens.insert(id, (name, Permissions::default()));
        // }
        //
        // parse_settings!("TOKEN", full_access, tokens, "FULL_ACCESS", bool);
        cfg
    }
}
