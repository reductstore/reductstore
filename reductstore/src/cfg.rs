// Copyright 2023-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

pub mod io;
mod provision;
mod remote_storage;
pub mod replication;

use crate::api::Components;
use crate::asset::asset_manager::create_asset_manager;
use crate::auth::token_auth::TokenAuthorization;
use crate::backend::Backend;
use crate::cfg::io::IoConfig;
use crate::cfg::remote_storage::RemoteStorageConfig;
use crate::cfg::replication::ReplicationConfig;
use crate::core::cache::Cache;
use crate::core::env::{Env, GetEnv};
use crate::core::file_cache::FILE_CACHE;
use crate::ext::ext_repository::create_ext_repository;
use log::info;
use reduct_base::error::ReductError;
use reduct_base::ext::ExtSettings;
use reduct_base::internal_server_error;
use reduct_base::msg::bucket_api::BucketSettings;
use reduct_base::msg::replication_api::ReplicationSettings;
use reduct_base::msg::token_api::Token;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

pub const DEFAULT_LOG_LEVEL: &str = "INFO";
pub const DEFAULT_HOST: &str = "0.0.0.0";
pub const DEFAULT_PORT: u16 = 8383;

pub const DEFAULT_CACHED_QUERIES: usize = 8;
pub const DEFAULT_CACHED_QUERIES_TTL: u64 = 600; // seconds

#[derive(Clone)]
pub struct Cfg {
    pub log_level: String,
    pub host: String,
    pub port: u16,
    pub public_url: String,
    pub api_base_path: String,
    pub data_path: String,
    pub api_token: String,
    pub cert_path: String,
    pub cert_key_path: String,
    pub license_path: Option<String>,
    pub ext_path: Option<String>,
    pub cors_allow_origin: Vec<String>,
    pub buckets: HashMap<String, BucketSettings>,
    pub tokens: HashMap<String, Token>,
    pub replications: HashMap<String, ReplicationSettings>,
    pub io_conf: IoConfig,
    pub replication_conf: ReplicationConfig,
    pub cs_config: RemoteStorageConfig,
}

impl Default for Cfg {
    fn default() -> Self {
        Cfg {
            log_level: DEFAULT_LOG_LEVEL.to_string(),
            host: DEFAULT_HOST.to_string(),
            port: DEFAULT_PORT,
            public_url: format!("http://{}:{}/", DEFAULT_HOST, DEFAULT_PORT),
            api_base_path: "/".to_string(),
            data_path: "/data".to_string(),
            api_token: "".to_string(),
            cert_path: "".to_string(),
            cert_key_path: "".to_string(),
            license_path: None,
            ext_path: None,
            cors_allow_origin: vec![],
            buckets: HashMap::new(),
            tokens: HashMap::new(),
            replications: HashMap::new(),
            io_conf: IoConfig::default(),
            replication_conf: ReplicationConfig::default(),
            cs_config: RemoteStorageConfig::default(),
        }
    }
}

/// Database configuration
pub struct CfgParser<EnvGetter: GetEnv> {
    pub cfg: Cfg,
    pub env: Env<EnvGetter>,
}

impl<EnvGetter: GetEnv> CfgParser<EnvGetter> {
    pub fn from_env(env_getter: EnvGetter) -> Self {
        let mut env = Env::new(env_getter);

        let mut api_base_path = env.get("RS_API_BASE_PATH", "/".to_string());
        Self::normalize_url_path(&mut api_base_path);

        let host = env.get("RS_HOST", DEFAULT_HOST.to_string());
        let port = env.get("RS_PORT", DEFAULT_PORT);
        let cert_path = env
            .get_optional::<String>("RS_CERT_PATH")
            .unwrap_or_default();
        let cert_key_path = env
            .get_optional::<String>("RS_CERT_KEY_PATH")
            .unwrap_or_default();
        let protocol = if cert_path.is_empty() {
            "http"
        } else {
            "https"
        };

        let default_public_url = if port == 80 || port == 443 {
            format!("{}://{}{}", protocol, host, api_base_path)
        } else {
            format!("{}://{}:{}{}", protocol, host, port, api_base_path)
        };

        let mut public_url = env.get("RS_PUBLIC_URL", default_public_url.clone());
        if !public_url.ends_with('/') {
            public_url.push('/');
        }

        let cfg = CfgParser {
            cfg: Cfg {
                log_level: env.get("RS_LOG_LEVEL", DEFAULT_LOG_LEVEL.to_string()),
                host,
                public_url,
                port,
                api_base_path,
                data_path: env.get("RS_DATA_PATH", "/data".to_string()),
                api_token: env.get_masked("RS_API_TOKEN", "".to_string()),
                cert_path,
                cert_key_path,
                license_path: env.get_optional("RS_LICENSE_PATH"),
                ext_path: env.get_optional("RS_EXT_PATH"),
                cors_allow_origin: Self::parse_cors_allow_origin(&mut env),
                buckets: Self::parse_buckets(&mut env),
                tokens: Self::parse_tokens(&mut env),
                replications: Self::parse_replications(&mut env),
                io_conf: Self::parse_io_config(&mut env),
                replication_conf: Self::parse_replication_config(&mut env, port),
                cs_config: Self::parse_remote_storage_cfg(&mut env),
            },
            env,
        };

        cfg
    }

    fn normalize_url_path(api_base_path: &mut String) {
        if !api_base_path.starts_with('/') {
            api_base_path.insert(0, '/');
        }

        if !api_base_path.ends_with('/') {
            api_base_path.push('/');
        }
    }

    pub fn build(&self) -> Result<Components, ReductError> {
        // Initialize storage backend
        let mut backend_builder = Backend::builder()
            .backend_type(self.cfg.cs_config.backend_type.clone())
            .local_data_path(&self.cfg.data_path)
            .cache_size(self.cfg.cs_config.cache_size);

        if let Some(bucket) = &self.cfg.cs_config.bucket {
            backend_builder = backend_builder.remote_bucket(bucket);
        }

        if let Some(region) = &self.cfg.cs_config.region {
            backend_builder = backend_builder.remote_region(region);
        }

        if let Some(endpoint) = &self.cfg.cs_config.endpoint {
            backend_builder = backend_builder.remote_endpoint(endpoint);
        }

        if let Some(access_key) = &self.cfg.cs_config.access_key {
            backend_builder = backend_builder.remote_access_key(access_key);
        }

        if let Some(secret_key) = &self.cfg.cs_config.secret_key {
            backend_builder = backend_builder.remote_secret_key(secret_key);
        }

        if let Some(cache_path) = &self.cfg.cs_config.cache_path {
            backend_builder = backend_builder.remote_cache_path(cache_path);
        }

        FILE_CACHE.set_storage_backend(backend_builder.try_build().map_err(|e| {
            internal_server_error!(
                "Failed to initialize storage backend at {}: {}",
                self.cfg.data_path,
                e
            )
        })?);

        let storage = Arc::new(self.provision_buckets());
        let token_repo = self.provision_tokens(storage.data_path());
        let console = create_asset_manager(load_console());
        let select_ext = create_asset_manager(load_select_ext());
        let ros_ext = create_asset_manager(load_ros_ext());
        let replication_engine = self.provision_replication_repo(Arc::clone(&storage))?;

        let ext_path = if let Some(ext_path) = &self.cfg.ext_path {
            Some(PathBuf::try_from(ext_path).map_err(|e| {
                internal_server_error!("Failed to resolve extension path {}: {}", ext_path, e)
            })?)
        } else {
            None
        };

        let server_info = storage.info()?;

        Ok(Components {
            storage,
            token_repo: tokio::sync::RwLock::new(token_repo),
            auth: TokenAuthorization::new(&self.cfg.api_token),
            console,
            replication_repo: tokio::sync::RwLock::new(replication_engine),
            ext_repo: create_ext_repository(
                ext_path,
                vec![select_ext, ros_ext],
                ExtSettings::builder()
                    .log_level(&self.cfg.log_level)
                    .server_info(server_info)
                    .build(),
            )?,
            query_link_cache: tokio::sync::RwLock::new(Cache::new(
                DEFAULT_CACHED_QUERIES,
                Duration::from_secs(DEFAULT_CACHED_QUERIES_TTL),
            )),
            cfg: self.cfg.clone(),
        })
    }

    fn parse_cors_allow_origin(env: &mut Env<EnvGetter>) -> Vec<String> {
        let cors_origins_str: String = env.get_optional("RS_CORS_ALLOW_ORIGIN").unwrap_or_default();

        cors_origins_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    }
}

#[cfg(feature = "web-console")]
fn load_console() -> &'static [u8] {
    info!("Load Web Console");
    include_bytes!(concat!(env!("OUT_DIR"), "/console.zip"))
}

#[cfg(not(feature = "web-console"))]
fn load_console() -> &'static [u8] {
    info!("Web Console is disabled");
    b""
}

#[cfg(feature = "select-ext")]
fn load_select_ext() -> &'static [u8] {
    info!("Load Reduct Select Extension");
    include_bytes!(concat!(env!("OUT_DIR"), "/select-ext.zip"))
}

#[cfg(not(feature = "select-ext"))]
fn load_select_ext() -> &'static [u8] {
    info!("Reduct Select Extension is disabled");
    b""
}

#[cfg(feature = "ros-ext")]
fn load_ros_ext() -> &'static [u8] {
    info!("Load Reduct ROS Extension");
    include_bytes!(concat!(env!("OUT_DIR"), "/ros-ext.zip"))
}

#[cfg(not(feature = "ros-ext"))]
fn load_ros_ext() -> &'static [u8] {
    info!("Reduct ROS Extension is disabled");
    b""
}

impl<EnvGetter: GetEnv> Display for CfgParser<EnvGetter> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.env.message())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use mockall::mock;
    use mockall::predicate::eq;
    use rstest::{fixture, rstest};
    use std::collections::BTreeMap;
    use std::env::VarError;

    mock! {
        pub(super) EnvGetter {}
        impl GetEnv for EnvGetter {
            fn get(&self, key: &str) -> Result<String, VarError>;
            fn all(&self) -> BTreeMap<String,String>;
        }
    }

    #[rstest]
    fn test_default_settings(mut env_getter: MockEnvGetter) {
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let parser = CfgParser::from_env(env_getter);
        assert_eq!(parser.cfg.log_level, "INFO");
        assert_eq!(parser.cfg.host, "0.0.0.0");
        assert_eq!(parser.cfg.port, 8383);
        assert_eq!(parser.cfg.api_base_path, "/");
        assert_eq!(parser.cfg.public_url, "http://0.0.0.0:8383/");
        assert_eq!(parser.cfg.data_path, "/data");
        assert_eq!(parser.cfg.api_token, "");
        assert_eq!(parser.cfg.cert_path, "");
        assert_eq!(parser.cfg.cert_key_path, "");
        assert_eq!(parser.cfg.cors_allow_origin.len(), 0);

        assert_eq!(parser.cfg.buckets.len(), 0);
        assert_eq!(parser.cfg.tokens.len(), 0);
    }

    #[rstest]
    fn test_log_level(mut env_getter: MockEnvGetter) {
        env_getter
            .expect_get()
            .with(eq("RS_LOG_LEVEL"))
            .times(1)
            .return_const(Ok("DEBUG".to_string()));
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));
        let parser = CfgParser::from_env(env_getter);
        assert_eq!(parser.cfg.log_level, "DEBUG");
    }

    #[rstest]
    fn test_host(mut env_getter: MockEnvGetter) {
        env_getter
            .expect_get()
            .with(eq("RS_HOST"))
            .times(1)
            .return_const(Ok("127.0.0.1".to_string()));
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));
        let parser = CfgParser::from_env(env_getter);
        assert_eq!(parser.cfg.host, "127.0.0.1");
    }

    #[rstest]
    fn test_port(mut env_getter: MockEnvGetter) {
        env_getter
            .expect_get()
            .with(eq("RS_PORT"))
            .times(1)
            .return_const(Ok("1234".to_string()));
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));
        let parser = CfgParser::from_env(env_getter);
        assert_eq!(parser.cfg.port, 1234);
    }

    #[rstest]
    #[case("/api")]
    #[case("/api/")]
    #[case("api/")]
    #[case("api")]
    fn test_api_base_path(mut env_getter: MockEnvGetter, #[case] path: &str) {
        env_getter
            .expect_get()
            .with(eq("RS_API_BASE_PATH"))
            .times(1)
            .return_const(Ok(path.to_string()));
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));
        let parser = CfgParser::from_env(env_getter);
        assert_eq!(parser.cfg.api_base_path, "/api/");
    }

    mod public_url {
        use super::*;
        use rstest::rstest;
        #[rstest]
        fn from_env(mut env_getter: MockEnvGetter) {
            env_getter
                .expect_get()
                .with(eq("RS_PUBLIC_URL"))
                .times(1)
                .return_const(Ok("https://example.com/".to_string()));
            env_getter
                .expect_get()
                .return_const(Err(VarError::NotPresent));
            let parser = CfgParser::from_env(env_getter);
            assert_eq!(parser.cfg.public_url, "https://example.com/");
        }

        #[rstest]
        fn from_env_without_slash(mut env_getter: MockEnvGetter) {
            env_getter
                .expect_get()
                .with(eq("RS_PUBLIC_URL"))
                .times(1)
                .return_const(Ok("https://example.com".to_string()));
            env_getter
                .expect_get()
                .return_const(Err(VarError::NotPresent));
            let parser = CfgParser::from_env(env_getter);
            assert_eq!(parser.cfg.public_url, "https://example.com/");
        }

        #[rstest]
        fn default_http(mut env_getter: MockEnvGetter) {
            env_getter
                .expect_get()
                .with(eq("RS_HOST"))
                .times(1)
                .return_const(Ok("example.com".to_string()));
            env_getter
                .expect_get()
                .with(eq("RS_PORT"))
                .times(1)
                .return_const(Ok("80".to_string()));
            env_getter
                .expect_get()
                .with(eq("RS_API_BASE_PATH"))
                .times(1)
                .return_const(Ok("/api/".to_string()));
            env_getter
                .expect_get()
                .return_const(Err(VarError::NotPresent));
            let parser = CfgParser::from_env(env_getter);
            assert_eq!(parser.cfg.public_url, "http://example.com/api/");
        }

        #[rstest]
        fn default_https(mut env_getter: MockEnvGetter) {
            env_getter
                .expect_get()
                .with(eq("RS_HOST"))
                .times(1)
                .return_const(Ok("example.com".to_string()));
            env_getter
                .expect_get()
                .with(eq("RS_PORT"))
                .times(1)
                .return_const(Ok("443".to_string()));
            env_getter
                .expect_get()
                .with(eq("RS_API_BASE_PATH"))
                .times(1)
                .return_const(Ok("/api/".to_string()));
            env_getter
                .expect_get()
                .with(eq("RS_CERT_PATH"))
                .times(1)
                .return_const(Ok("/tmp/cert.pem".to_string()));
            env_getter
                .expect_get()
                .with(eq("RS_CERT_KEY_PATH"))
                .times(1)
                .return_const(Ok("/tmp/cert.key".to_string()));
            env_getter
                .expect_get()
                .return_const(Err(VarError::NotPresent));
            let parser = CfgParser::from_env(env_getter);
            assert_eq!(parser.cfg.public_url, "https://example.com/api/");
        }
    }

    #[rstest]
    fn test_data_path(mut env_getter: MockEnvGetter) {
        env_getter
            .expect_get()
            .with(eq("RS_DATA_PATH"))
            .times(1)
            .return_const(Ok("/tmp".to_string()));
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));
        let parser = CfgParser::from_env(env_getter);
        assert_eq!(parser.cfg.data_path, "/tmp");
    }

    #[rstest]
    fn test_api_token(mut env_getter: MockEnvGetter) {
        env_getter
            .expect_get()
            .with(eq("RS_API_TOKEN"))
            .times(1)
            .return_const(Ok("XXX".to_string()));
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));
        let parser = CfgParser::from_env(env_getter);
        assert_eq!(parser.cfg.api_token, "XXX");
    }

    #[rstest]
    fn test_cert_path(mut env_getter: MockEnvGetter) {
        env_getter
            .expect_get()
            .with(eq("RS_CERT_PATH"))
            .times(1)
            .return_const(Ok("/tmp/cert.pem".to_string()));
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));
        let parser = CfgParser::from_env(env_getter);
        assert_eq!(parser.cfg.cert_path, "/tmp/cert.pem");
    }

    #[rstest]
    fn test_cert_key_path(mut env_getter: MockEnvGetter) {
        env_getter
            .expect_get()
            .with(eq("RS_CERT_KEY_PATH"))
            .times(1)
            .return_const(Ok("/tmp/cert.key".to_string()));
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));
        let parser = CfgParser::from_env(env_getter);
        assert_eq!(parser.cfg.cert_key_path, "/tmp/cert.key");
    }

    #[rstest]
    fn test_cors_allow_origin(mut env_getter: MockEnvGetter) {
        env_getter
            .expect_get()
            .with(eq("RS_CORS_ALLOW_ORIGIN"))
            .times(1)
            .return_const(Ok("http://localhost,http://example.com".to_string()));
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));
        let parser = CfgParser::from_env(env_getter);
        assert_eq!(
            parser.cfg.cors_allow_origin,
            vec!["http://localhost", "http://example.com"]
        );
    }

    #[rstest]
    fn test_ext_path(mut env_getter: MockEnvGetter) {
        env_getter
            .expect_get()
            .with(eq("RS_EXT_PATH"))
            .times(1)
            .return_const(Ok("/tmp/ext".to_string()));
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));
        let parser = CfgParser::from_env(env_getter);
        assert_eq!(parser.cfg.ext_path, Some("/tmp/ext".to_string()));
    }

    #[cfg(feature = "fs-backend")]
    #[rstest]
    fn test_remote_storage_s3() {
        // we cover only s3 parts here, filesystem is used as backend
        let mut env_getter = MockEnvGetter::new();
        env_getter
            .expect_get()
            .with(eq("RS_DATA_PATH"))
            .return_const(Ok("/tmp/data".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_REMOTE_BUCKET"))
            .return_const(Ok("my-bucket".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_REMOTE_ENDPOINT"))
            .return_const(Ok("https://s3.example.com".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_REMOTE_REGION"))
            .return_const(Ok("us-east-1".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_REMOTE_ACCESS_KEY"))
            .return_const(Ok("my-access-key".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_REMOTE_SECRET_KEY"))
            .return_const(Ok("my-secret-key".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_REMOTE_CACHE_PATH"))
            .return_const(Ok("/tmp/cache".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_REMOTE_CACHE_SIZE"))
            .return_const(Ok("1073741824".to_string()));
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));
        env_getter.expect_all().returning(|| BTreeMap::new());
        let parser = CfgParser::from_env(env_getter);
        parser.build().unwrap();
    }

    #[fixture]
    fn env_getter() -> MockEnvGetter {
        let mut mock_getter = MockEnvGetter::new();
        mock_getter.expect_all().returning(|| BTreeMap::new());
        mock_getter
    }
}
