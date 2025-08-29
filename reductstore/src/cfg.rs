// Copyright 2023-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

pub mod io;
mod provision;
mod remote_storage;
pub mod replication;

use crate::api::Components;
use crate::asset::asset_manager::create_asset_manager;
use crate::auth::token_auth::TokenAuthorization;
use crate::cfg::io::IoConfig;
use crate::cfg::remote_storage::RemoteStorageConfig;
use crate::cfg::replication::ReplicationConfig;
use crate::core::env::{Env, GetEnv};
use crate::core::file_cache::FILE_CACHE;
use crate::ext::ext_repository::create_ext_repository;
use backpack_rs::Backpack;
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
use std::sync::Arc;

pub const DEFAULT_LOG_LEVEL: &str = "INFO";
pub const DEFAULT_HOST: &str = "0.0.0.0";
pub const DEFAULT_PORT: u16 = 8383;

/// Database configuration
pub struct Cfg<EnvGetter: GetEnv> {
    pub log_level: String,
    pub host: String,
    pub port: u16,
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
    pub env: Env<EnvGetter>,
}

impl<EnvGetter: GetEnv> Cfg<EnvGetter> {
    pub fn from_env(env_getter: EnvGetter) -> Self {
        let mut env = Env::new(env_getter);
        let port: u16 = env.get("RS_PORT", DEFAULT_PORT);

        let mut api_base_path = env.get("RS_API_BASE_PATH", "/".to_string());
        Self::normalize_url_path(&mut api_base_path);

        let cfg = Cfg {
            log_level: env.get("RS_LOG_LEVEL", DEFAULT_LOG_LEVEL.to_string()),
            host: env.get("RS_HOST", DEFAULT_HOST.to_string()),
            port: port.clone(),
            api_base_path,
            data_path: env.get("RS_DATA_PATH", "/data".to_string()),
            api_token: env.get_masked("RS_API_TOKEN", "".to_string()),
            cert_path: env.get_masked("RS_CERT_PATH", "".to_string()),
            cert_key_path: env.get_masked("RS_CERT_KEY_PATH", "".to_string()),
            license_path: env.get_optional("RS_LICENSE_PATH"),
            ext_path: env.get_optional("RS_EXT_PATH"),
            cors_allow_origin: Self::parse_cors_allow_origin(&mut env),
            buckets: Self::parse_buckets(&mut env),
            tokens: Self::parse_tokens(&mut env),
            replications: Self::parse_replications(&mut env),
            io_conf: Self::parse_io_config(&mut env),
            replication_conf: Self::parse_replication_config(&mut env, port),
            cs_config: Self::parse_remote_storage_cfg(&mut env),
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
        let mut backend_builder = Backpack::builder()
            .backend_type(self.cs_config.backend_type.clone())
            .local_data_path(&self.data_path);

        if let Some(bucket) = &self.cs_config.bucket {
            backend_builder = backend_builder.remote_bucket(bucket);
        }

        if let Some(cache_path) = &self.cs_config.endpoint {
            backend_builder = backend_builder.remote_cache_path(cache_path);
        }

        if let Some(region) = &self.cs_config.region {
            backend_builder = backend_builder.remote_region(region);
        }

        if let Some(endpoint) = &self.cs_config.endpoint {
            backend_builder = backend_builder.remote_endpoint(endpoint);
        }

        if let Some(access_key) = &self.cs_config.access_key {
            backend_builder = backend_builder.remote_access_key(access_key);
        }

        if let Some(secret_key) = &self.cs_config.secret_key {
            backend_builder = backend_builder.remote_secret_key(secret_key);
        }

        if let Some(cache_path) = &self.cs_config.cache_path {
            backend_builder = backend_builder.remote_cache_path(cache_path);
        }

        FILE_CACHE.set_storage_backend(backend_builder.try_build().map_err(|e| {
            internal_server_error!(
                "Failed to initialize storage backend at {}: {}",
                self.data_path,
                e
            )
        })?);

        let storage = Arc::new(self.provision_buckets());
        let token_repo = self.provision_tokens();
        let console = create_asset_manager(load_console());
        let select_ext = create_asset_manager(load_select_ext());
        let ros_ext = create_asset_manager(load_ros_ext());
        let replication_engine = self.provision_replication_repo(Arc::clone(&storage))?;

        let ext_path = if let Some(ext_path) = &self.ext_path {
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
            auth: TokenAuthorization::new(&self.api_token),
            console,
            replication_repo: tokio::sync::RwLock::new(replication_engine),
            ext_repo: create_ext_repository(
                ext_path,
                vec![select_ext, ros_ext],
                ExtSettings::builder()
                    .log_level(&self.log_level)
                    .server_info(server_info)
                    .build(),
            )?,

            base_path: self.api_base_path.clone(),
            io_settings: self.io_conf.clone(),
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

impl<EnvGetter: GetEnv> Display for Cfg<EnvGetter> {
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

        let cfg = Cfg::from_env(env_getter);
        assert_eq!(cfg.log_level, "INFO");
        assert_eq!(cfg.host, "0.0.0.0");
        assert_eq!(cfg.port, 8383);
        assert_eq!(cfg.api_base_path, "/");
        assert_eq!(cfg.data_path, "/data");
        assert_eq!(cfg.api_token, "");
        assert_eq!(cfg.cert_path, "");
        assert_eq!(cfg.cert_key_path, "");
        assert_eq!(cfg.cors_allow_origin.len(), 0);

        assert_eq!(cfg.buckets.len(), 0);
        assert_eq!(cfg.tokens.len(), 0);
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
        let cfg = Cfg::from_env(env_getter);
        assert_eq!(cfg.log_level, "DEBUG");
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
        let cfg = Cfg::from_env(env_getter);
        assert_eq!(cfg.host, "127.0.0.1");
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
        let cfg = Cfg::from_env(env_getter);
        assert_eq!(cfg.port, 1234);
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
        let cfg = Cfg::from_env(env_getter);
        assert_eq!(cfg.api_base_path, "/api/");
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
        let cfg = Cfg::from_env(env_getter);
        assert_eq!(cfg.data_path, "/tmp");
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
        let cfg = Cfg::from_env(env_getter);
        assert_eq!(cfg.api_token, "XXX");
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
        let cfg = Cfg::from_env(env_getter);
        assert_eq!(cfg.cert_path, "/tmp/cert.pem");
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
        let cfg = Cfg::from_env(env_getter);
        assert_eq!(cfg.cert_key_path, "/tmp/cert.key");
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
        let cfg = Cfg::from_env(env_getter);
        assert_eq!(
            cfg.cors_allow_origin,
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
        let cfg = Cfg::from_env(env_getter);
        assert_eq!(cfg.ext_path, Some("/tmp/ext".to_string()));
    }

    #[fixture]
    fn env_getter() -> MockEnvGetter {
        let mut mock_getter = MockEnvGetter::new();
        mock_getter.expect_all().returning(|| BTreeMap::new());
        return mock_getter;
    }
}
