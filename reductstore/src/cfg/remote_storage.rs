// Copyright 2025-2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::backend::BackendType;
use crate::cfg::CfgParser;
use crate::core::env::{Env, GetEnv};
use bytesize::ByteSize;
use std::path::PathBuf;
use std::time::Duration;

/// Cloud storage settings
#[derive(Clone, Debug, PartialEq, Default)]
pub struct RemoteStorageConfig {
    pub backend_type: BackendType,
    pub bucket: Option<String>,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub cache_path: Option<PathBuf>,
    pub cache_size: u64,
    pub sync_interval: Duration,
    pub default_storage_class: Option<String>,
}

impl<EnvGetter: GetEnv, ExtCfg: Clone + Send + Sync> CfgParser<EnvGetter, ExtCfg> {
    pub(super) fn parse_remote_storage_cfg(env: &mut Env<EnvGetter>) -> RemoteStorageConfig {
        let secret_key = env.get_masked("RS_REMOTE_SECRET_KEY", "".to_string());
        let (backend_type, default_sync_interval) = match env
            .get_optional::<String>("RS_REMOTE_BACKEND_TYPE")
            .map(|s| s.to_lowercase())
            .as_deref()
        {
            #[cfg(feature = "s3-backend")]
            Some("s3") => (BackendType::S3, Duration::from_secs(60)),
            _ => (BackendType::Filesystem, Duration::from_millis(100)),
        };
        RemoteStorageConfig {
            backend_type,
            bucket: env.get_optional::<String>("RS_REMOTE_BUCKET"),
            endpoint: env.get_optional::<String>("RS_REMOTE_ENDPOINT"),
            region: env.get_optional::<String>("RS_REMOTE_REGION"),
            access_key: env.get_optional::<String>("RS_REMOTE_ACCESS_KEY"),
            secret_key: if secret_key.is_empty() {
                None
            } else {
                Some(secret_key)
            },
            cache_path: env
                .get_optional::<String>("RS_REMOTE_CACHE_PATH")
                .map(PathBuf::from),
            cache_size: env
                .get_optional::<ByteSize>("RS_REMOTE_CACHE_SIZE")
                .unwrap_or(ByteSize::gb(1))
                .as_u64(),
            sync_interval: env
                .get_optional::<f64>("RS_REMOTE_SYNC_INTERVAL")
                .map(Duration::from_secs_f64)
                .unwrap_or(default_sync_interval),
            default_storage_class: env.get_optional::<String>("RS_REMOTE_DEFAULT_STORAGE_CLASS"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::tests::MockEnvGetter;
    use mockall::predicate::eq;
    use rstest::*;
    use std::env::VarError;

    #[cfg(feature = "fs-backend")]
    #[rstest]
    fn test_default_remote_storage_cfg() {
        let mut env_getter = MockEnvGetter::new();
        env_getter
            .expect_get()
            .with(eq("RS_REMOTE_BACKEND_TYPE"))
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_REMOTE_BUCKET"))
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_REMOTE_ENDPOINT"))
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_REMOTE_REGION"))
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_REMOTE_ACCESS_KEY"))
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_REMOTE_SECRET_KEY"))
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_REMOTE_CACHE_PATH"))
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_REMOTE_CACHE_SIZE"))
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_REMOTE_SYNC_INTERVAL"))
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_REMOTE_DEFAULT_STORAGE_CLASS"))
            .return_const(Err(VarError::NotPresent));

        let mut env = Env::new(env_getter);

        let cfg = CfgParser::parse_remote_storage_cfg(&mut env);
        assert_eq!(
            cfg,
            RemoteStorageConfig {
                backend_type: BackendType::Filesystem,
                bucket: None,
                endpoint: None,
                region: None,
                access_key: None,
                secret_key: None,
                cache_path: None,
                cache_size: ByteSize::gb(1).as_u64(),
                sync_interval: Duration::from_millis(100),
                default_storage_class: None,
            }
        );
    }

    #[cfg(feature = "s3-backend")]
    #[rstest]
    fn test_custom_remote_storage_cfg() {
        let mut env_getter = MockEnvGetter::new();
        env_getter
            .expect_get()
            .with(eq("RS_REMOTE_BACKEND_TYPE"))
            .return_const(Ok("s3".to_string()));
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
            .return_const(Ok("us-west-1".to_string()));
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
            .return_const(Ok("2GB".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_REMOTE_SYNC_INTERVAL"))
            .return_const(Ok("60".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_REMOTE_DEFAULT_STORAGE_CLASS"))
            .return_const(Ok("STANDARD".to_string()));

        let mut env = Env::new(env_getter);

        let cfg = CfgParser::parse_remote_storage_cfg(&mut env);
        assert_eq!(
            cfg,
            RemoteStorageConfig {
                backend_type: BackendType::S3,
                bucket: Some("my-bucket".to_string()),
                endpoint: Some("https://s3.example.com".to_string()),
                region: Some("us-west-1".to_string()),
                access_key: Some("my-access-key".to_string()),
                secret_key: Some("my-secret-key".to_string()),
                cache_path: Some(PathBuf::from("/tmp/cache")),
                cache_size: ByteSize::gb(2).as_u64(),
                sync_interval: Duration::from_secs(60),
                default_storage_class: Some("STANDARD".to_string()),
            }
        );
    }
}
