// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::backend::BackendType;
use crate::cfg::Cfg;
use crate::core::env::{Env, GetEnv};
use bytesize::ByteSize;

/// Cloud storage settings
#[derive(Clone, Debug, PartialEq)]
pub struct RemoteStorageConfig {
    pub backend_type: BackendType,
    pub bucket: Option<String>,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub cache_path: Option<String>,
    pub cache_size: u64,
}

impl<EnvGetter: GetEnv> Cfg<EnvGetter> {
    pub(super) fn parse_remote_storage_cfg(env: &mut Env<EnvGetter>) -> RemoteStorageConfig {
        RemoteStorageConfig {
            backend_type: env
                .get_optional::<String>("RS_REMOTE_BACKEND_TYPE")
                .map(|s| match s.to_lowercase().as_str() {
                    "s3" => BackendType::S3,
                    _ => BackendType::Filesystem,
                })
                .unwrap_or(BackendType::Filesystem),
            bucket: env.get_optional::<String>("RS_REMOTE_BUCKET"),
            endpoint: env.get_optional::<String>("RS_REMOTE_ENDPOINT"),
            region: env.get_optional::<String>("RS_REMOTE_REGION"),
            access_key: env.get_optional::<String>("RS_REMOTE_ACCESS_KEY"),
            secret_key: env.get_optional::<String>("RS_REMOTE_SECRET_KEY"),
            cache_path: env.get_optional::<String>("RS_REMOTE_CACHE_PATH"),
            cache_size: env
                .get_optional::<ByteSize>("RS_REMOTE_CACHE_SIZE")
                .unwrap_or(ByteSize::gb(1))
                .as_u64(),
        }
    }
}
