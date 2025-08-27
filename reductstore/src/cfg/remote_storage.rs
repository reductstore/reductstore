// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::cfg::Cfg;
use crate::core::env::{Env, GetEnv};
use bytesize::ByteSize;
use std::time::Duration;

/// Cloud storage settings
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RemoteStorageConfig {
    pub region: Option<String>,
    pub local_cache_path: Option<String>,
}

impl<EnvGetter: GetEnv> Cfg<EnvGetter> {
    pub(super) fn parse_remote_storage_cfg(env: &mut Env<EnvGetter>) -> RemoteStorageConfig {
        RemoteStorageConfig {
            region: env.get_optional::<String>("RS_REMOTE_REGION"),
            local_cache_path: env.get_optional::<String>("RS_REMOTE_CACHE_PATH"),
        }
    }
}
