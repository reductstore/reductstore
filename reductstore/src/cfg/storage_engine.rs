// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::cfg::CfgParser;
use crate::core::env::{Env, GetEnv};
use std::time::Duration;

#[derive(Clone, Debug, PartialEq)]
pub struct StorageEngineConfig {
    pub compaction_interval: Duration,
}

const DEFAULT_COMPACTION_INTERVAL_SECS: u64 = 60;

impl Default for StorageEngineConfig {
    fn default() -> Self {
        StorageEngineConfig {
            compaction_interval: Duration::from_secs(DEFAULT_COMPACTION_INTERVAL_SECS), // default to 1 hour
        }
    }
}

impl<EnvGetter: GetEnv> CfgParser<EnvGetter> {
    pub fn parse_storage_engine_config(env: &mut Env<EnvGetter>) -> StorageEngineConfig {
        StorageEngineConfig {
            compaction_interval: Duration::from_secs(
                env.get_optional::<u64>("RS_ENGINE_COMPACTION_INTERVAL")
                    .unwrap_or(DEFAULT_COMPACTION_INTERVAL_SECS),
            ),
        }
    }
}
