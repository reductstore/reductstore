// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::cfg::{CfgParser, ExtCfgBounds};
use crate::core::env::{Env, GetEnv};
use std::time::Duration;

#[derive(Clone, Debug, PartialEq)]
pub struct StorageEngineConfig {
    pub compaction_interval: Duration,
    pub replica_update_interval: Duration,
    pub enable_integrity_checks: bool,
}

const DEFAULT_COMPACTION_INTERVAL_SECS: u64 = 60;
const DEFAULT_REPLICA_UPDATE_INTERVAL_SECS: u64 = 60;

impl Default for StorageEngineConfig {
    fn default() -> Self {
        StorageEngineConfig {
            compaction_interval: Duration::from_secs(DEFAULT_COMPACTION_INTERVAL_SECS),
            replica_update_interval: Duration::from_secs(DEFAULT_REPLICA_UPDATE_INTERVAL_SECS),
            enable_integrity_checks: true,
        }
    }
}

impl<EnvGetter: GetEnv, ExtCfg: ExtCfgBounds> CfgParser<EnvGetter, ExtCfg> {
    pub fn parse_storage_engine_config(env: &mut Env<EnvGetter>) -> StorageEngineConfig {
        StorageEngineConfig {
            compaction_interval: Duration::from_secs(
                env.get_optional::<u64>("RS_ENGINE_COMPACTION_INTERVAL")
                    .unwrap_or(DEFAULT_COMPACTION_INTERVAL_SECS),
            ),
            replica_update_interval: Duration::from_secs(
                env.get_optional::<u64>("RS_ENGINE_REPLICA_UPDATE_INTERVAL")
                    .unwrap_or(DEFAULT_REPLICA_UPDATE_INTERVAL_SECS),
            ),
            enable_integrity_checks: env
                .get_optional::<bool>("RS_ENGINE_ENABLE_INTEGRITY_CHECKS")
                .unwrap_or(true),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::tests::MockEnvGetter;
    use mockall::predicate::eq;
    use rstest::rstest;
    use std::env::VarError;

    #[rstest]
    fn parses_engine_config_from_env() {
        let mut env_getter = MockEnvGetter::new();

        env_getter
            .expect_get()
            .with(eq("RS_ENGINE_COMPACTION_INTERVAL"))
            .return_const(Ok("120".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_ENGINE_REPLICA_UPDATE_INTERVAL"))
            .return_const(Ok("45".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_ENGINE_ENABLE_INTEGRITY_CHECKS"))
            .return_const(Ok("false".to_string()));

        let expected = StorageEngineConfig {
            compaction_interval: Duration::from_secs(120),
            replica_update_interval: Duration::from_secs(45),
            enable_integrity_checks: false,
        };

        assert_eq!(
            expected,
            CfgParser::<MockEnvGetter>::parse_storage_engine_config(&mut Env::new(env_getter))
        );
    }

    #[rstest]
    fn uses_default_engine_config_when_missing() {
        let mut env_getter = MockEnvGetter::new();

        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let expected = StorageEngineConfig::default();

        assert_eq!(
            expected,
            CfgParser::<MockEnvGetter>::parse_storage_engine_config(&mut Env::new(env_getter))
        );
    }
}
