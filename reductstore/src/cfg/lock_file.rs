// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::cfg::CfgParser;
use crate::core::env::{Env, GetEnv};
use crate::lock_file::FailureAction;
use std::time::Duration;

const DEFAULT_ACQUIRE_TIMEOUT_S: u64 = 60;
const DEFAULT_LOCK_FILE_TTL_S: u64 = 30;
const DEFAULT_POLLING_INTERVAL_S: u64 = 10;

/// IO configuration
#[derive(Clone, Debug, PartialEq)]
pub struct LockFileConfig {
    /// Polling interval for checking the lock file
    pub polling_interval: Duration,
    /// Timeout for acquiring the lock file
    /// if set to 0, it will wait indefinitely
    pub timeout: Duration,
    /// TTL for the lock file
    pub ttl: Duration,
    /// Failure action if lock file cannot be acquired
    pub failure_action: FailureAction,
}

impl Default for LockFileConfig {
    fn default() -> Self {
        LockFileConfig {
            polling_interval: Duration::from_secs(DEFAULT_POLLING_INTERVAL_S),
            timeout: Duration::from_secs(DEFAULT_ACQUIRE_TIMEOUT_S),
            ttl: Duration::from_secs(DEFAULT_LOCK_FILE_TTL_S),
            failure_action: FailureAction::default(),
        }
    }
}

impl<EnvGetter: GetEnv, ExtCfg: Clone + Send + Sync> CfgParser<EnvGetter, ExtCfg> {
    pub(super) fn parse_lock_file_config(env: &mut Env<EnvGetter>) -> LockFileConfig {
        LockFileConfig {
            polling_interval: Duration::from_secs(
                env.get_optional::<u64>("RS_LOCK_FILE_POLLING_INTERVAL")
                    .unwrap_or(DEFAULT_POLLING_INTERVAL_S),
            ),
            timeout: Duration::from_secs(
                env.get_optional::<u64>("RS_LOCK_FILE_TIMEOUT")
                    .unwrap_or(DEFAULT_ACQUIRE_TIMEOUT_S),
            ),
            ttl: Duration::from_secs(
                env.get_optional::<u64>("RS_LOCK_FILE_TTL")
                    .unwrap_or(DEFAULT_LOCK_FILE_TTL_S),
            ),
            failure_action: match env
                .get_optional::<String>("RS_LOCK_FILE_FAILURE_ACTION")
                .unwrap_or("abort".to_string())
                .to_lowercase()
                .as_str()
            {
                "proceed" => FailureAction::Proceed,
                "abort" => FailureAction::Abort,
                _ => panic!(
                    "Invalid value for RS_LOCK_FILE_FAILURE_ACTION: must be 'proceed' or 'abort'"
                ),
            },
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
    fn test_lock_file_config() {
        let mut env_getter = MockEnvGetter::new();

        env_getter
            .expect_get()
            .with(eq("RS_LOCK_FILE_ENABLED"))
            .return_const(Ok("true".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_LOCK_FILE_POLLING_INTERVAL"))
            .return_const(Ok("15".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_LOCK_FILE_TIMEOUT"))
            .return_const(Ok("20".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_INSTANCE_ROLE"))
            .return_const(Ok("primary".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_LOCK_FILE_TTL"))
            .return_const(Ok("30".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_LOCK_FILE_FAILURE_ACTION"))
            .return_const(Ok("proceed".to_string()));

        let lock_file_settings = LockFileConfig {
            polling_interval: Duration::from_secs(15),
            timeout: Duration::from_secs(20),
            ttl: Duration::from_secs(DEFAULT_LOCK_FILE_TTL_S),
            failure_action: FailureAction::Proceed,
        };

        assert_eq!(
            lock_file_settings,
            CfgParser::<MockEnvGetter>::parse_lock_file_config(&mut Env::new(env_getter))
        );
    }

    #[rstest]
    fn test_default_lock_file_config() {
        let mut env_getter = MockEnvGetter::new();

        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let lock_file_settings = LockFileConfig::default();

        assert_eq!(
            lock_file_settings,
            CfgParser::<MockEnvGetter>::parse_lock_file_config(&mut Env::new(env_getter))
        );
    }

    #[rstest]
    #[should_panic(
        expected = "Invalid value for RS_LOCK_FILE_FAILURE_ACTION: must be 'proceed' or 'abort'"
    )]
    fn test_invalid_failure_action() {
        let mut env_getter = MockEnvGetter::new();

        env_getter
            .expect_get()
            .with(eq("RS_LOCK_FILE_ENABLED"))
            .return_const(Ok("true".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_LOCK_FILE_POLLING_INTERVAL"))
            .return_const(Ok("10".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_LOCK_FILE_TIMEOUT"))
            .return_const(Ok("20".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_LOCK_FILE_FAILURE_ACTION"))
            .return_const(Ok("invalid_action".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_INSTANCE_ROLE"))
            .return_const(Ok("primary".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_LOCK_FILE_TTL"))
            .return_const(Ok("30".to_string()));

        CfgParser::<MockEnvGetter>::parse_lock_file_config(&mut Env::new(env_getter));
    }
}
