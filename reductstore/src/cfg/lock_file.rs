// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::cfg::CfgParser;
use crate::core::env::{Env, GetEnv};
use crate::lock_file::FailureAction;
use std::time::Duration;

const DEFAULT_ACQUIRE_TIMEOUT_S: u64 = 10;

/// IO configuration
#[derive(Clone, Debug, PartialEq)]
pub struct LockFileConfig {
    /// Whether to enable lock file usage
    pub enabled: bool,
    /// Timeout for acquiring the lock file
    pub timeout: Duration,
    /// Failure action if lock file cannot be acquired
    pub failure_action: FailureAction,
}

impl Default for LockFileConfig {
    fn default() -> Self {
        LockFileConfig {
            enabled: false,
            timeout: Duration::from_secs(DEFAULT_ACQUIRE_TIMEOUT_S),
            failure_action: FailureAction::Abort,
        }
    }
}

impl<EnvGetter: GetEnv> CfgParser<EnvGetter> {
    pub(super) fn parse_lock_file_config(env: &mut Env<EnvGetter>) -> LockFileConfig {
        LockFileConfig {
            enabled: env
                .get_optional::<bool>("RS_LOCK_FILE_ENABLED")
                .unwrap_or(false),
            timeout: Duration::from_secs(
                env.get_optional::<u64>("RS_LOCK_FILE_ACQUIRE_TIMEOUT")
                    .unwrap_or(DEFAULT_ACQUIRE_TIMEOUT_S),
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
            .with(eq("RS_LOCK_FILE_ACQUIRE_TIMEOUT"))
            .return_const(Ok("20".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_LOCK_FILE_FAILURE_ACTION"))
            .return_const(Ok("proceed".to_string()));

        let lock_file_settings = LockFileConfig {
            enabled: true,
            timeout: Duration::from_secs(20),
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
            .with(eq("RS_LOCK_FILE_ACQUIRE_TIMEOUT"))
            .return_const(Ok("20".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_LOCK_FILE_FAILURE_ACTION"))
            .return_const(Ok("invalid_action".to_string()));

        CfgParser::<MockEnvGetter>::parse_lock_file_config(&mut Env::new(env_getter));
    }
}
