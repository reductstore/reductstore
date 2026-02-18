// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::cfg::{CfgParser, ExtCfgBounds};
use crate::core::env::{Env, GetEnv};
use crate::core::sync::{
    default_rwlock_failure_action, default_rwlock_timeout, RwLockFailureAction,
};
use std::time::Duration;

#[derive(Clone, Debug, PartialEq)]
pub struct RwLockConfig {
    pub timeout: Duration,
    pub failure_action: RwLockFailureAction,
}

impl Default for RwLockConfig {
    fn default() -> Self {
        RwLockConfig {
            timeout: default_rwlock_timeout(),
            failure_action: default_rwlock_failure_action(),
        }
    }
}

impl<EnvGetter: GetEnv, ExtCfg: ExtCfgBounds> CfgParser<EnvGetter, ExtCfg> {
    pub(super) fn parse_rw_lock_config(env: &mut Env<EnvGetter>) -> RwLockConfig {
        let timeout = env
            .get_optional::<u64>("RS_RWLOCK_TIMEOUT")
            .map(Duration::from_secs)
            .unwrap_or_else(default_rwlock_timeout);

        let failure_action = match env
            .get_optional::<String>("RS_RWLOCK_FAILURE_ACTION")
            .unwrap_or_else(|| default_rwlock_failure_action().as_str().to_string())
            .to_lowercase()
            .as_str()
        {
            "panic" => RwLockFailureAction::Panic,
            "error" => RwLockFailureAction::Error,
            _ => panic!("Invalid value for RS_RWLOCK_FAILURE_ACTION: must be 'panic' or 'error'"),
        };

        RwLockConfig {
            timeout,
            failure_action,
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
    fn test_rwlock_config_from_env() {
        let mut env_getter = MockEnvGetter::new();
        env_getter
            .expect_get()
            .with(eq("RS_RWLOCK_TIMEOUT"))
            .return_const(Ok("42".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_RWLOCK_FAILURE_ACTION"))
            .return_const(Ok("panic".to_string()));
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let config = CfgParser::<MockEnvGetter>::parse_rw_lock_config(&mut Env::new(env_getter));
        assert_eq!(
            config,
            RwLockConfig {
                timeout: Duration::from_secs(42),
                failure_action: RwLockFailureAction::Panic
            }
        );
    }

    #[rstest]
    fn test_rwlock_config_default() {
        let mut env_getter = MockEnvGetter::new();
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let config = CfgParser::<MockEnvGetter>::parse_rw_lock_config(&mut Env::new(env_getter));
        assert_eq!(config, RwLockConfig::default());
    }

    #[rstest]
    #[should_panic(
        expected = "Invalid value for RS_RWLOCK_FAILURE_ACTION: must be 'panic' or 'error'"
    )]
    fn test_rwlock_config_invalid_action() {
        let mut env_getter = MockEnvGetter::new();
        env_getter
            .expect_get()
            .with(eq("RS_RWLOCK_FAILURE_ACTION"))
            .return_const(Ok("invalid".to_string()));
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let _ = CfgParser::<MockEnvGetter>::parse_rw_lock_config(&mut Env::new(env_getter));
    }
}
