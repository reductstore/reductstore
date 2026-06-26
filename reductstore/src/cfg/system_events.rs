// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::cfg::{parse_bool, CfgParser, ExtCfgBounds};
use crate::core::env::{Env, GetEnv};
use bytesize::ByteSize;
use log::Level;
use reduct_base::logger::parse_log_level;
use std::path::PathBuf;
use std::time::Duration;

const DEFAULT_SYSTEM_EVENTS_ENABLED: bool = true;
const DEFAULT_SYSTEM_EVENTS_REMOTE_VERIFY_SSL: bool = true;
const DEFAULT_SYSTEM_EVENTS_REMOTE_TIMEOUT_S: u64 = 5;

#[derive(Clone, Debug, PartialEq)]
pub struct SystemEventsConfig {
    pub enabled: bool,
    /// Minimum level for persisting the instance's own log messages to
    /// `$system/logs/<instance>`. `None` (unset / `OFF`) disables log capture.
    pub log_level: Option<Level>,
    pub quota_size: Option<u64>,
    pub remote_verify_ssl: bool,
    pub remote_ca_path: Option<PathBuf>,
    pub remote_timeout: Duration,
}

impl Default for SystemEventsConfig {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_SYSTEM_EVENTS_ENABLED,
            log_level: None,
            quota_size: None,
            remote_verify_ssl: DEFAULT_SYSTEM_EVENTS_REMOTE_VERIFY_SSL,
            remote_ca_path: None,
            remote_timeout: Duration::from_secs(DEFAULT_SYSTEM_EVENTS_REMOTE_TIMEOUT_S),
        }
    }
}

impl<EnvGetter: GetEnv, ExtCfg: ExtCfgBounds> CfgParser<EnvGetter, ExtCfg> {
    pub(super) fn parse_system_events_config(
        env: &mut Env<EnvGetter>,
        has_lifecycles: bool,
    ) -> SystemEventsConfig {
        SystemEventsConfig {
            enabled: parse_bool(
                env.get_optional::<String>("RS_SYSTEM_EVENTS_ENABLED"),
                DEFAULT_SYSTEM_EVENTS_ENABLED || has_lifecycles,
            ),
            log_level: env
                .get_optional::<String>("RS_SYSTEM_EVENTS_LOG_LEVEL")
                .and_then(|level| parse_log_level(&level)),
            quota_size: env
                .get_optional::<ByteSize>("RS_SYSTEM_EVENTS_QUOTA_SIZE")
                .map(|size| size.as_u64()),
            remote_verify_ssl: env
                .get_optional("RS_SYSTEM_EVENTS_REMOTE_VERIFY_SSL")
                .unwrap_or(DEFAULT_SYSTEM_EVENTS_REMOTE_VERIFY_SSL),
            remote_ca_path: env
                .get_optional::<String>("RS_SYSTEM_EVENTS_REMOTE_CA_PATH")
                .and_then(|p| {
                    if p.is_empty() {
                        None
                    } else {
                        Some(PathBuf::from(p))
                    }
                }),
            remote_timeout: Duration::from_secs(
                env.get_optional("RS_SYSTEM_EVENTS_REMOTE_TIMEOUT")
                    .unwrap_or(DEFAULT_SYSTEM_EVENTS_REMOTE_TIMEOUT_S),
            ),
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
    fn test_system_events_config_from_env() {
        let mut env_getter = MockEnvGetter::new();
        env_getter
            .expect_get()
            .with(eq("RS_SYSTEM_EVENTS_ENABLED"))
            .return_const(Ok("true".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_SYSTEM_EVENTS_LOG_LEVEL"))
            .return_const(Ok("WARN".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_SYSTEM_EVENTS_QUOTA_SIZE"))
            .return_const(Ok("10MB".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_SYSTEM_EVENTS_REMOTE_VERIFY_SSL"))
            .return_const(Ok("false".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_SYSTEM_EVENTS_REMOTE_CA_PATH"))
            .return_const(Ok("/tmp/system-ca.pem".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_SYSTEM_EVENTS_REMOTE_TIMEOUT"))
            .return_const(Ok("10".to_string()));

        let config = CfgParser::<MockEnvGetter>::parse_system_events_config(
            &mut Env::new(env_getter),
            false,
        );

        assert_eq!(
            config,
            SystemEventsConfig {
                enabled: true,
                log_level: Some(Level::Warn),
                quota_size: Some(10_000_000),
                remote_verify_ssl: false,
                remote_ca_path: Some(PathBuf::from("/tmp/system-ca.pem")),
                remote_timeout: Duration::from_secs(10),
            }
        );
    }

    #[rstest]
    fn test_default_without_lifecycles() {
        let mut env_getter = MockEnvGetter::new();
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let config = CfgParser::<MockEnvGetter>::parse_system_events_config(
            &mut Env::new(env_getter),
            false,
        );
        assert_eq!(
            config,
            SystemEventsConfig {
                enabled: true,
                log_level: None,
                quota_size: None,
                remote_verify_ssl: true,
                remote_ca_path: None,
                remote_timeout: Duration::from_secs(5),
            }
        );
    }

    #[rstest]
    fn test_default_with_lifecycles() {
        let mut env_getter = MockEnvGetter::new();
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let config =
            CfgParser::<MockEnvGetter>::parse_system_events_config(&mut Env::new(env_getter), true);
        assert_eq!(
            config,
            SystemEventsConfig {
                enabled: true,
                log_level: None,
                quota_size: None,
                remote_verify_ssl: true,
                remote_ca_path: None,
                remote_timeout: Duration::from_secs(5),
            }
        );
    }

    #[rstest]
    fn test_empty_ca_path_is_treated_as_none() {
        let mut env_getter = MockEnvGetter::new();
        env_getter
            .expect_get()
            .with(eq("RS_SYSTEM_EVENTS_ENABLED"))
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_SYSTEM_EVENTS_LOG_LEVEL"))
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_SYSTEM_EVENTS_QUOTA_SIZE"))
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_SYSTEM_EVENTS_REMOTE_VERIFY_SSL"))
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_SYSTEM_EVENTS_REMOTE_CA_PATH"))
            .return_const(Ok("".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_SYSTEM_EVENTS_REMOTE_TIMEOUT"))
            .return_const(Err(VarError::NotPresent));

        let config = CfgParser::<MockEnvGetter>::parse_system_events_config(
            &mut Env::new(env_getter),
            false,
        );

        assert_eq!(config.remote_ca_path, None);
    }
}
