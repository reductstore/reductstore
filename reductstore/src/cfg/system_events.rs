// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::cfg::{parse_bool, CfgParser, ExtCfgBounds};
use crate::core::env::{Env, GetEnv};
use bytesize::ByteSize;

const DEFAULT_SYSTEM_EVENTS_ENABLED: bool = true;

#[derive(Clone, Debug, PartialEq)]
pub struct SystemEventsConfig {
    pub enabled: bool,
    pub quota_size: Option<u64>,
}

impl Default for SystemEventsConfig {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_SYSTEM_EVENTS_ENABLED,
            quota_size: None,
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
            quota_size: env
                .get_optional::<ByteSize>("RS_SYSTEM_EVENTS_QUOTA_SIZE")
                .map(|size| size.as_u64()),
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
            .with(eq("RS_SYSTEM_EVENTS_QUOTA_SIZE"))
            .return_const(Ok("10MB".to_string()));

        let config = CfgParser::<MockEnvGetter>::parse_system_events_config(
            &mut Env::new(env_getter),
            false,
        );

        assert_eq!(
            config,
            SystemEventsConfig {
                enabled: true,
                quota_size: Some(10_000_000),
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
                quota_size: None,
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
                quota_size: None,
            }
        );
    }
}
