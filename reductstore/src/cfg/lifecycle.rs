// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::cfg::{CfgParser, ExtCfgBounds};
use crate::core::env::{Env, GetEnv};
use std::time::Duration;

const DEFAULT_LIFECYCLE_INTERVAL_SECS: u64 = 3600;

/// Lifecycle worker configuration.
#[derive(Clone, Debug, PartialEq)]
pub struct LifecycleConfig {
    /// Interval between lifecycle worker runs.
    pub interval: Duration,
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        LifecycleConfig {
            interval: Duration::from_secs(DEFAULT_LIFECYCLE_INTERVAL_SECS),
        }
    }
}

impl<EnvGetter: GetEnv, ExtCfg: ExtCfgBounds> CfgParser<EnvGetter, ExtCfg> {
    pub(super) fn parse_lifecycle_config(env: &mut Env<EnvGetter>) -> LifecycleConfig {
        LifecycleConfig {
            interval: Duration::from_secs(
                env.get_optional("RS_LIFECYCLE_INTERVAL")
                    .unwrap_or(DEFAULT_LIFECYCLE_INTERVAL_SECS),
            ),
        }
    }
}
