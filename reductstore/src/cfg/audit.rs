// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::cfg::{CfgParser, ExtCfgBounds};
use crate::core::env::{Env, GetEnv};
use std::path::PathBuf;
use std::time::Duration;

const DEFAULT_AUDIT_ENABLED: bool = false;
const DEFAULT_AUDIT_REMOTE_VERIFY_SSL: bool = true;
const DEFAULT_AUDIT_REMOTE_TIMEOUT_S: u64 = 5;

/// Audit settings
#[derive(Clone, Debug, PartialEq)]
pub struct AuditConfig {
    /// Enable/disable audit collection and persistence.
    pub enabled: bool,
    /// Optional quota size for the `$audit` bucket.
    pub quota_size: Option<u64>,
    /// Verify SSL certificates for replica audit forwarding.
    pub remote_verify_ssl: bool,
    /// Optional custom CA certificate path for replica audit forwarding.
    pub remote_ca_path: Option<PathBuf>,
    /// Timeout for replica audit HTTP communication.
    pub remote_timeout: Duration,
}

impl Default for AuditConfig {
    fn default() -> Self {
        AuditConfig {
            enabled: DEFAULT_AUDIT_ENABLED,
            quota_size: None,
            remote_verify_ssl: DEFAULT_AUDIT_REMOTE_VERIFY_SSL,
            remote_ca_path: None,
            remote_timeout: Duration::from_secs(DEFAULT_AUDIT_REMOTE_TIMEOUT_S),
        }
    }
}

impl<EnvGetter: GetEnv, ExtCfg: ExtCfgBounds> CfgParser<EnvGetter, ExtCfg> {
    pub(super) fn parse_audit_config(_env: &mut Env<EnvGetter>, _api_token: &str) -> AuditConfig {
        AuditConfig::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::tests::MockEnvGetter;
    use rstest::rstest;

    #[rstest]
    fn test_audit_config_is_default_even_if_env_present() {
        let mut env_getter = MockEnvGetter::new();
        env_getter.expect_get().times(0);
        let config =
            CfgParser::<MockEnvGetter>::parse_audit_config(&mut Env::new(env_getter), "token");
        assert_eq!(config, AuditConfig::default());
    }

    #[rstest]
    fn test_default_audit_config_without_api_token() {
        let mut env_getter = MockEnvGetter::new();
        env_getter.expect_get().times(0);

        let config = CfgParser::<MockEnvGetter>::parse_audit_config(&mut Env::new(env_getter), "");
        assert_eq!(config, AuditConfig::default());
    }

    #[rstest]
    fn test_default_audit_config_with_api_token() {
        let mut env_getter = MockEnvGetter::new();
        env_getter.expect_get().times(0);

        let config =
            CfgParser::<MockEnvGetter>::parse_audit_config(&mut Env::new(env_getter), "token");
        assert_eq!(config, AuditConfig::default());
    }
}
