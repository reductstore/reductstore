// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::cfg::{parse_bool, CfgParser, ExtCfgBounds};
use crate::core::env::{Env, GetEnv};
use bytesize::ByteSize;
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
    pub(super) fn parse_audit_config(env: &mut Env<EnvGetter>) -> AuditConfig {
        AuditConfig {
            enabled: parse_bool(
                env.get_optional::<String>("RS_AUDIT_ENABLED"),
                DEFAULT_AUDIT_ENABLED,
            ),
            quota_size: env
                .get_optional::<ByteSize>("RS_AUDIT_QUOTA_SIZE")
                .map(|size| size.as_u64()),
            remote_verify_ssl: env
                .get_optional("RS_AUDIT_REMOTE_VERIFY_SSL")
                .unwrap_or(DEFAULT_AUDIT_REMOTE_VERIFY_SSL),
            remote_ca_path: env
                .get_optional::<String>("RS_AUDIT_REMOTE_CA_PATH")
                .and_then(|p| {
                    if p.is_empty() {
                        None
                    } else {
                        Some(PathBuf::from(p))
                    }
                }),
            remote_timeout: Duration::from_secs(
                env.get_optional("RS_AUDIT_REMOTE_TIMEOUT")
                    .unwrap_or(DEFAULT_AUDIT_REMOTE_TIMEOUT_S),
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
    fn test_audit_config() {
        let mut env_getter = MockEnvGetter::new();

        env_getter
            .expect_get()
            .with(eq("RS_AUDIT_ENABLED"))
            .return_const(Ok("true".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_AUDIT_QUOTA_SIZE"))
            .return_const(Ok("1MB".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_AUDIT_REMOTE_VERIFY_SSL"))
            .return_const(Ok("false".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_AUDIT_REMOTE_CA_PATH"))
            .return_const(Ok("/tmp/ca.pem".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_AUDIT_REMOTE_TIMEOUT"))
            .return_const(Ok("10".to_string()));

        let config = CfgParser::<MockEnvGetter>::parse_audit_config(&mut Env::new(env_getter));

        assert_eq!(
            config,
            AuditConfig {
                enabled: true,
                quota_size: Some(1_000_000),
                remote_verify_ssl: false,
                remote_ca_path: Some(PathBuf::from("/tmp/ca.pem")),
                remote_timeout: Duration::from_secs(10),
            }
        );
    }

    #[rstest]
    fn test_default_audit_config() {
        let mut env_getter = MockEnvGetter::new();
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let config = CfgParser::<MockEnvGetter>::parse_audit_config(&mut Env::new(env_getter));
        assert_eq!(config, AuditConfig::default());
    }
}
