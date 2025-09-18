// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::cfg::{CfgParser, DEFAULT_PORT};
use crate::core::env::{Env, GetEnv};
use std::time::Duration;
const DEFAULT_CONNECTION_TIMEOUT_S: u64 = 5;
const DEFAULT_REPLICATION_LOG_SIZE: usize = 1000000;

/// IO settings
#[derive(Clone, Debug, PartialEq)]
pub struct ReplicationConfig {
    /// Maximum time to wait for connection with the replication server
    pub connection_timeout: Duration,
    /// Size of replication log in records
    pub replication_log_size: usize,
    /// Listening port (to check if we replicate to ourselves)
    pub listening_port: u16,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        ReplicationConfig {
            connection_timeout: Duration::from_secs(DEFAULT_CONNECTION_TIMEOUT_S),
            replication_log_size: DEFAULT_REPLICATION_LOG_SIZE,
            listening_port: DEFAULT_PORT,
        }
    }
}

impl<EnvGetter: GetEnv> CfgParser<EnvGetter> {
    pub(super) fn parse_replication_config(
        env: &mut Env<EnvGetter>,
        listening_port: u16,
    ) -> ReplicationConfig {
        ReplicationConfig {
            connection_timeout: Duration::from_secs(
                env.get_optional("RS_REPLICATION_TIMEOUT")
                    .unwrap_or(DEFAULT_CONNECTION_TIMEOUT_S),
            ),
            replication_log_size: env
                .get_optional("RS_REPLICATION_LOG_SIZE")
                .unwrap_or(DEFAULT_REPLICATION_LOG_SIZE),
            listening_port,
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
    fn test_replication_config() {
        let mut env_getter = MockEnvGetter::new();

        env_getter
            .expect_get()
            .with(eq("RS_REPLICATION_TIMEOUT"))
            .return_const(Ok("10".to_string()));

        env_getter
            .expect_get()
            .with(eq("RS_REPLICATION_LOG_SIZE"))
            .return_const(Ok("500".to_string()));

        let replication_settings = ReplicationConfig {
            connection_timeout: Duration::from_secs(10),
            replication_log_size: 500,
            listening_port: 8000,
        };

        assert_eq!(
            replication_settings,
            CfgParser::<MockEnvGetter>::parse_replication_config(&mut Env::new(env_getter), 8000)
        );
    }

    #[rstest]
    fn test_default_replication_config() {
        let mut env_getter = MockEnvGetter::new();

        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let replication_settings = ReplicationConfig::default();

        assert_eq!(
            replication_settings,
            CfgParser::<MockEnvGetter>::parse_replication_config(
                &mut Env::new(env_getter),
                DEFAULT_PORT
            )
        );
    }
}
