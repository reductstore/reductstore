// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::cfg::{CfgParser, ExtCfgBounds};
use crate::core::env::{Env, GetEnv};
use bytesize::ByteSize;
use std::time::Duration;

const DEFAULT_BATCH_MAX_SIZE: u64 = 8000000;
const DEFAULT_BATCH_MAX_RECORDS: usize = 85;
const DEFAULT_BATCH_MAX_METADATA_SIZE: u64 = 512000;
const DEFAULT_BATCH_TIMEOUT_S: u64 = 5;
const DEFAULT_BATCH_RECORDS_TIMEOUT_S: u64 = 1;
const DEFAULT_OPERATION_TIMEOUT_S: u64 = 30;

/// IO configuration
#[derive(Clone, Debug, PartialEq)]
pub struct IoConfig {
    /// Maximum size of a batch in bytes sent from the server
    pub batch_max_size: u64,
    /// Maximum number of records in a batch sent from the server and client
    pub batch_max_records: usize,
    /// Maximum size of metadata in a batch sent from the server and client
    pub batch_max_metadata_size: usize,
    /// Maximum time to wait for a batch to be filled before sending it
    pub batch_timeout: Duration,
    /// Maximum time to wait for a record to be added to a batch before sending it
    pub batch_records_timeout: Duration,
    /// Maximum time to wait for an IO operation to complete
    pub operation_timeout: Duration,
}

impl Default for IoConfig {
    fn default() -> Self {
        IoConfig {
            batch_max_size: DEFAULT_BATCH_MAX_SIZE,
            batch_max_records: DEFAULT_BATCH_MAX_RECORDS,
            batch_max_metadata_size: DEFAULT_BATCH_MAX_METADATA_SIZE as usize,
            batch_timeout: Duration::from_secs(DEFAULT_BATCH_TIMEOUT_S),
            batch_records_timeout: Duration::from_secs(DEFAULT_BATCH_RECORDS_TIMEOUT_S),
            operation_timeout: Duration::from_secs(DEFAULT_OPERATION_TIMEOUT_S),
        }
    }
}

impl<EnvGetter: GetEnv, ExtCfg: ExtCfgBounds> CfgParser<EnvGetter, ExtCfg> {
    pub(super) fn parse_io_config(env: &mut Env<EnvGetter>) -> IoConfig {
        IoConfig {
            batch_max_size: env
                .get_optional::<ByteSize>("RS_IO_BATCH_MAX_SIZE")
                .unwrap_or(ByteSize::b(DEFAULT_BATCH_MAX_SIZE))
                .as_u64(),
            batch_max_records: env
                .get_optional("RS_IO_BATCH_MAX_RECORDS")
                .unwrap_or(DEFAULT_BATCH_MAX_RECORDS),
            batch_max_metadata_size: env
                .get_optional::<ByteSize>("RS_IO_BATCH_MAX_METADATA_SIZE")
                .unwrap_or(ByteSize::b(DEFAULT_BATCH_MAX_METADATA_SIZE))
                .as_u64() as usize,
            batch_timeout: Duration::from_secs(
                env.get_optional("RS_IO_BATCH_TIMEOUT")
                    .unwrap_or(DEFAULT_BATCH_TIMEOUT_S),
            ),
            batch_records_timeout: Duration::from_secs(
                env.get_optional("RS_IO_BATCH_RECORDS_TIMEOUT")
                    .unwrap_or(DEFAULT_BATCH_RECORDS_TIMEOUT_S),
            ),
            operation_timeout: Duration::from_secs(
                env.get_optional("RS_IO_OPERATION_TIMEOUT")
                    .unwrap_or(DEFAULT_OPERATION_TIMEOUT_S),
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
    fn test_io_config() {
        let mut env_getter = MockEnvGetter::new();

        env_getter
            .expect_get()
            .with(eq("RS_IO_BATCH_MAX_SIZE"))
            .return_const(Ok("1MB".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_IO_BATCH_MAX_RECORDS"))
            .return_const(Ok("10".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_IO_BATCH_MAX_METADATA_SIZE"))
            .return_const(Ok("1KB".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_IO_BATCH_TIMEOUT"))
            .return_const(Ok("10".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_IO_BATCH_RECORDS_TIMEOUT"))
            .return_const(Ok("5".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_IO_OPERATION_TIMEOUT"))
            .return_const(Ok("60".to_string()));

        let io_settings = IoConfig {
            batch_max_size: 1000000,
            batch_max_records: 10,
            batch_max_metadata_size: 1000,
            batch_timeout: Duration::from_secs(10),
            batch_records_timeout: Duration::from_secs(5),
            operation_timeout: Duration::from_secs(60),
        };

        assert_eq!(
            io_settings,
            CfgParser::<MockEnvGetter>::parse_io_config(&mut Env::new(env_getter))
        );
    }

    #[rstest]
    fn test_default_io_config() {
        let mut env_getter = MockEnvGetter::new();

        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let io_settings = IoConfig::default();

        assert_eq!(
            io_settings,
            CfgParser::<MockEnvGetter>::parse_io_config(&mut Env::new(env_getter))
        );
    }
}
