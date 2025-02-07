// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::cfg::Cfg;
use crate::core::env::{Env, GetEnv};
use std::time::Duration;

const DEFAULT_BATCH_MAX_SIZE: u64 = 8000;
const DEFAULT_BATCH_MAX_RECORDS: usize = 85;
const DEFAULT_BATCH_MAX_METADATA_SIZE: usize = 512000;
const DEFAULT_BATCH_TIMEOUT_S: u64 = 5;
const DEFAULT_BATCH_RECORDS_TIMEOUT_S: u64 = 1;

/// IO settings
#[derive(Clone, Debug)]
pub struct IoSettings {
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
}

impl Default for IoSettings {
    fn default() -> Self {
        IoSettings {
            batch_max_size: DEFAULT_BATCH_MAX_SIZE,
            batch_max_records: DEFAULT_BATCH_MAX_RECORDS,
            batch_max_metadata_size: DEFAULT_BATCH_MAX_METADATA_SIZE,
            batch_timeout: Duration::from_secs(DEFAULT_BATCH_TIMEOUT_S),
            batch_records_timeout: Duration::from_secs(DEFAULT_BATCH_RECORDS_TIMEOUT_S),
        }
    }
}

impl<EnvGetter: GetEnv> Cfg<EnvGetter> {
    pub(super) fn parse_io_settings(env: &mut Env<EnvGetter>) -> IoSettings {
        IoSettings {
            batch_max_size: env
                .get_optional("RS_IO_BATCH_MAX_SIZE")
                .unwrap_or(DEFAULT_BATCH_MAX_SIZE),
            batch_max_records: env
                .get_optional("RS_IO_BATCH_MAX_RECORDS")
                .unwrap_or(DEFAULT_BATCH_MAX_RECORDS),
            batch_max_metadata_size: env
                .get_optional("RS_IO_BATCH_MAX_METADATA_SIZE")
                .unwrap_or(DEFAULT_BATCH_MAX_METADATA_SIZE),
            batch_timeout: Duration::from_secs(
                env.get_optional("RS_IO_BATCH_TIMEOUT")
                    .unwrap_or(DEFAULT_BATCH_TIMEOUT_S),
            ),
            batch_records_timeout: Duration::from_secs(
                env.get_optional("RS_IO_BATCH_RECORDS_TIMEOUT_S")
                    .unwrap_or(DEFAULT_BATCH_RECORDS_TIMEOUT_S),
            ),
        }
    }
}
