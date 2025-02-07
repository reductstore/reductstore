// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::cfg::Cfg;
use crate::core::env::{Env, GetEnv};
use std::time::Duration;

const DEFAULT_BATCH_MAX_SIZE: u64 = 8000;
const DEFAULT_BATCH_MAX_RECORDS: usize = 100;
const DEFAULT_BATCH_MAX_METADATA_SIZE: usize = 512000;
const DEFAULT_BATCH_TIMEOUT_S: u64 = 5;
const DEFAULT_BATCH_RECORDS_TIMEOUT_S: u64 = 1;

#[derive(Clone, Debug)]
pub struct IoSettings {
    pub batch_max_size: u64,
    pub batch_max_records: usize,
    pub batch_max_metadata_size: usize,
    pub batch_timeout: Duration,
    pub batch_records_timeout: Duration,
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
