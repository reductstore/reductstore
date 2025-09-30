// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::cfg::io::IoConfig;
use crate::storage::query::condition::{Directives, Value};
use bytesize::ByteSize;
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;
use std::str::FromStr;
use std::time::Duration;

pub(super) fn merge_io_config_from_directives(
    dirs: &mut Directives,
    mut df: IoConfig,
) -> Result<IoConfig, ReductError> {
    /*
        "#batch_size",
    "#batch_records",
    "#batch_metadata_size",
    "#batch_timeout",
    "#record_timeout",
    "#io_timeout",
     */
    df.batch_max_size = parse_size("#batch_size", dirs, df.batch_max_size)?;
    df.batch_max_records = parse_number("#batch_records", dirs, df.batch_max_records)?;
    df.batch_max_size = parse_size(
        "#batch_metadata_size",
        dirs,
        df.batch_max_metadata_size as u64,
    )?;
    df.batch_timeout = parse_duration("#batch_timeout", dirs, df.batch_timeout)?;
    df.batch_records_timeout = parse_duration("#record_timeout", dirs, df.batch_records_timeout)?;
    df.operation_timeout = parse_duration("#io_timeout", dirs, df.operation_timeout)?;

    Ok(df)
}

fn parse_size(
    directive: &str,
    directives: &mut Directives,
    default: u64,
) -> Result<u64, ReductError> {
    if let Some(io_cfg) = directives.remove(directive) {
        if io_cfg.len() != 1 {
            return Err(unprocessable_entity!(
                "{} must be a single integer or string value",
                directive
            ));
        }

        let batch_size = match &io_cfg[0] {
            Value::Int(i) if i >= &0 => *i as u64,
            Value::String(s) => {
                let parsed = ByteSize::from_str(&s)
                    .map_err(|e| {
                        unprocessable_entity!(
                            "{} must be a positive integer or string: {}",
                            directive,
                            e
                        )
                    })?
                    .as_u64();

                parsed
            }
            _ => {
                return Err(unprocessable_entity!(
                "#batch_size must be a positive integer or string representing a positive integer"
            ))
            }
        };

        Ok(batch_size)
    } else {
        Ok(default)
    }
}

fn parse_duration(
    directive: &str,
    directives: &mut Directives,
    default: Duration,
) -> Result<Duration, ReductError> {
    if let Some(io_cfg) = directives.remove(directive) {
        if io_cfg.len() != 1 {
            return Err(unprocessable_entity!(
                "{} must be a single integer or duration value",
                directive
            ));
        }

        let batch_timeout = match io_cfg[0] {
            Value::Int(i) if i >= 0 => Duration::from_secs(i as u64),
            Value::Duration(i) if i >= 0 => Duration::from_micros(i as u64),
            _ => {
                return Err(unprocessable_entity!(
                    "{} must be a positive integer or duration",
                    directive
                ))
            }
        };

        Ok(batch_timeout)
    } else {
        Ok(default)
    }
}

fn parse_number(
    directive: &str,
    directives: &mut Directives,
    default: usize,
) -> Result<usize, ReductError> {
    if let Some(io_cfg) = directives.remove(directive) {
        if io_cfg.len() != 1 {
            return Err(unprocessable_entity!(
                "{} must be a single integer value",
                directive
            ));
        }

        let number = match io_cfg[0] {
            Value::Int(i) if i >= 0 => i as u64,
            _ => {
                return Err(unprocessable_entity!(
                    "{} must be a positive integer",
                    directive
                ))
            }
        };

        Ok(number as usize)
    } else {
        Ok(default)
    }
}
