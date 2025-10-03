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
    df.batch_max_size = parse_size("#batch_size", dirs, df.batch_max_size)?;
    df.batch_max_records = parse_number("#batch_records", dirs, df.batch_max_records)?;
    df.batch_max_metadata_size = parse_size(
        "#batch_metadata_size",
        dirs,
        df.batch_max_metadata_size as u64,
    )? as usize;
    df.batch_timeout = parse_duration("#batch_timeout", dirs, df.batch_timeout)?;
    df.batch_records_timeout = parse_duration("#record_timeout", dirs, df.batch_records_timeout)?;

    // TODO: reserved
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
                "{} must be a positive integer or string representing a positive integer",
                directive
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

#[cfg(test)]
mod tests {
    use crate::storage::query::condition::{Directives, Value};
    use reduct_base::error::ReductError;
    use reduct_base::unprocessable_entity;
    use std::collections::HashMap;
    mod merge_io_config_from_directives {
        use super::*;
        use crate::cfg::io::IoConfig;
        use crate::storage::query::filters::when::io_cfg::merge_io_config_from_directives;
        use std::time::Duration;

        #[test]
        fn test_merge_io_config_from_directives() {
            let mut directives: Directives = HashMap::new();
            directives.insert("#batch_size".to_string(), vec![Value::Int(2048)]);
            directives.insert("#batch_timeout".to_string(), vec![Value::Duration(15000)]);
            directives.insert("#batch_records".to_string(), vec![Value::Int(100)]);
            directives.insert(
                "#batch_metadata_size".to_string(),
                vec![Value::String("1MB".to_string())],
            );
            directives.insert("#io_timeout".to_string(), vec![Value::Int(30)]);

            let default_cfg = IoConfig {
                batch_max_size: 1024,
                batch_max_records: 50,
                batch_max_metadata_size: 512,
                batch_timeout: Duration::from_secs(5),
                batch_records_timeout: Duration::from_secs(2),
                operation_timeout: Duration::from_secs(10),
            };

            let merged_cfg = merge_io_config_from_directives(&mut directives, default_cfg).unwrap();

            assert_eq!(merged_cfg.batch_max_size, 2048);
            assert_eq!(merged_cfg.batch_timeout, Duration::from_micros(15000));
            assert_eq!(merged_cfg.batch_max_records, 100);
            assert_eq!(merged_cfg.batch_max_metadata_size, 1_000_000);
            assert_eq!(merged_cfg.operation_timeout, Duration::from_secs(30));
            assert_eq!(merged_cfg.batch_records_timeout, Duration::from_secs(2));

            // Ensure directives are removed after processing
            assert!(!directives.contains_key("#batch_size"));
            assert!(!directives.contains_key("#batch_timeout"));
            assert!(!directives.contains_key("#batch_records"));
            assert!(!directives.contains_key("#batch_metadata_size"));
            assert!(!directives.contains_key("#io_timeout"));
        }

        #[test]
        fn test_merge_io_config_with_missing_directives() {
            let mut directives: Directives = HashMap::new();
            directives.insert("#batch_size".to_string(), vec![Value::Int(2048)]);

            let default_cfg = IoConfig {
                batch_max_size: 1024,
                batch_max_records: 50,
                batch_max_metadata_size: 512,
                batch_timeout: Duration::from_secs(5),
                batch_records_timeout: Duration::from_secs(2),
                operation_timeout: Duration::from_secs(10),
            };
            let merged_cfg = merge_io_config_from_directives(&mut directives, default_cfg).unwrap();
            assert_eq!(merged_cfg.batch_max_size, 2048);
            assert_eq!(merged_cfg.batch_timeout, Duration::from_secs(5));
            assert_eq!(merged_cfg.batch_max_records, 50);
            assert_eq!(merged_cfg.batch_max_metadata_size, 512);
            assert_eq!(merged_cfg.operation_timeout, Duration::from_secs(10));
            assert_eq!(merged_cfg.batch_records_timeout, Duration::from_secs(2));
            assert!(!directives.contains_key("#batch_size"));
        }
    }

    mod parse_size {
        use super::*;
        use crate::storage::query::filters::when::io_cfg::parse_size;
        use crate::storage::query::ReductError;

        #[test]
        fn test_parse_size_valid_integer() {
            let mut directives = build_directive("#batch_size", vec![Value::Int(2048)]);
            let result = parse_size("#batch_size", &mut directives, 1024).unwrap();
            assert_eq!(result, 2048);
            assert!(!directives.contains_key("#batch_size"));
        }

        #[test]
        fn test_parse_size_valid_string() {
            let mut directives: Directives = HashMap::new();
            directives.insert(
                "#batch_size".to_string(),
                vec![Value::String("2MB".to_string())],
            );

            let result = parse_size("#batch_size", &mut directives, 1024).unwrap();
            assert_eq!(result, 2_000_000);
            assert!(!directives.contains_key("#batch_size"));
        }

        #[test]
        fn test_parse_size_invalid_value() {
            let mut directives = build_directive("#batch_size", vec![Value::Float(2.5)]);

            let result = parse_size("#batch_size", &mut directives, 1024);
            assert_eq!(result, Err(unprocessable_entity!("#batch_size must be a positive integer or string representing a positive integer")));
            assert!(!directives.contains_key("#batch_size"));
        }

        #[test]
        fn test_parse_size_missing_directive() {
            let mut directives: Directives = HashMap::new();
            let result = parse_size("#batch_size", &mut directives, 1024).unwrap();
            assert_eq!(result, 1024);
        }

        #[test]
        fn test_parse_size_multiple_values() {
            let mut directives =
                build_directive("#batch_size", vec![Value::Int(2048), Value::Int(4096)]);

            let result = parse_size("#batch_size", &mut directives, 1024);
            assert_eq!(
                result,
                Err(unprocessable_entity!(
                    "#batch_size must be a single integer or string value"
                ))
            );
            assert!(!directives.contains_key("#batch_size"));
        }
    }

    mod parse_duration {
        use super::*;
        use crate::storage::query::filters::when::io_cfg::parse_duration;
        use std::time::Duration;

        #[test]
        fn test_parse_duration_valid_integer() {
            let mut directives = build_directive("#batch_timeout", vec![Value::Int(10)]);
            let result =
                parse_duration("#batch_timeout", &mut directives, Duration::from_secs(5)).unwrap();
            assert_eq!(result, Duration::from_secs(10));
            assert!(!directives.contains_key("#batch_timeout"));
        }

        #[test]
        fn test_parse_duration_valid_duration() {
            let mut directives = build_directive("#batch_timeout", vec![Value::Duration(15000)]);
            let result =
                parse_duration("#batch_timeout", &mut directives, Duration::from_secs(5)).unwrap();
            assert_eq!(result, Duration::from_micros(15000));
            assert!(!directives.contains_key("#batch_timeout"));
        }

        #[test]
        fn test_parse_duration_invalid_value() {
            let mut directives =
                build_directive("#batch_timeout", vec![Value::String("10s".to_string())]);

            let result = parse_duration("#batch_timeout", &mut directives, Duration::from_secs(5));
            assert_eq!(
                result,
                Err(unprocessable_entity!(
                    "#batch_timeout must be a positive integer or duration"
                ))
            );
            assert!(!directives.contains_key("#batch_timeout"));
        }

        #[test]
        fn test_parse_duration_missing_directive() {
            let mut directives: Directives = HashMap::new();
            let result =
                parse_duration("#batch_timeout", &mut directives, Duration::from_secs(5)).unwrap();
            assert_eq!(result, Duration::from_secs(5));
        }

        #[test]
        fn test_parse_duration_multiple_values() {
            let mut directives =
                build_directive("#batch_timeout", vec![Value::Int(10), Value::Int(20)]);

            let result = parse_duration("#batch_timeout", &mut directives, Duration::from_secs(5));
            assert_eq!(
                result,
                Err(unprocessable_entity!(
                    "#batch_timeout must be a single integer or duration value"
                ))
            );
            assert!(!directives.contains_key("#batch_timeout"));
        }
    }

    mod parse_number {
        use super::*;
        use crate::storage::query::filters::when::io_cfg::parse_number;

        #[test]
        fn test_parse_number_valid_integer() {
            let mut directives = build_directive("#batch_records", vec![Value::Int(100)]);
            let result = parse_number("#batch_records", &mut directives, 50).unwrap();
            assert_eq!(result, 100);
            assert!(!directives.contains_key("#batch_records"));
        }

        #[test]
        fn test_parse_number_invalid_value() {
            let mut directives = build_directive("#batch_records", vec![Value::Float(2.5)]);

            let result = parse_number("#batch_records", &mut directives, 50);
            assert_eq!(
                result,
                Err(unprocessable_entity!(
                    "#batch_records must be a positive integer"
                ))
            );
            assert!(!directives.contains_key("#batch_records"));
        }

        #[test]
        fn test_parse_number_missing_directive() {
            let mut directives: Directives = HashMap::new();
            let result = parse_number("#batch_records", &mut directives, 50).unwrap();
            assert_eq!(result, 50);
        }

        #[test]
        fn test_parse_number_multiple_values() {
            let mut directives =
                build_directive("#batch_records", vec![Value::Int(100), Value::Int(200)]);

            let result = parse_number("#batch_records", &mut directives, 50);
            assert_eq!(
                result,
                Err(unprocessable_entity!(
                    "#batch_records must be a single integer value"
                ))
            );
            assert!(!directives.contains_key("#batch_records"));
        }
    }

    fn build_directive(name: &str, values: Vec<Value>) -> Directives {
        let mut directives: Directives = HashMap::new();
        directives.insert(name.to_string(), values);
        directives
    }
}
