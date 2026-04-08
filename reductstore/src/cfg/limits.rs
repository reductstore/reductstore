// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::limits::{LimitsConfig, WindowLimit};
use crate::cfg::{CfgParser, ExtCfgBounds};
use crate::core::duration::parse_duration_to_micros;
use crate::core::env::{Env, GetEnv};
use bytesize::ByteSize;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

const ONE_HOUR_US: u64 = 3_600_000_000;

#[derive(Clone, Debug, PartialEq, Eq)]
struct ApiRequestsRateLimit {
    limit: WindowLimit,
    raw: String,
}

impl Default for ApiRequestsRateLimit {
    fn default() -> Self {
        Self {
            limit: WindowLimit::new(0, std::time::Duration::from_secs(3600)),
            raw: String::new(),
        }
    }
}

impl Display for ApiRequestsRateLimit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.raw)
    }
}

impl FromStr for ApiRequestsRateLimit {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let raw = value.trim().to_string();
        let limit = parse_rate_limit(&raw, parse_request_amount)?;
        Ok(Self { limit, raw })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ByteRateLimit {
    limit: WindowLimit,
    raw: String,
}

impl Default for ByteRateLimit {
    fn default() -> Self {
        Self {
            limit: WindowLimit::new(0, std::time::Duration::from_secs(3600)),
            raw: String::new(),
        }
    }
}

impl Display for ByteRateLimit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.raw)
    }
}

impl FromStr for ByteRateLimit {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let raw = value.trim().to_string();
        let limit = parse_rate_limit(&raw, parse_byte_amount)?;
        Ok(Self { limit, raw })
    }
}

impl<EnvGetter: GetEnv, ExtCfg: ExtCfgBounds> CfgParser<EnvGetter, ExtCfg> {
    pub(super) fn parse_limits_config(env: &mut Env<EnvGetter>) -> LimitsConfig {
        LimitsConfig {
            api_requests_per_window: env
                .get_optional::<ApiRequestsRateLimit>("RS_RATE_LIMIT_API")
                .map(|v| v.limit),
            ingress_bytes_per_window: env
                .get_optional::<ByteRateLimit>("RS_RATE_LIMIT_INGRESS")
                .map(|v| v.limit),
            egress_bytes_per_window: env
                .get_optional::<ByteRateLimit>("RS_RATE_LIMIT_EGRESS")
                .map(|v| v.limit),
        }
    }
}

fn parse_rate_limit(
    value: &str,
    parse_amount: fn(&str) -> Result<u64, String>,
) -> Result<WindowLimit, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("Rate limit cannot be empty".to_string());
    }

    let (amount_raw, period_us) = if let Some((amount_raw, period_raw)) = value.rsplit_once('/') {
        (
            amount_raw.trim(),
            parse_period_to_micros(period_raw.trim())?,
        )
    } else {
        (value, ONE_HOUR_US)
    };

    let amount = parse_amount(amount_raw)?;
    Ok(WindowLimit::new(
        amount,
        std::time::Duration::from_micros(period_us),
    ))
}

fn parse_request_amount(value: &str) -> Result<u64, String> {
    let value = value.trim();
    let value = value.strip_suffix("req").unwrap_or(value).trim();
    value
        .parse::<u64>()
        .map_err(|e| format!("Invalid request rate limit: {}", e))
}

fn parse_byte_amount(value: &str) -> Result<u64, String> {
    ByteSize::from_str(value)
        .map(|size| size.as_u64())
        .map_err(|e| format!("Invalid byte rate limit: {}", e))
}

fn parse_period_to_micros(value: &str) -> Result<u64, String> {
    if value.trim().is_empty() {
        return Err("Rate period cannot be empty".to_string());
    }

    // Support shorthand periods like `/s` and `/m`.
    let normalized_value = if value.chars().all(|ch| ch.is_ascii_alphabetic()) {
        format!("1{}", value)
    } else {
        value.to_string()
    };

    let usecs = parse_duration_to_micros(&normalized_value)
        .map_err(|e| format!("Invalid rate period '{}': {}", value, e.message))?;
    if usecs <= 0 {
        return Err(format!(
            "Rate period '{}' must be a positive duration",
            value
        ));
    }

    Ok(usecs as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::tests::MockEnvGetter;
    use mockall::predicate::eq;
    use rstest::rstest;
    use std::env::VarError;

    #[rstest]
    fn test_limits_config_with_hourly_suffix() {
        let mut env_getter = MockEnvGetter::new();
        env_getter
            .expect_get()
            .with(eq("RS_RATE_LIMIT_API"))
            .return_const(Ok("100000req/h".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_RATE_LIMIT_INGRESS"))
            .return_const(Ok("10GB/h".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_RATE_LIMIT_EGRESS"))
            .return_const(Ok("512MB/h".to_string()));

        assert_eq!(
            CfgParser::<MockEnvGetter>::parse_limits_config(&mut Env::new(env_getter)),
            LimitsConfig {
                api_requests_per_window: Some(WindowLimit::new(
                    100_000,
                    std::time::Duration::from_secs(3600)
                )),
                ingress_bytes_per_window: Some(WindowLimit::new(
                    10_000_000_000,
                    std::time::Duration::from_secs(3600)
                )),
                egress_bytes_per_window: Some(WindowLimit::new(
                    512_000_000,
                    std::time::Duration::from_secs(3600)
                )),
            }
        );
    }

    #[rstest]
    fn test_limits_config_with_rate_units() {
        let mut env_getter = MockEnvGetter::new();
        env_getter
            .expect_get()
            .with(eq("RS_RATE_LIMIT_API"))
            .return_const(Ok("100req/s".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_RATE_LIMIT_INGRESS"))
            .return_const(Ok("10MB/m".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_RATE_LIMIT_EGRESS"))
            .return_const(Ok("2GB/s".to_string()));

        assert_eq!(
            CfgParser::<MockEnvGetter>::parse_limits_config(&mut Env::new(env_getter)),
            LimitsConfig {
                api_requests_per_window: Some(WindowLimit::new(
                    100,
                    std::time::Duration::from_secs(1)
                )),
                ingress_bytes_per_window: Some(WindowLimit::new(
                    10_000_000,
                    std::time::Duration::from_secs(60)
                )),
                egress_bytes_per_window: Some(WindowLimit::new(
                    2_000_000_000,
                    std::time::Duration::from_secs(1)
                )),
            }
        );
    }

    #[rstest]
    fn test_limits_config_with_unit_shortcuts() {
        let mut env_getter = MockEnvGetter::new();
        env_getter
            .expect_get()
            .with(eq("RS_RATE_LIMIT_API"))
            .return_const(Ok("120req/m".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_RATE_LIMIT_INGRESS"))
            .return_const(Ok("10MB/s".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_RATE_LIMIT_EGRESS"))
            .return_const(Ok("10MB/ms".to_string()));

        assert_eq!(
            CfgParser::<MockEnvGetter>::parse_limits_config(&mut Env::new(env_getter)),
            LimitsConfig {
                api_requests_per_window: Some(WindowLimit::new(
                    120,
                    std::time::Duration::from_secs(60)
                )),
                ingress_bytes_per_window: Some(WindowLimit::new(
                    10_000_000,
                    std::time::Duration::from_secs(1)
                )),
                egress_bytes_per_window: Some(WindowLimit::new(
                    10_000_000,
                    std::time::Duration::from_millis(1)
                )),
            }
        );
    }

    #[rstest]
    fn test_limits_config_without_hourly_suffix() {
        let mut env_getter = MockEnvGetter::new();
        env_getter
            .expect_get()
            .with(eq("RS_RATE_LIMIT_API"))
            .return_const(Ok("123".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_RATE_LIMIT_INGRESS"))
            .return_const(Ok("2KB".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_RATE_LIMIT_EGRESS"))
            .return_const(Ok("4096".to_string()));

        assert_eq!(
            CfgParser::<MockEnvGetter>::parse_limits_config(&mut Env::new(env_getter)),
            LimitsConfig {
                api_requests_per_window: Some(WindowLimit::new(
                    123,
                    std::time::Duration::from_secs(3600)
                )),
                ingress_bytes_per_window: Some(WindowLimit::new(
                    2_000,
                    std::time::Duration::from_secs(3600)
                )),
                egress_bytes_per_window: Some(WindowLimit::new(
                    4_096,
                    std::time::Duration::from_secs(3600)
                )),
            }
        );
    }

    #[rstest]
    fn test_limits_config_defaults_to_unlimited_when_missing_or_invalid() {
        let mut env_getter = MockEnvGetter::new();
        env_getter
            .expect_get()
            .with(eq("RS_RATE_LIMIT_API"))
            .return_const(Ok("wrong-format".to_string()));
        env_getter
            .expect_get()
            .with(eq("RS_RATE_LIMIT_INGRESS"))
            .return_const(Err(VarError::NotPresent));
        env_getter
            .expect_get()
            .with(eq("RS_RATE_LIMIT_EGRESS"))
            .return_const(Ok("also-wrong".to_string()));

        assert_eq!(
            CfgParser::<MockEnvGetter>::parse_limits_config(&mut Env::new(env_getter)),
            LimitsConfig::default()
        );
    }

    #[rstest]
    fn api_requests_rate_limit_trimmed_roundtrip_display() {
        let parsed = ApiRequestsRateLimit::from_str(" 100req/s ").unwrap();
        assert_eq!(parsed.limit.amount, 100);
        assert_eq!(parsed.limit.window, std::time::Duration::from_secs(1));
        assert_eq!(parsed.to_string(), "100req/s");
    }

    #[rstest]
    fn byte_rate_limit_trimmed_roundtrip_display() {
        let parsed = ByteRateLimit::from_str(" 10MB/m ").unwrap();
        assert_eq!(parsed.limit.amount, 10_000_000);
        assert_eq!(parsed.limit.window, std::time::Duration::from_secs(60));
        assert_eq!(parsed.to_string(), "10MB/m");
    }

    #[rstest]
    fn api_requests_rate_limit_default_is_unset() {
        let default = ApiRequestsRateLimit::default();
        assert_eq!(default.limit.amount, 0);
        assert_eq!(default.limit.window, std::time::Duration::from_secs(3600));
        assert_eq!(default.to_string(), "");
    }

    #[rstest]
    fn byte_rate_limit_default_is_unset() {
        let default = ByteRateLimit::default();
        assert_eq!(default.limit.amount, 0);
        assert_eq!(default.limit.window, std::time::Duration::from_secs(3600));
        assert_eq!(default.to_string(), "");
    }

    #[rstest]
    fn parse_rate_limit_rejects_empty_literal() {
        let err = parse_rate_limit("", parse_request_amount).err().unwrap();
        assert!(err.contains("cannot be empty"));
    }

    #[rstest]
    fn parse_rate_limit_rejects_empty_period() {
        let err = parse_rate_limit("10req/", parse_request_amount)
            .err()
            .unwrap();
        assert!(err.contains("Rate period cannot be empty"));
    }

    #[rstest]
    fn parse_request_amount_supports_req_suffix() {
        assert_eq!(parse_request_amount("42req").unwrap(), 42);
    }

    #[rstest]
    fn parse_request_amount_rejects_invalid_value() {
        let err = parse_request_amount("oops").err().unwrap();
        assert!(err.contains("Invalid request rate limit"));
    }

    #[rstest]
    fn parse_byte_amount_rejects_invalid_value() {
        let err = parse_byte_amount("oops").err().unwrap();
        assert!(err.contains("Invalid byte rate limit"));
    }

    #[rstest]
    fn parse_period_to_micros_rejects_non_positive_values() {
        let zero = parse_period_to_micros("0s").err().unwrap();
        assert!(zero.contains("must be a positive duration"));

        let negative = parse_period_to_micros("-1s").err().unwrap();
        assert!(negative.contains("must be a positive duration"));
    }

    #[rstest]
    fn parse_period_to_micros_rejects_invalid_format() {
        let err = parse_period_to_micros("??").err().unwrap();
        assert!(err.contains("Invalid rate period"));
    }
}
