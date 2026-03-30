// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0
use crate::core::duration::parse_duration_to_micros;
use crate::storage::query::condition::value::Value;
use reduct_base::error::ReductError;

/// Parses a duration string containing multiple parts (e.g., "100ms 500us") into a `Value::Duration`.
/// # Arguments
///
/// * `duration_string` - The string representing the duration, which can contain multiple parts separated by whitespace.
///
/// # Returns
///
/// A `Result` containing the total duration as `Value::Duration` or an error if the string is invalid.
pub(crate) fn parse_duration(duration_string: &str) -> Result<Value, ReductError> {
    Ok(Value::Duration(parse_duration_to_micros(duration_string)?))
}

/// Formats a duration in microseconds into a human-readable string.
///
/// # Arguments
///
/// * `usec` - The duration in microseconds.
/// * `f` - The formatter to write the output to.
///
/// # Returns
///
/// A `Result` indicating success or failure of the formatting operation.
pub(super) fn fmt_duration(mut usec: i64, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let mut parts = Vec::new();
    let units = [
        ("d", 86_400_000_000), // days
        ("h", 3_600_000_000),  // hours
        ("m", 60_000_000),     // minutes
        ("s", 1_000_000),      // seconds
        ("ms", 1000),          // milliseconds
        ("us", 1),             // microseconds
    ];
    for &(unit, unit_seconds) in &units {
        if usec.abs() >= unit_seconds {
            let value = usec / unit_seconds;
            parts.push(format!("{}{}", value, unit));
            usec -= value * unit_seconds;
        }
    }
    if parts.is_empty() {
        parts.push("0us".to_string());
    }
    write!(f, "{}", parts.join(" "))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(0, "0us")]
    #[case(1, "1us")]
    #[case(-1, "-1us")]
    #[case(100, "100us")]
    #[case(100_000, "100ms")]
    #[case(1_000_000, "1s")]
    #[case(-1_000_000, "-1s")]
    #[case(60_000_000, "1m")]
    #[case(3_600_000_000, "1h")]
    #[case(86_400_000_000, "1d")]
    #[case(86_400_000_000 + 3_600_000_000, "1d 1h")]
    #[case(86_400_000_000 - 3_600_000_000 + 5, "23h 5us")]
    fn test_fmt_duration(#[case] value: i64, #[case] literal: &str) {
        let value = Value::Duration(value);
        assert_eq!(parse_duration(literal).unwrap(), value);
        assert_eq!(value.to_string(), literal);
    }

    #[rstest]
    fn test_parse_invalid_duration() {
        assert!(parse_duration("").is_err());
        assert!(parse_duration("100xyz").is_err());
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("2.5m").is_err());
    }

    #[rstest]
    fn test_parse_duration() {
        assert_eq!(
            parse_duration("100ms 500us").unwrap(),
            Value::Duration(100_500)
        );
        assert_eq!(
            parse_duration("1h -30m").unwrap(),
            Value::Duration(1_800_000_000)
        );
        assert_eq!(
            parse_duration("2d 3h").unwrap(),
            Value::Duration(183_600_000_000)
        );
    }

    #[rstest]
    fn test_invalid_duration() {
        assert!(parse_duration("").is_err());
        assert!(parse_duration("1h 100xyz").is_err());
        assert!(parse_duration("1h,2m").is_err());
    }
}
