// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1
use crate::core::duration::parse_duration_usecs;
use crate::storage::query::condition::value::Value;
use reduct_base::error::ReductError;

pub(super) use crate::core::duration::fmt_duration;

#[cfg(test)]
use crate::core::duration::parse_single_duration;

/// Parses a duration string containing multiple parts (e.g., "100ms 500us") into a `Value::Duration`.
/// # Arguments
///
/// * `duration_string` - The string representing the duration, which can contain multiple parts separated by whitespace.
///
/// # Returns
///
/// A `Result` containing the total duration as `Value::Duration` or an error if the string is invalid.
pub(crate) fn parse_duration(duration_string: &str) -> Result<Value, ReductError> {
    parse_duration_usecs(duration_string).map(Value::Duration)
}

#[cfg(test)]
mod tests {
    use super::*;
    use reduct_base::unprocessable_entity;
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
        assert_eq!(
            parse_single_duration("").err().unwrap(),
            unprocessable_entity!("Duration literal cannot be empty")
        );
        assert_eq!(
            parse_single_duration("100xyz").err().unwrap(),
            unprocessable_entity!("Invalid duration unit: xyz")
        );
        assert_eq!(
            parse_single_duration("abc").err().unwrap(),
            unprocessable_entity!("Invalid duration value: abc")
        );

        assert_eq!(
            parse_single_duration("2.5m").err().unwrap(),
            unprocessable_entity!("Invalid duration value: 2.5m")
        );
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
        assert_eq!(
            parse_duration("").err().unwrap(),
            unprocessable_entity!("Duration literal cannot be empty")
        );
        assert_eq!(
            parse_duration("1h 100xyz").err().unwrap(),
            unprocessable_entity!("Invalid duration unit: xyz")
        );
        assert_eq!(
            parse_duration("1h,2m").err().unwrap(),
            unprocessable_entity!("Invalid duration unit: h,m")
        );
    }
}
