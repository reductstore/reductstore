// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1
use crate::storage::query::condition::value::Value;
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// Parses a duration string into a `Value::Duration`.
///
/// # Arguments
///
/// * `duration_string` - The string representing the duration, e.g., "100ms", "2.5m", "1h".
///
/// # Returns
///
/// A `Result` containing the parsed duration as i64 (in microseconds) or an error if the string is invalid.
fn parse_single_duration(duration_string: &str) -> Result<i64, ReductError> {
    if duration_string.trim().is_empty() {
        return Err(unprocessable_entity!("Duration literal cannot be empty"));
    }

    let duration_string = duration_string.trim();
    let (num_part, unit_part) = duration_string
        .chars()
        .partition::<String, _>(|c| c.is_digit(10) || *c == '.' || *c == '-');
    let value: i64 = num_part
        .parse()
        .map_err(|_| unprocessable_entity!("Invalid duration value: {}", duration_string))?;

    let unit = unit_part.as_str();
    let seconds = match unit {
        "us" => value,
        "ms" => value * 1000,
        "s" => value * 1_000_000,
        "m" => value * 60_000_000,
        "h" => value * 3_600_000_000,
        "d" => value * 86_400_000_000,
        _ => {
            return Err(unprocessable_entity!(
                "Invalid duration unit: {}",
                unit_part
            ))
        }
    };
    Ok(seconds)
}

/// Parses a duration string containing multiple parts (e.g., "100ms 500us") into a `Value::Duration`.
/// # Arguments
///
/// * `duration_string` - The string representing the duration, which can contain multiple parts separated by whitespace.
///
/// # Returns
///
/// A `Result` containing the total duration as `Value::Duration` or an error if the string is invalid.
pub(crate) fn parse_duration(duration_string: &str) -> Result<Value, ReductError> {
    if duration_string.trim().is_empty() {
        return Err(unprocessable_entity!("Duration literal cannot be empty"));
    }

    let mut total_seconds = 0;
    for part in duration_string.split_whitespace() {
        let seconds = parse_single_duration(part)?;
        total_seconds += seconds;
    }
    Ok(Value::Duration(total_seconds))
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
