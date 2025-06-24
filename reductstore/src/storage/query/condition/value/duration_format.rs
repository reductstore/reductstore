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
/// A `Result` containing the parsed duration as `Value::Duration` or an error if the string is invalid.
fn parse_single_duration(duration_string: &str) -> Result<Value, ReductError> {
    let duration_string = duration_string.trim();
    let (num_part, unit_part) = duration_string
        .chars()
        .partition::<String, _>(|c| c.is_digit(10) || *c == '.' || *c == '-');
    let value: f64 = num_part
        .parse()
        .map_err(|_| unprocessable_entity!("Invalid duration value: {}", duration_string))?;

    let unit = unit_part.as_str();
    let seconds = match unit {
        "us" => value / 1_000_000.0,
        "ms" => value / 1_000.0,
        "s" => value,
        "m" => value * 60.0,
        "h" => value * 3600.0,
        "d" => value * 86400.0,
        _ => {
            return Err(unprocessable_entity!(
                "Invalid duration unit: {}",
                unit_part
            ))
        }
    };
    Ok(Value::Duration(seconds))
}

pub(crate) fn parse_duration(duration_string: &str) -> Result<Value, ReductError> {
    let mut total_seconds = 0.0;
    for part in duration_string.split_whitespace() {
        let Value::Duration(seconds) = parse_single_duration(part)? else {
            return Err(unprocessable_entity!("Invalid duration part: {}", part));
        };
        total_seconds += seconds;
    }
    Ok(Value::Duration(total_seconds))
}

pub(super) fn fmt_duration(mut seconds: f64, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let mut parts = Vec::new();
    let units = [
        ("d", 86400.0),
        ("h", 3600.0),
        ("m", 60.0),
        ("s", 1.0),
        ("ms", 0.001),
        ("us", 0.000001),
    ];
    for &(unit, unit_seconds) in &units {
        if seconds >= unit_seconds {
            let value = (seconds / unit_seconds).floor();
            if value > 0.0 {
                parts.push(format!("{}{}", value as u64, unit));
                seconds -= value * unit_seconds;
            }
        }
    }
    if parts.is_empty() {
        parts.push("0s".to_string());
    }
    write!(f, "{}", parts.join(" "))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    fn test_parse_units() {
        assert_eq!(
            parse_single_duration("100us").unwrap(),
            Value::Duration(0.0001)
        );
        assert_eq!(
            parse_single_duration("100ms").unwrap(),
            Value::Duration(0.1)
        );
        assert_eq!(
            parse_single_duration("100s").unwrap(),
            Value::Duration(100.0)
        );
        assert_eq!(
            parse_single_duration("2.5m").unwrap(),
            Value::Duration(150.0)
        );
        assert_eq!(
            parse_single_duration("-1h").unwrap(),
            Value::Duration(-3600.0)
        );
        assert_eq!(
            parse_single_duration("1d").unwrap(),
            Value::Duration(86400.0)
        );
    }

    #[rstest]
    fn test_parse_invalid_duration() {
        assert_eq!(
            parse_single_duration("100xyz").err().unwrap(),
            unprocessable_entity!("Invalid duration unit: xyz")
        );
        assert_eq!(
            parse_single_duration("abc").err().unwrap(),
            unprocessable_entity!("Invalid duration value: abc")
        );
    }

    #[rstest]
    fn test_parse_duration() {
        assert_eq!(
            parse_duration("100ms 500us").unwrap(),
            Value::Duration(0.1005)
        );
        assert_eq!(parse_duration("1h -30m").unwrap(), Value::Duration(1800.0));
        assert_eq!(parse_duration("2d 3h").unwrap(), Value::Duration(183600.0));
    }
}
