// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// Parse a duration literal into microseconds.
///
/// Supported units: `us`, `ms`, `s`, `m`, `h`, `d`.
/// A literal may contain multiple whitespace-separated parts (e.g. `1h -30m`).
pub(crate) fn parse_duration_to_micros(duration_string: &str) -> Result<i64, ReductError> {
    if duration_string.trim().is_empty() {
        return Err(unprocessable_entity!("Duration literal cannot be empty"));
    }

    let mut total_usecs = 0;
    for part in duration_string.split_whitespace() {
        total_usecs += parse_single_duration_to_micros(part)?;
    }

    Ok(total_usecs)
}

fn parse_single_duration_to_micros(duration_string: &str) -> Result<i64, ReductError> {
    if duration_string.trim().is_empty() {
        return Err(unprocessable_entity!("Duration literal cannot be empty"));
    }

    let duration_string = duration_string.trim();
    let (num_part, unit_part) = duration_string
        .chars()
        .partition::<String, _>(|c| c.is_ascii_digit() || *c == '.' || *c == '-');
    let value: i64 = num_part
        .parse()
        .map_err(|_| unprocessable_entity!("Invalid duration value: {}", duration_string))?;

    match unit_part.as_str() {
        "us" => Ok(value),
        "ms" => Ok(value * 1_000),
        "s" => Ok(value * 1_000_000),
        "m" => Ok(value * 60_000_000),
        "h" => Ok(value * 3_600_000_000),
        "d" => Ok(value * 86_400_000_000),
        _ => Err(unprocessable_entity!(
            "Invalid duration unit: {}",
            unit_part
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("100ms 500us", 100_500)]
    #[case("1h -30m", 1_800_000_000)]
    #[case("2d 3h", 183_600_000_000)]
    #[case("15us", 15)]
    #[case("-2s", -2_000_000)]
    fn parses_valid_duration(#[case] literal: &str, #[case] expected_us: i64) {
        assert_eq!(parse_duration_to_micros(literal).unwrap(), expected_us);
    }

    #[rstest]
    #[case("")]
    #[case("100xyz")]
    #[case("abc")]
    #[case("2.5m")]
    #[case("1h,2m")]
    fn rejects_invalid_duration(#[case] literal: &str) {
        assert!(parse_duration_to_micros(literal).is_err());
    }

    #[rstest]
    fn rejects_empty_single_duration_literal() {
        assert!(parse_single_duration_to_micros("").is_err());
    }
}
