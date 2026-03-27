// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::storage::query::condition::{BoxedNode, Context};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;
use std::str::FromStr;

mod day;
mod hour;
mod minute;
mod month;
mod second;
mod weekday;
mod year;

pub(crate) use day::Day;
pub(crate) use hour::Hour;
pub(crate) use minute::Minute;
pub(crate) use month::Month;
pub(crate) use second::Second;
pub(crate) use weekday::Weekday;
pub(crate) use year::Year;

pub(super) fn parse_timestamp_and_timezone(
    operator: &str,
    operands: &mut [BoxedNode],
    context: &Context,
) -> Result<(DateTime<Utc>, Tz), ReductError> {
    if operands.len() != 1 && operands.len() != 2 {
        return Err(unprocessable_entity!(
            "{} requires one or two operands",
            operator
        ));
    }

    let timestamp = operands[0].apply(context)?.as_int()?;
    let date_time = DateTime::from_timestamp_micros(timestamp).ok_or_else(|| {
        unprocessable_entity!(
            "{} requires a valid timestamp in microseconds, got {}",
            operator,
            timestamp
        )
    })?;

    let timezone = if operands.len() == 2 {
        let timezone = operands[1].apply(context)?;
        if !timezone.is_string() {
            return Err(unprocessable_entity!(
                "{} requires timezone to be a string",
                operator
            ));
        }

        let timezone = timezone.to_string();
        Tz::from_str(&timezone)
            .map_err(|_| unprocessable_entity!("Invalid timezone: '{}'", timezone))?
    } else {
        Tz::UTC
    };

    Ok((date_time, timezone))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::query::condition::constant::Constant;
    use crate::storage::query::condition::value::Value;
    use reduct_base::unprocessable_entity;
    use rstest::rstest;

    #[rstest]
    fn test_parse_timestamp_with_default_timezone() {
        let mut operands = vec![Constant::boxed(Value::Int(1704067200123456))];
        let (_, timezone) =
            parse_timestamp_and_timezone("$hour", &mut operands, &Context::default()).unwrap();
        assert_eq!(timezone, Tz::UTC);
    }

    #[rstest]
    fn test_parse_timestamp_with_timezone() {
        let mut operands = vec![
            Constant::boxed(Value::Int(1704067200123456)),
            Constant::boxed(Value::String("Europe/Berlin".to_string())),
        ];

        let (_, timezone) =
            parse_timestamp_and_timezone("$hour", &mut operands, &Context::default()).unwrap();
        assert_eq!(timezone, Tz::Europe__Berlin);
    }

    #[rstest]
    fn test_parse_timestamp_invalid_timezone() {
        let mut operands = vec![
            Constant::boxed(Value::Int(1704067200123456)),
            Constant::boxed(Value::String("Mars/Olympus".to_string())),
        ];

        let result = parse_timestamp_and_timezone("$hour", &mut operands, &Context::default());
        assert_eq!(
            result.err().unwrap(),
            unprocessable_entity!("Invalid timezone: 'Mars/Olympus'")
        );
    }

    #[rstest]
    fn test_parse_timestamp_invalid_number_of_operands() {
        let mut operands = vec![];
        let result = parse_timestamp_and_timezone("$hour", &mut operands, &Context::default());
        assert_eq!(
            result.err().unwrap(),
            unprocessable_entity!("$hour requires one or two operands")
        );
    }

    #[rstest]
    fn test_parse_timestamp_non_string_timezone() {
        let mut operands = vec![
            Constant::boxed(Value::Int(1704067200123456)),
            Constant::boxed(Value::Int(123)),
        ];
        let result = parse_timestamp_and_timezone("$hour", &mut operands, &Context::default());
        assert_eq!(
            result.err().unwrap(),
            unprocessable_entity!("$hour requires timezone to be a string")
        );
    }

    #[rstest]
    fn test_parse_timestamp_invalid_timestamp() {
        let mut operands = vec![Constant::boxed(Value::Int(i64::MAX))];
        let result = parse_timestamp_and_timezone("$hour", &mut operands, &Context::default());
        assert_eq!(
            result.err().unwrap(),
            unprocessable_entity!(
                "$hour requires a valid timestamp in microseconds, got {}",
                i64::MAX
            )
        );
    }
}
