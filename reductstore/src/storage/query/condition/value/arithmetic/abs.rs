// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// A trait for calculating an absolute value.
pub(crate) trait Abs {
    /// Calculates the absolute value of a value.
    fn abs(self) -> Result<Self, ReductError>
    where
        Self: Sized;
}

impl Abs for Value {
    fn abs(self) -> Result<Self, ReductError> {
        let mut result = self;
        match result {
            Value::Bool(s) => result = Value::Int(s as i64),
            Value::Int(s) => result = Value::Int(s.abs()),
            Value::Float(s) | Value::Duration(s) => result = Value::Float(s.abs()),
            Value::String(_) => {
                return Err(unprocessable_entity!(
                    "Cannot calculate absolute value of a string"
                ));
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    #[rstest]
    #[case(Value::Bool(true), Value::Int(1))]
    #[case(Value::Bool(false), Value::Int(0))]
    #[case(Value::Int(-1), Value::Int(1))]
    #[case(Value::Int(0), Value::Int(0))]
    #[case(Value::Int(1), Value::Int(1))]
    #[case(Value::Float(-1.0), Value::Float(1.0))]
    #[case(Value::Float(0.0), Value::Float(0.0))]
    #[case(Value::Float(1.0), Value::Float(1.0))]
    #[case(Value::Duration(1.0), Value::Duration(1.0))]
    fn test_abs(#[case] value: Value, #[case] expected: Value) {
        let result = value.abs().unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_abs_string() {
        let value = Value::String("test".to_string());
        assert_eq!(
            value.abs(),
            Err(unprocessable_entity!(
                "Cannot calculate absolute value of a string"
            ))
        );
    }
}
