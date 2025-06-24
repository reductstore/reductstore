// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use reduct_base::error::ReductError;

pub(crate) trait StartsWith {
    /// Checks if the first value starts with the second value.
    ///
    /// It converts the both values to string before checking.
    ///
    /// # Arguments
    ///
    /// * `self` - The value to check.
    /// * `other` - The value to check for.
    fn starts_with(self, other: Self) -> Result<bool, ReductError>
    where
        Self: Sized;
}

impl StartsWith for Value {
    fn starts_with(self, other: Self) -> Result<bool, ReductError> {
        let other = other.to_string();
        let self_string = self.to_string();
        Ok(self_string.starts_with(&other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(Value::String("test".to_string()), Value::String("te".to_string()), true)]
    #[case(Value::String("test".to_string()), Value::String("es".to_string()), false)]
    fn test_logic(#[case] value: Value, #[case] other: Value, #[case] expected: bool) {
        let result = value.starts_with(other).unwrap();
        assert_eq!(result, expected);
    }

    #[rstest]
    #[case(Value::Bool(true), Value::String("t".to_string()), true)]
    #[case(Value::Bool(false), Value::String("a".to_string()), false)]
    #[case(Value::Int(10), Value::String("0".to_string()), false)]
    #[case(Value::Int(0), Value::String("0".to_string()), true)]
    #[case(Value::Float(1.0), Value::String("2.".to_string()), false)]
    #[case(Value::Float(0.5), Value::String("0.".to_string()), true)]
    #[case(Value::String("test".to_string()), Value::Bool(true), false)]
    #[case(Value::String("falseee".to_string()), Value::Bool(false), true)]
    #[case(Value::String("test".to_string()), Value::Int(1), false)]
    #[case(Value::String("1000".to_string()), Value::Int(1), true)]
    #[case(Value::String("test".to_string()), Value::Float(1.0), false)]
    #[case(Value::String("1.0000".to_string()), Value::Float(1.0), true)]
    fn test_convertion(#[case] value: Value, #[case] other: Value, #[case] expected: bool) {
        let result = value.starts_with(other).unwrap();
        assert_eq!(result, expected);
    }
}
