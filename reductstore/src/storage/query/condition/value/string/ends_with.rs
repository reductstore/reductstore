// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use reduct_base::error::ReductError;

pub(crate) trait EndsWith {
    /// Checks if the first value ends with the second value.
    ///
    /// It converts the both values to string before checking.
    ///
    /// # Arguments
    ///
    /// * `self` - The value to check.
    /// * `other` - The value to check for.
    fn ends_with(self, other: Self) -> Result<bool, ReductError>
    where
        Self: Sized;
}

impl EndsWith for Value {
    fn ends_with(self, other: Self) -> Result<bool, ReductError> {
        let other = other.as_string()?;
        let self_string = self.as_string()?;
        Ok(self_string.ends_with(&other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(Value::String("test".to_string()), Value::String("st".to_string()), true)]
    #[case(Value::String("test".to_string()), Value::String("es".to_string()), false)]
    fn test_logic(#[case] value: Value, #[case] other: Value, #[case] expected: bool) {
        let result = value.ends_with(other).unwrap();
        assert_eq!(result, expected);
    }

    #[rstest]
    #[case(Value::Bool(true), Value::String("ue".to_string()), true)]
    #[case(Value::Bool(false), Value::String("a".to_string()), false)]
    #[case(Value::Int(10), Value::String("1".to_string()), false)]
    #[case(Value::Int(0), Value::String("0".to_string()), true)]
    #[case(Value::Float(1.0), Value::String("1.".to_string()), false)]
    #[case(Value::Float(0.5), Value::String("5".to_string()), true)]
    #[case(Value::String("test".to_string()), Value::Bool(true), false)]
    #[case(Value::String("xxxfalse".to_string()), Value::Bool(false), true)]
    #[case(Value::String("test".to_string()), Value::Int(1), false)]
    #[case(Value::String("1000".to_string()), Value::Int(0), true)]
    #[case(Value::String("test".to_string()), Value::Float(1.0), false)]
    #[case(Value::String("111.5".to_string()), Value::Float(1.5), true)]
    fn test_convertion(#[case] value: Value, #[case] other: Value, #[case] expected: bool) {
        let result = value.ends_with(other).unwrap();
        assert_eq!(result, expected);
    }
}
