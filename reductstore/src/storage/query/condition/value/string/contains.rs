// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use reduct_base::error::ReductError;

pub(crate) trait Contains {
    /// Checks if the value contains a string representation of another value.
    ///
    /// It converts the both values to string and checks if the first value contains the second value.
    ///
    /// # Arguments
    ///
    /// * `self` - The value to check.
    /// * `other` - The value to check for.
    fn contains(self, other: Self) -> Result<bool, ReductError>
    where
        Self: Sized;
}

impl Contains for Value {
    fn contains(self, other: Self) -> Result<bool, ReductError> {
        let other = other.as_string()?;
        let self_string = self.as_string()?;
        Ok(self_string.contains(&other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(Value::String("test".to_string()), Value::String("es".to_string()), true)]
    #[case(Value::String("test".to_string()), Value::String("te".to_string()), true)]
    #[case(Value::String("test".to_string()), Value::String("st".to_string()), true)]
    #[case(Value::String("test".to_string()), Value::String("t".to_string()), true)]
    #[case(Value::String("test".to_string()), Value::String("test".to_string()), true)]
    #[case(Value::String("test".to_string()), Value::String("test1".to_string()), false)]
    #[case(Value::String("test".to_string()), Value::String("test2".to_string()), false)]
    #[case(Value::String("test".to_string()), Value::String("test3".to_string()), false)]
    #[case(Value::String("test".to_string()), Value::String("test4".to_string()), false)]
    #[case(Value::String("test".to_string()), Value::String("test5".to_string()), false)]
    fn test_logic(#[case] value: Value, #[case] other: Value, #[case] expected: bool) {
        let result = value.contains(other).unwrap();
        assert_eq!(result, expected);
    }

    #[rstest]
    #[case(Value::Bool(true), Value::String("t".to_string()), true)]
    #[case(Value::Bool(false), Value::String("t".to_string()), false)]
    #[case(Value::Int(1), Value::String("0".to_string()), false)]
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
        let result = value.contains(other).unwrap();
        assert_eq!(result, expected);
    }
}
