// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// A trait for numeric division.
pub(crate) trait DivNum {
    /// Divides two values as integers
    ///
    /// # Arguments
    ///
    /// * `self` - The first value.
    /// * `other` - The second value.
    ///
    /// # Returns
    ///
    /// The quotient of the two values or an error if the values cannot be divided.
    fn divide_num(self, other: Value) -> Result<Value, ReductError>
    where
        Self: Sized;
}

impl DivNum for Value {
    fn divide_num(self, divisor: Self) -> Result<Self, ReductError> {
        let dividend = self;

        if dividend.is_string() {
            return Err(unprocessable_entity!("Cannot divide string"));
        }

        if divisor.is_string() {
            return Err(unprocessable_entity!("Cannot divide by string"));
        }

        let divisor = divisor.as_int()?;
        if divisor == 0 {
            return Err(unprocessable_entity!("Cannot divide by zero"));
        }

        let result = Value::Int(dividend.as_int()? / divisor);
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    #[rstest]
    #[case(Value::Bool(true), Value::Bool(true), Value::Int(1))]
    #[case(Value::Bool(true), Value::Int(2), Value::Int(0))]
    #[case(Value::Bool(true), Value::Float(3.0), Value::Int(0))]
    #[case(Value::Bool(true), Value::Duration(4.0), Value::Int(0))]
    #[case(Value::Int(4), Value::Bool(true), Value::Int(4))]
    #[case(Value::Int(5), Value::Int(2), Value::Int(2))]
    #[case(Value::Int(6), Value::Float(3.0), Value::Int(2))]
    #[case(Value::Int(7), Value::Duration(2.0), Value::Int(3))]
    #[case(Value::Float(7.0), Value::Bool(true), Value::Int(7))]
    #[case(Value::Float(8.0), Value::Int(2), Value::Int(4))]
    #[case(Value::Float(9.0), Value::Float(3.0), Value::Int(3))]
    #[case(Value::Float(10.0), Value::Duration(2.0), Value::Int(5))]
    fn divide_ok(#[case] value: Value, #[case] other: Value, #[case] expected: Value) {
        let result = value.divide_num(other).unwrap();
        assert_eq!(result, expected);
    }

    #[rstest]
    #[case(Value::Bool(true), Value::String("xxx".to_string()))]
    #[case(Value::Int(1), Value::String("xxx".to_string()))]
    #[case(Value::Float(2.0), Value::String("xxx".to_string()))]
    #[case(Value::Duration(3.0), Value::String("xxx".to_string()))]
    fn divide_by_string(#[case] value: Value, #[case] other: Value) {
        let result = value.divide_num(other);
        assert_eq!(
            result,
            Err(unprocessable_entity!("Cannot divide by string"))
        );
    }

    #[rstest]
    #[case(Value::String("xxx".to_string()), Value::Bool(true))]
    #[case(Value::String("xxx".to_string()), Value::Int(1))]
    #[case(Value::String("xxx".to_string()), Value::Float(2.0))]
    #[case(Value::String("xxx".to_string()), Value::String("xxx".to_string()))]
    #[case(Value::String("xxx".to_string()), Value::Duration(3.0))]
    fn divide_string(#[case] value: Value, #[case] other: Value) {
        let result = value.divide_num(other);
        assert_eq!(result, Err(unprocessable_entity!("Cannot divide string")));
    }

    #[rstest]
    #[case(Value::Int(1), Value::Int(0))]
    #[case(Value::Float(1.0), Value::Float(0.0))]
    #[case(Value::Duration(1.0), Value::Duration(0.0))]
    fn divide_by_zero(#[case] value: Value, #[case] other: Value) {
        let result = value.divide_num(other);
        assert_eq!(result, Err(unprocessable_entity!("Cannot divide by zero")));
    }
}
