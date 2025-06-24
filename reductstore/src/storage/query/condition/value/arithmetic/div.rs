// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// A trait for dividing two values.
pub(crate) trait Div {
    /// Divides two values as floating point numbers.
    ///
    /// # Arguments
    ///
    /// * `self` - The first value.
    /// * `other` - The second value.
    ///
    /// # Returns
    ///
    /// The quotient of the two values or an error if the values cannot be divided.
    fn divide(self, other: Value) -> Result<Value, ReductError>
    where
        Self: Sized;
}

impl Div for Value {
    fn divide(self, divisor: Self) -> Result<Self, ReductError> {
        let dividend = self;

        if dividend.is_string() {
            return Err(unprocessable_entity!("Cannot divide string"));
        }

        if divisor.is_string() {
            return Err(unprocessable_entity!("Cannot divide by string"));
        }

        let divisor = divisor.as_float()?;
        if divisor == 0.0 {
            return Err(unprocessable_entity!("Cannot divide by zero"));
        }

        let result = Value::Float(dividend.as_float()? / divisor);
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    #[rstest]
    #[case(Value::Bool(true), Value::Bool(true), Value::Float(1.0))]
    #[case(Value::Bool(true), Value::Int(2), Value::Float(0.5))]
    #[case(Value::Bool(true), Value::Float(3.0), Value::Float(1.0 / 3.0))]
    #[case(Value::Bool(true), Value::Duration(4.0), Value::Float(0.25))]
    #[case(Value::Int(4), Value::Bool(true), Value::Float(4.0))]
    #[case(Value::Int(5), Value::Int(2), Value::Float(2.5))]
    #[case(Value::Int(6), Value::Float(3.0), Value::Float(2.0))]
    #[case(Value::Int(7), Value::Duration(2.0), Value::Float(3.5))]
    #[case(Value::Float(7.0), Value::Bool(true), Value::Float(7.0))]
    #[case(Value::Float(8.0), Value::Int(2), Value::Float(4.0))]
    #[case(Value::Float(9.0), Value::Float(3.0), Value::Float(3.0))]
    #[case(Value::Float(10.0), Value::Duration(2.0), Value::Float(5.0))]
    fn divide_ok(#[case] value: Value, #[case] other: Value, #[case] expected: Value) {
        let result = value.divide(other).unwrap();
        assert_eq!(result, expected);
    }

    #[rstest]
    #[case(Value::Bool(true), Value::String("xxx".to_string()))]
    #[case(Value::Int(1), Value::String("xxx".to_string()))]
    #[case(Value::Float(2.0), Value::String("xxx".to_string()))]
    #[case(Value::Duration(3.0), Value::String("xxx".to_string()))]
    fn divide_by_string(#[case] value: Value, #[case] other: Value) {
        let result = value.divide(other);
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
        let result = value.divide(other);
        assert_eq!(result, Err(unprocessable_entity!("Cannot divide string")));
    }

    #[rstest]
    #[case(Value::Int(1), Value::Int(0))]
    #[case(Value::Float(1.0), Value::Float(0.0))]
    fn divide_by_zero(#[case] value: Value, #[case] other: Value) {
        let result = value.divide(other);
        assert_eq!(result, Err(unprocessable_entity!("Cannot divide by zero")));
    }
}
