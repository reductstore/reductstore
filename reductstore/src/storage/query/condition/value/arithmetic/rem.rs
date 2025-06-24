// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// A trait for calculating a remainder of division.
pub(crate) trait Rem {
    /// Calculates the remainder of division of two values.
    ///
    /// # Arguments
    ///
    /// * `self` - The devidend.
    /// * `other` - The divisor.
    ///
    /// # Returns
    ///
    /// The remainder of the division of the two values or an error if the values cannot be divided.
    fn remainder(self, other: Self) -> Result<Self, ReductError>
    where
        Self: Sized;
}

impl Rem for Value {
    fn remainder(self, other: Self) -> Result<Self, ReductError> {
        let mut result = self;
        match result {
            Value::Bool(s) => match other {
                Value::Bool(v) => result = Value::Int(s as i64 % v as i64),
                Value::Int(v) | Value::Duration(v) => result = Value::Int(s as i64 % v),
                Value::Float(v) => result = Value::Float(s as i8 as f64 % v),
                Value::String(_) => {
                    return Err(unprocessable_entity!("Cannot divide boolean by string"));
                }
            },

            Value::Int(s) | Value::Duration(s) => match other {
                Value::Bool(v) => result = Value::Int(s % v as i64),
                Value::Int(v) | Value::Duration(v) => result = Value::Int(s % v),
                Value::Float(v) => result = Value::Float(s as f64 % v),
                Value::String(_) => {
                    if let Value::Duration(_) = result {
                        return Err(unprocessable_entity!("Cannot divide duration by string"));
                    }
                    return Err(unprocessable_entity!("Cannot divide integer by string"));
                }
            },

            Value::Float(s) => match other {
                Value::Bool(v) => result = Value::Float(s % v as i8 as f64),
                Value::Int(v) | Value::Duration(v) => result = Value::Float(s % v as f64),
                Value::Float(v) => result = Value::Float(s % v),
                Value::String(_) => {
                    return Err(unprocessable_entity!("Cannot divide float by string"));
                }
            },

            Value::String(_) => {
                return match other {
                    Value::Bool(_) => Err(unprocessable_entity!("Cannot divide string by boolean")),
                    Value::Int(_) => Err(unprocessable_entity!("Cannot divide string by integer")),
                    Value::Float(_) => Err(unprocessable_entity!("Cannot divide string by float")),
                    Value::String(_) => Err(unprocessable_entity!("Cannot divide string")),
                    Value::Duration(_) => {
                        Err(unprocessable_entity!("Cannot divide string by duration"))
                    }
                }
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
    #[case(Value::Bool(true), Value::Bool(true), Value::Int(0))]
    #[case(Value::Bool(true), Value::Int(2), Value::Int(1))]
    #[case(Value::Bool(true), Value::Float(3.0), Value::Float(1.0))]
    #[case(Value::Bool(true), Value::Duration(4), Value::Float(1.0))]
    #[case(Value::Int(4), Value::Bool(true), Value::Int(0))]
    #[case(Value::Int(5), Value::Int(2), Value::Int(1))]
    #[case(Value::Int(6), Value::Float(3.5), Value::Float(2.5))]
    #[case(Value::Int(7), Value::Duration(2), Value::Float(1.0))]
    #[case(Value::Float(7.0), Value::Bool(true), Value::Float(0.0))]
    #[case(Value::Float(8.0), Value::Int(3), Value::Float(2.0))]
    #[case(Value::Float(-9.0), Value::Float(3.5), Value::Float(-2.0))]
    fn rem_ok(#[case] value: Value, #[case] other: Value, #[case] expected: Value) {
        let result = value.remainder(other).unwrap();
        assert_eq!(result, expected);
    }

    #[rstest]
    #[case(Value::Bool(true), Value::String("string".to_string()), unprocessable_entity!("Cannot divide boolean by string"))]
    #[case(Value::Int(1), Value::String("string".to_string()), unprocessable_entity!("Cannot divide integer by string"))]
    #[case(Value::Float(2.0), Value::String("string".to_string()), unprocessable_entity!("Cannot divide float by string"))]
    #[case( Value::Duration(3), Value::String("string".to_string()), unprocessable_entity!("Cannot divide duration by string"))]
    #[case(Value::String("string".to_string()), Value::Bool(true), unprocessable_entity!("Cannot divide string by boolean"))]
    #[case(Value::String("string".to_string()), Value::Int(1), unprocessable_entity!("Cannot divide string by integer"))]
    #[case(Value::String("string".to_string()), Value::Float(2.0), unprocessable_entity!("Cannot divide string by float"))]
    #[case(Value::String("string".to_string()), Value::String("string".to_string()), unprocessable_entity!("Cannot divide string"))]
    #[case(Value::String("string".to_string()),  Value::Duration(3), unprocessable_entity!("Cannot divide string by duration"))]
    fn rem_err(#[case] value: Value, #[case] other: Value, #[case] expected: ReductError) {
        let result = value.remainder(other);
        assert_eq!(result, Err(expected));
    }
}
