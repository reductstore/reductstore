// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// A trait for multiplying two values.
pub(crate) trait Mult {
    /// Multiplies two values.
    ///
    /// # Arguments
    ///
    /// * `self` - The first value.
    /// * `other` - The second value.
    ///
    /// # Returns
    ///
    /// The product of the two values or an error if the values cannot be multiplied.
    fn multiply(self, other: Self) -> Result<Self, ReductError>
    where
        Self: Sized;
}

impl Mult for Value {
    fn multiply(self, other: Self) -> Result<Self, ReductError> {
        let mut result = self;
        match result {
            Value::Bool(s) => match other {
                Value::Bool(v) => result = Value::Int(s as i64 * v as i64),
                Value::Int(v) => result = Value::Int(s as i64 * v),
                Value::Float(v) => result = Value::Float(s as i8 as f64 * v),
                Value::String(_) => {
                    return Err(unprocessable_entity!("Cannot multiply boolean by string"));
                }
            },

            Value::Int(s) => match other {
                Value::Bool(v) => result = Value::Int(s * v as i64),
                Value::Int(v) => result = Value::Int(s * v),
                Value::Float(v) => result = Value::Float(s as f64 * v),
                Value::String(_) => {
                    return Err(unprocessable_entity!("Cannot multiply integer by string"));
                }
            },

            Value::Float(s) => match other {
                Value::Bool(v) => result = Value::Float(s * v as i8 as f64),
                Value::Int(v) => result = Value::Float(s * v as f64),
                Value::Float(v) => result = Value::Float(s * v),
                Value::String(_) => {
                    return Err(unprocessable_entity!("Cannot multiply float by string"));
                }
            },

            Value::String(_) => {
                return match other {
                    Value::Bool(_) => {
                        Err(unprocessable_entity!("Cannot multiply string by boolean"))
                    }
                    Value::Int(_) => {
                        Err(unprocessable_entity!("Cannot multiply string by integer"))
                    }

                    Value::Float(_) => {
                        Err(unprocessable_entity!("Cannot multiply string by float"))
                    }
                    Value::String(_) => {
                        Err(unprocessable_entity!("Cannot multiply string by string"))
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
    #[case(Value::Bool(true), Value::Bool(false), Value::Int(0))]
    #[case(Value::Bool(true), Value::Int(2), Value::Int(2))]
    #[case(Value::Bool(true), Value::Float(2.0), Value::Float(2.0))]
    #[case(Value::Int(2), Value::Bool(true), Value::Int(2))]
    #[case(Value::Int(2), Value::Int(2), Value::Int(4))]
    #[case(Value::Int(2), Value::Float(2.0), Value::Float(4.0))]
    #[case(Value::Float(2.0), Value::Bool(true), Value::Float(2.0))]
    #[case(Value::Float(2.0), Value::Int(2), Value::Float(4.0))]
    #[case(Value::Float(2.0), Value::Float(2.0), Value::Float(4.0))]
    fn multiply(#[case] value: Value, #[case] other: Value, #[case] expected: Value) {
        let result = value.multiply(other).unwrap();
        assert_eq!(result, expected);
    }

    #[rstest]
    #[case(Value::Bool(true), Value::String("string".to_string()), unprocessable_entity!("Cannot multiply boolean by string"))]
    #[case(Value::Int(2), Value::String("string".to_string()), unprocessable_entity!("Cannot multiply integer by string"))]
    #[case(Value::Float(2.0), Value::String("string".to_string()), unprocessable_entity!("Cannot multiply float by string"))]
    #[case(Value::String("string".to_string()), Value::Bool(true), unprocessable_entity!("Cannot multiply string by boolean"))]
    #[case(Value::String("string".to_string()), Value::Int(2), unprocessable_entity!("Cannot multiply string by integer"))]
    #[case(Value::String("string".to_string()), Value::Float(2.0), unprocessable_entity!("Cannot multiply string by float"))]
    #[case(Value::String("string".to_string()), Value::String("string".to_string()), unprocessable_entity!("Cannot multiply string by string"))]
    fn multiply_error(#[case] value: Value, #[case] other: Value, #[case] expected: ReductError) {
        let result = value.multiply(other);
        assert_eq!(result, Err(expected));
    }
}
