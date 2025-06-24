// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// A trait for subtracting two values.
pub(crate) trait Sub {
    /// Subtracts two values.
    ///
    /// # Arguments
    ///
    /// * `self` - The first value.
    /// * `other` - The second value.
    ///
    /// # Returns
    ///
    /// The difference of the two values or an error if the values cannot be subtracted.
    fn subtract(self, other: Self) -> Result<Self, ReductError>
    where
        Self: Sized;
}

impl Sub for Value {
    fn subtract(self, other: Self) -> Result<Self, ReductError> {
        let mut result = self;
        match result {
            Value::Bool(s) => match other {
                Value::Bool(v) => result = Value::Int(s as i64 - v as i64),
                Value::Int(v) => result = Value::Int(s as i64 - v),
                Value::Float(v) | Value::Duration(v) => result = Value::Float(s as i8 as f64 - v),
                Value::String(_) => {
                    return Err(unprocessable_entity!("Cannot subtract string from boolean"));
                }
            },

            Value::Int(s) => match other {
                Value::Bool(v) => result = Value::Int(s - v as i64),
                Value::Int(v) => result = Value::Int(s - v),
                Value::Float(v) | Value::Duration(v) => result = Value::Float(s as f64 - v),
                Value::String(_) => {
                    return Err(unprocessable_entity!("Cannot subtract string from integer"));
                }
            },

            Value::Float(s) | Value::Duration(s) => match other {
                Value::Bool(v) => result = Value::Float(s - v as i8 as f64),
                Value::Int(v) => result = Value::Float(s - v as f64),
                Value::Float(v) | Value::Duration(v) => result = Value::Float(s - v),
                Value::String(_) => {
                    if let Value::Duration(_) = result {
                        return Err(unprocessable_entity!(
                            "Cannot subtract string from duration"
                        ));
                    }
                    return Err(unprocessable_entity!("Cannot subtract string from float"));
                }
            },

            Value::String(_) => {
                return match other {
                    Value::Bool(_) => {
                        Err(unprocessable_entity!("Cannot subtract string from boolean"))
                    }
                    Value::Int(_) => {
                        Err(unprocessable_entity!("Cannot subtract string from integer"))
                    }

                    Value::Float(_) => {
                        Err(unprocessable_entity!("Cannot subtract string from float"))
                    }
                    Value::String(_) => Err(unprocessable_entity!("Cannot subtract string")),
                    Value::Duration(_) => Err(unprocessable_entity!(
                        "Cannot subtract string from duration"
                    )),
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
    #[case(Value::Bool(true), Value::Bool(false), Value::Int(1))]
    #[case(Value::Bool(true), Value::Int(1), Value::Int(0))]
    #[case(Value::Bool(true), Value::Float(1.0), Value::Float(0.0))]
    #[case(Value::Bool(true), Value::Duration(1.0), Value::Float(0.0))]
    #[case(Value::Int(1), Value::Bool(true), Value::Int(0))]
    #[case(Value::Int(1), Value::Int(1), Value::Int(0))]
    #[case(Value::Int(1), Value::Float(1.0), Value::Float(0.0))]
    #[case(Value::Int(1), Value::Duration(1.0), Value::Float(0.0))]
    #[case(Value::Float(1.0), Value::Bool(true), Value::Float(0.0))]
    #[case(Value::Float(1.0), Value::Int(1), Value::Float(0.0))]
    #[case(Value::Float(1.0), Value::Float(1.0), Value::Float(0.0))]
    #[case(Value::Float(1.0), Value::Duration(1.0), Value::Float(0.0))]
    fn sub(#[case] value: Value, #[case] other: Value, #[case] expected: Value) {
        let result = value.subtract(other).unwrap();
        assert_eq!(result, expected);
    }

    #[rstest]
    #[case(Value::Bool(true), Value::String("string".to_string()), unprocessable_entity!("Cannot subtract string from boolean"))]
    #[case(Value::Int(1), Value::String("string".to_string()), unprocessable_entity!("Cannot subtract string from integer"))]
    #[case(Value::Float(1.0), Value::String("string".to_string()), unprocessable_entity!("Cannot subtract string from float"))]
    #[case(Value::Duration(1.0), Value::String("string".to_string()), unprocessable_entity!("Cannot subtract string from duration"))]
    #[case(Value::String("string".to_string()), Value::Bool(true), unprocessable_entity!("Cannot subtract string from boolean"))]
    #[case(Value::String("string".to_string()), Value::Int(1), unprocessable_entity!("Cannot subtract string from integer"))]
    #[case(Value::String("string".to_string()), Value::Float(1.0), unprocessable_entity!("Cannot subtract string from float"))]
    #[case(Value::String("string".to_string()), Value::String("string".to_string()), unprocessable_entity!("Cannot subtract string"))]
    #[case(Value::String("string".to_string()), Value::Duration(1.0), unprocessable_entity!("Cannot subtract string from duration"))]

    fn sub_err(#[case] value: Value, #[case] other: Value, #[case] expected: ReductError) {
        let result = value.subtract(other);
        assert_eq!(result, Err(expected));
    }
}
