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
    fn sub(self, other: Self) -> Result<Self, ReductError>
    where
        Self: Sized;
}

impl Sub for Value {
    fn sub(self, other: Self) -> Result<Self, ReductError> {
        let mut result = self;
        match result {
            Value::Bool(s) => match other {
                Value::Bool(v) => result = Value::Int(s as i64 - v as i64),
                Value::Int(v) => result = Value::Int(s as i64 - v),
                Value::Float(v) => result = Value::Float(s as i8 as f64 - v),
                Value::String(_) => {
                    return Err(unprocessable_entity!("Cannot subtract string from boolean"));
                }
            },

            Value::Int(s) => match other {
                Value::Bool(v) => result = Value::Int(s - v as i64),
                Value::Int(v) => result = Value::Int(s - v),
                Value::Float(v) => result = Value::Float(s as f64 - v),
                Value::String(_) => {
                    return Err(unprocessable_entity!("Cannot subtract string from integer"));
                }
            },

            Value::Float(s) => match other {
                Value::Bool(v) => result = Value::Float(s - v as i8 as f64),
                Value::Int(v) => result = Value::Float(s - v as f64),
                Value::Float(v) => result = Value::Float(s - v),
                Value::String(_) => {
                    return Err(unprocessable_entity!("Cannot subtract string from float"));
                }
            },

            Value::String(_) => match other {
                Value::Bool(_) => {
                    return Err(unprocessable_entity!("Cannot subtract string from boolean"));
                }
                Value::Int(_) => {
                    return Err(unprocessable_entity!("Cannot subtract string from integer"));
                }

                Value::Float(_) => {
                    return Err(unprocessable_entity!("Cannot subtract string from float"));
                }
                Value::String(_) => {
                    return Err(unprocessable_entity!("Cannot subtract string"));
                }
            },
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
    #[case(Value::Int(1), Value::Int(1), Value::Int(0))]
    #[case(Value::Int(1), Value::Float(1.0), Value::Float(0.0))]
    #[case(Value::Float(1.0), Value::Float(1.0), Value::Float(0.0))]
    fn sub(#[case] value: Value, #[case] other: Value, #[case] expected: Value) {
        let result = value.sub(other).unwrap();
        assert_eq!(result, expected);
    }

    #[rstest]
    #[case(Value::Bool(true), Value::String("string".to_string()), unprocessable_entity!("Cannot subtract string from boolean"))]
    #[case(Value::Int(1), Value::String("string".to_string()), unprocessable_entity!("Cannot subtract string from integer"))]
    #[case(Value::Float(1.0), Value::String("string".to_string()), unprocessable_entity!("Cannot subtract string from float"))]
    #[case(Value::String("string".to_string()), Value::Bool(true), unprocessable_entity!("Cannot subtract string from boolean"))]
    #[case(Value::String("string".to_string()), Value::Int(1), unprocessable_entity!("Cannot subtract string from integer"))]
    #[case(Value::String("string".to_string()), Value::Float(1.0), unprocessable_entity!("Cannot subtract string from float"))]
    #[case(Value::String("string".to_string()), Value::String("string".to_string()), unprocessable_entity!("Cannot subtract string"))]

    fn sub_err(#[case] value: Value, #[case] other: Value, #[case] expected: ReductError) {
        let result = value.sub(other);
        assert_eq!(result, Err(expected));
    }
}
