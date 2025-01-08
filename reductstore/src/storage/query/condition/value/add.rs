// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// A trait for adding two values.
pub(crate) trait Add {
    /// Adds two values.
    ///
    /// # Arguments
    ///
    /// * `self` - The first value.
    /// * `other` - The second value.
    ///
    /// # Returns
    ///
    /// The sum of the two values or an error if the values cannot be added.
    fn add(self, other: Self) -> Result<Self, ReductError>
    where
        Self: Sized;
}

impl Add for Value {
    fn add(self, other: Self) -> Result<Self, ReductError> {
        let mut result = self;
        match result {
            Value::Bool(s) => match other {
                Value::Bool(v) => result = Value::Int(s as i64 + v as i64),
                Value::Int(v) => result = Value::Int(s as i64 + v),
                Value::Float(v) => result = Value::Float(s as i8 as f64 + v),
                Value::String(_) => {
                    return Err(unprocessable_entity!("Cannot add boolean to string"));
                }
            },

            Value::Int(s) => match other {
                Value::Bool(v) => result = Value::Int(s + v as i64),
                Value::Int(v) => result = Value::Int(s + v),
                Value::Float(v) => result = Value::Float(s as f64 + v),
                Value::String(_) => {
                    return Err(unprocessable_entity!("Cannot add integer to string"));
                }
            },

            Value::Float(s) => match other {
                Value::Bool(v) => result = Value::Float(s + v as i8 as f64),
                Value::Int(v) => result = Value::Float(s + v as f64),
                Value::Float(v) => result = Value::Float(s + v),
                Value::String(_) => {
                    return Err(unprocessable_entity!("Cannot add float to string"));
                }
            },

            Value::String(s) => match other {
                Value::Bool(_) => {
                    return Err(unprocessable_entity!("Cannot add string to boolean"));
                }
                Value::Int(_) => {
                    return Err(unprocessable_entity!("Cannot add string to integer"));
                }

                Value::Float(_) => {
                    return Err(unprocessable_entity!("Cannot add string to float"));
                }
                Value::String(v) => result = Value::String(format!("{}{}", s, v)),
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
    #[case(Value::Bool(true), Value::Int(2), Value::Int(3))]
    #[case(Value::Bool(true), Value::Float(2.0), Value::Float(3.0))]
    #[case(Value::Int(2), Value::Int(3), Value::Int(5))]
    #[case(Value::Int(2), Value::Float(3.0), Value::Float(5.0))]
    #[case(Value::Float(2.0), Value::Float(3.0), Value::Float(5.0))]
    #[case(Value::String("hello".to_string()), Value::String("world".to_string()), Value::String("helloworld".to_string()))]
    fn test_add_ok(#[case] a: Value, #[case] b: Value, #[case] expected: Value) {
        assert_eq!(a.add(b).unwrap(), expected);
    }

    #[rstest]
    #[case(Value::Bool(true), Value::String("world".to_string()), unprocessable_entity!("Cannot add boolean to string"))]
    #[case(Value::Int(2), Value::String("world".to_string()), unprocessable_entity!("Cannot add integer to string"))]
    #[case(Value::Float(2.0), Value::String("world".to_string()), unprocessable_entity!("Cannot add float to string"))]
    #[case(Value::String("hello".to_string()), Value::Bool(true), unprocessable_entity!("Cannot add string to boolean"))]
    #[case(Value::String("hello".to_string()), Value::Int(2), unprocessable_entity!("Cannot add string to integer"))]
    #[case(Value::String("hello".to_string()), Value::Float(2.0), unprocessable_entity!("Cannot add string to float"))]
    fn test_add_err(#[case] a: Value, #[case] b: Value, #[case] expected: ReductError) {
        assert_eq!(a.add(b), Err(expected));
    }
}
