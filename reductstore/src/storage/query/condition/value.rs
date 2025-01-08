// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

mod add;
mod cmp;
mod div;
mod mult;
mod sub;

use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

pub(crate) use add::Add;
pub(crate) use div::Div;
pub(crate) use mult::Mult;
pub(crate) use sub::Sub;

/// A value that can be used in a condition.
#[derive(Debug, Clone)]
pub(crate) enum Value {
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
}

impl Value {
    /// Parses a string into a value.
    ///
    /// # Arguments
    ///
    /// * `value` - The string to parse.
    ///
    /// # Returns
    ///
    /// The parsed value or `None` if the value is invalid.
    pub(crate) fn parse(value: &str) -> Value {
        if let Ok(value) = value.parse::<bool>() {
            Value::Bool(value)
        } else if let Ok(value) = value.parse::<i64>() {
            Value::Int(value)
        } else if let Ok(value) = value.parse::<f64>() {
            Value::Float(value)
        } else {
            Value::String(value.to_string())
        }
    }
}

impl Value {
    /// Converts the value to a boolean.
    pub fn as_bool(&self) -> Result<bool, ReductError> {
        match self {
            Value::Bool(value) => Ok(*value),
            Value::Int(value) => Ok(value != &0),
            Value::Float(value) => Ok(value != &0.0),
            Value::String(value) => Ok(!value.is_empty()),
        }
    }

    /// Converts the value to an integer.
    #[allow(dead_code)]
    pub fn as_int(&self) -> Result<i64, ReductError> {
        match self {
            Value::Bool(value) => Ok(*value as i64),
            Value::Int(value) => Ok(*value),
            Value::Float(value) => Ok(*value as i64),
            Value::String(value) => {
                if let Ok(value) = value.parse::<i64>() {
                    Ok(value)
                } else {
                    Err(unprocessable_entity!(
                        "Value '{}' could not be parsed as integer",
                        value
                    ))
                }
            }
        }
    }

    /// Converts the value to a float.
    pub fn as_float(&self) -> Result<f64, ReductError> {
        match self {
            Value::Bool(value) => Ok(*value as i64 as f64),
            Value::Int(value) => Ok(*value as f64),
            Value::Float(value) => Ok(*value),
            Value::String(value) => {
                if let Ok(value) = value.parse::<f64>() {
                    Ok(value)
                } else {
                    Err(unprocessable_entity!(
                        "Value '{}' could not be parsed as float",
                        value
                    ))
                }
            }
        }
    }

    /// Converts the value to a string.
    #[allow(dead_code)]
    pub fn as_string(&self) -> Result<String, ReductError> {
        match self {
            Value::Bool(value) => Ok(value.to_string()),
            Value::Int(value) => Ok(value.to_string()),
            Value::Float(value) => Ok(value.to_string()),
            Value::String(value) => Ok(value.clone()),
        }
    }

    /// Check if it is a string
    pub fn is_string(&self) -> bool {
        match self {
            Value::String(_) => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    mod parse {
        use super::*;

        #[test]
        fn parse_bool() {
            let result = Value::parse("true");
            assert_eq!(result, Value::Bool(true));
        }

        #[test]
        fn parse_int() {
            let result = Value::parse("42");
            assert_eq!(result, Value::Int(42));

            let result = Value::parse("-42");
            assert_eq!(result, Value::Int(-42));
        }

        #[test]
        fn parse_float() {
            let result = Value::parse("42.0");
            assert_eq!(result, Value::Float(42.0));

            let result = Value::parse("2000.0");
            assert_eq!(result, Value::Float(2000.0));
        }

        #[test]
        fn parse_string() {
            let result = Value::parse("some string");
            assert_eq!(result, Value::String("some string".to_string()));
        }
    }

    mod as_bool {
        use super::*;

        #[rstest]
        #[case(Value::Bool(true), Ok(true))]
        #[case(Value::Bool(false), Ok(false))]
        #[case(Value::Int(1), Ok(true))]
        #[case(Value::Int(0), Ok(false))]
        #[case(Value::Int(-1), Ok(true))]
        #[case(Value::Int(2), Ok(true))]
        #[case(Value::Float(1.5), Ok(true))]
        #[case(Value::Float(0.0), Ok(false))]
        #[case(Value::Float(-1.5), Ok(true))]
        #[case(Value::String("string".to_string()), Ok(true))]
        #[case(Value::String("".to_string()), Ok(false))]
        fn as_bool(#[case] value: Value, #[case] expected: Result<bool, ReductError>) {
            let result = value.as_bool();
            assert_eq!(result, expected);
        }

        #[rstest]
        #[case(Value::Bool(true), Ok(1))]
        #[case(Value::Bool(false), Ok(0))]
        #[case(Value::Int(1), Ok(1))]
        #[case(Value::Int(0), Ok(0))]
        #[case(Value::Int(-1), Ok(-1))]
        #[case(Value::Float(1.5), Ok(1))]
        #[case(Value::Float(0.0), Ok(0))]
        #[case(Value::Float(-1.5), Ok(-1))]
        #[case(Value::String("42".to_string()), Ok(42))]
        #[case(Value::String("string".to_string()), Err(unprocessable_entity!("Value 'string' could not be parsed as integer")))]
        fn as_int(#[case] value: Value, #[case] expected: Result<i64, ReductError>) {
            let result = value.as_int();
            assert_eq!(result, expected);
        }

        #[rstest]
        #[case(Value::Bool(true), Ok(1.0))]
        #[case(Value::Bool(false), Ok(0.0))]
        #[case(Value::Int(1), Ok(1.0))]
        #[case(Value::Int(0), Ok(0.0))]
        #[case(Value::Int(-1), Ok(-1.0))]
        #[case(Value::Float(1.5), Ok(1.5))]
        #[case(Value::Float(0.0), Ok(0.0))]
        #[case(Value::Float(-1.5), Ok(-1.5))]
        #[case(Value::String("42.0".to_string()), Ok(42.0))]
        #[case(Value::String("string".to_string()), Err(unprocessable_entity!("Value 'string' could not be parsed as float")))]
        fn as_float(#[case] value: Value, #[case] expected: Result<f64, ReductError>) {
            let result = value.as_float();
            assert_eq!(result, expected);
        }

        #[rstest]
        #[case(Value::Bool(true), Ok("true".to_string()))]
        #[case(Value::Bool(false), Ok("false".to_string()))]
        #[case(Value::Int(42), Ok("42".to_string()))]
        #[case(Value::Float(42.0), Ok("42".to_string()))]
        #[case(Value::String("string".to_string()), Ok("string".to_string()))]
        fn as_string(#[case] value: Value, #[case] expected: Result<String, ReductError>) {
            let result = value.as_string();
            assert_eq!(result, expected);
        }
    }
}
