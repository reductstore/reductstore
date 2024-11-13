// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

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

impl PartialEq<Self> for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Bool(left), Value::Bool(right)) => left == right,
            (Value::Bool(left), Value::Int(right)) => *left as i64 == *right,
            (Value::Bool(left), Value::Float(right)) => *left as i64 as f64 == *right,

            (Value::Int(left), Value::Int(right)) => left == right,
            (Value::Int(left), Value::Bool(right)) => *left == *right as i64,
            (Value::Int(left), Value::Float(right)) => *left as f64 == *right,

            (Value::Float(left), Value::Float(right)) => left == right,
            (Value::Float(left), Value::Int(right)) => *left == *right as f64,
            (Value::Float(left), Value::Bool(right)) => *left == *right as i64 as f64,

            (Value::String(left), Value::String(right)) => left == right,
            (Value::String(_), _) => false,
            (_, Value::String(_)) => false,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Value::Bool(left), Value::Bool(right)) => left.partial_cmp(right),
            (Value::Bool(left), Value::Int(right)) => (*left as i64).partial_cmp(right),
            (Value::Bool(left), Value::Float(right)) => (*left as i64 as f64).partial_cmp(right),

            (Value::Int(left), Value::Int(right)) => left.partial_cmp(right),
            (Value::Int(left), Value::Bool(right)) => left.partial_cmp(&(*right as i64)),
            (Value::Int(left), Value::Float(right)) => (*left as f64).partial_cmp(right),

            (Value::Float(left), Value::Float(right)) => left.partial_cmp(right),
            (Value::Float(left), Value::Int(right)) => left.partial_cmp(&(*right as f64)),
            (Value::Float(left), Value::Bool(right)) => left.partial_cmp(&(*right as i64 as f64)),

            (Value::String(left), Value::String(right)) => left.partial_cmp(right),
            (Value::String(_), _) => None,
            (_, Value::String(_)) => None,
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

    mod partial_eq {
        use super::*;

        #[rstest]
        #[case(Value::Bool(true), Value::Bool(true), true)]
        #[case(Value::Bool(true), Value::Bool(false), false)]
        #[case(Value::Bool(false), Value::Bool(true), false)]
        #[case(Value::Bool(true), Value::Int(1), true)]
        #[case(Value::Bool(true), Value::Int(0), false)]
        #[case(Value::Bool(true), Value::Int(-1), false)]
        #[case(Value::Bool(true), Value::Float(1.0), true)]
        #[case(Value::Bool(true), Value::Float(0.0), false)]
        #[case(Value::Bool(true), Value::Float(-1.0), false)]
        #[case(Value::Bool(true), Value::String("string".to_string()), false)]
        #[case(Value::Bool(false), Value::String("string".to_string()), false)]
        #[case(Value::Bool(true), Value::String("true".to_string()), false)]
        fn partial_eq_bool(#[case] left: Value, #[case] right: Value, #[case] expected: bool) {
            let result = left == right;
            assert_eq!(result, expected);
        }

        #[rstest]
        #[case(Value::Int(1), Value::Int(1), true)]
        #[case(Value::Int(1), Value::Int(-1), false)]
        #[case(Value::Int(-1), Value::Int(-1), true)]
        #[case(Value::Int(-1), Value::Int(1), false)]
        #[case(Value::Int(1), Value::Bool(true), true)]
        #[case(Value::Int(1), Value::Bool(false), false)]
        #[case(Value::Int(0), Value::Bool(true), false)]
        #[case(Value::Int(0), Value::Bool(false), true)]
        #[case(Value::Int(-1), Value::Bool(true), false)]
        #[case(Value::Int(-1), Value::Bool(false), false)]
        #[case(Value::Int(1), Value::Float(1.0), true)]
        #[case(Value::Int(1), Value::Float(0.0), false)]
        #[case(Value::Int(1), Value::Float(-1.0), false)]
        #[case(Value::Int(1), Value::String("string".to_string()), false)]
        #[case(Value::Int(-1), Value::String("string".to_string()), false)]
        fn partial_eq_int(#[case] left: Value, #[case] right: Value, #[case] expected: bool) {
            let result = left == right;
            assert_eq!(result, expected);
        }

        #[rstest]
        #[case(Value::Float(1.0), Value::Float(1.0), true)]
        #[case(Value::Float(1.0), Value::Float(-1.0), false)]
        #[case(Value::Float(-1.0), Value::Float(-1.0), true)]
        #[case(Value::Float(-1.0), Value::Float(1.0), false)]
        #[case(Value::Float(1.0), Value::Bool(true), true)]
        #[case(Value::Float(1.0), Value::Bool(false), false)]
        #[case(Value::Float(0.0), Value::Bool(true), false)]
        #[case(Value::Float(0.0), Value::Bool(false), true)]
        #[case(Value::Float(-1.0), Value::Bool(true), false)]
        #[case(Value::Float(-1.0), Value::Bool(false), false)]
        #[case(Value::Float(1.0), Value::Int(1), true)]
        #[case(Value::Float(1.0), Value::Int(0), false)]
        #[case(Value::Float(1.0), Value::Int(-1), false)]
        #[case(Value::Float(1.0), Value::String("string".to_string()), false)]
        #[case(Value::Float(-1.0), Value::String("string".to_string()), false)]
        fn partial_eq_float(#[case] left: Value, #[case] right: Value, #[case] expected: bool) {
            let result = left == right;
            assert_eq!(result, expected);
        }

        #[rstest]
        #[case(Value::String("a".to_string()), Value::String("a".to_string()), true)]
        #[case(Value::String("a".to_string()), Value::String("b".to_string()), false)]
        #[case(Value::String("b".to_string()), Value::String("a".to_string()), false)]
        #[case(Value::String("a".to_string()), Value::Bool(true), false)]
        #[case(Value::String("a".to_string()), Value::Bool(false), false)]
        #[case(Value::String("a".to_string()), Value::Int(1), false)]
        #[case(Value::String("a".to_string()), Value::Float(1.0), false)]
        fn partial_eq_string(#[case] left: Value, #[case] right: Value, #[case] expected: bool) {
            let result = left == right;
            assert_eq!(result, expected);
        }
    }

    mod partial_cmp {
        use super::*;
        use std::cmp::Ordering;

        #[rstest]
        #[case(Value::Bool(true), Value::Bool(true), Some(Ordering::Equal))]
        #[case(Value::Bool(true), Value::Bool(false), Some(Ordering::Greater))]
        #[case(Value::Bool(false), Value::Bool(true), Some(Ordering::Less))]
        #[case(Value::Bool(true), Value::Int(2), Some(Ordering::Less))]
        #[case(Value::Bool(true), Value::Int(1), Some(Ordering::Equal))]
        #[case(Value::Bool(true), Value::Int(0), Some(Ordering::Greater))]
        #[case(Value::Bool(true), Value::Int(-1), Some(Ordering::Greater))]
        #[case(Value::Bool(false), Value::Int(1), Some(Ordering::Less))]
        #[case(Value::Bool(false), Value::Int(0), Some(Ordering::Equal))]
        #[case(Value::Bool(false), Value::Int(-1), Some(Ordering::Greater))]
        #[case(Value::Bool(true), Value::Float(2.0), Some(Ordering::Less))]
        #[case(Value::Bool(true), Value::Float(1.0), Some(Ordering::Equal))]
        #[case(Value::Bool(true), Value::Float(0.0), Some(Ordering::Greater))]
        #[case(Value::Bool(true), Value::Float(-1.0), Some(Ordering::Greater))]
        #[case(Value::Bool(false), Value::Float(1.0), Some(Ordering::Less))]
        #[case(Value::Bool(false), Value::Float(0.0), Some(Ordering::Equal))]
        #[case(Value::Bool(false), Value::Float(-1.0), Some(Ordering::Greater))]
        #[case(Value::Bool(true), Value::String("string".to_string()), None)]
        #[case(Value::Bool(false), Value::String("string".to_string()), None)]
        #[case(Value::Bool(true), Value::String("true".to_string()), None)]
        #[case(Value::Bool(false), Value::String("true".to_string()), None)]
        fn partial_cmp_bool(
            #[case] left: Value,
            #[case] right: Value,
            #[case] expected: Option<Ordering>,
        ) {
            let result = left.partial_cmp(&right);
            assert_eq!(result, expected);
        }

        #[rstest]
        #[case(Value::Int(1), Value::Int(1), Some(Ordering::Equal))]
        #[case(Value::Int(1), Value::Int(-1), Some(Ordering::Greater))]
        #[case(Value::Int(-1), Value::Int(-1), Some(Ordering::Equal))]
        #[case(Value::Int(-1), Value::Int(1), Some(Ordering::Less))]
        #[case(Value::Int(1), Value::Bool(true), Some(Ordering::Equal))]
        #[case(Value::Int(1), Value::Bool(false), Some(Ordering::Greater))]
        #[case(Value::Int(0), Value::Bool(true), Some(Ordering::Less))]
        #[case(Value::Int(0), Value::Bool(false), Some(Ordering::Equal))]
        #[case(Value::Int(-1), Value::Bool(true), Some(Ordering::Less))]
        #[case(Value::Int(-1), Value::Bool(false), Some(Ordering::Less))]
        #[case(Value::Int(1), Value::Float(2.0), Some(Ordering::Less))]
        #[case(Value::Int(1), Value::Float(1.0), Some(Ordering::Equal))]
        #[case(Value::Int(1), Value::Float(0.0), Some(Ordering::Greater))]
        #[case(Value::Int(1), Value::Float(-1.0), Some(Ordering::Greater))]
        #[case(Value::Int(1), Value::String("string".to_string()), None)]
        #[case(Value::Int(-1), Value::String("string".to_string()), None)]
        fn partial_cmp_int(
            #[case] left: Value,
            #[case] right: Value,
            #[case] expected: Option<Ordering>,
        ) {
            let result = left.partial_cmp(&right);
            assert_eq!(result, expected);
        }

        #[rstest]
        #[case(Value::Float(1.0), Value::Float(1.0), Some(Ordering::Equal))]
        #[case(Value::Float(1.0), Value::Float(-1.0), Some(Ordering::Greater))]
        #[case(Value::Float(-1.0), Value::Float(-1.0), Some(Ordering::Equal))]
        #[case(Value::Float(-1.0), Value::Float(1.0), Some(Ordering::Less))]
        #[case(Value::Float(1.0), Value::Bool(true), Some(Ordering::Equal))]
        #[case(Value::Float(1.0), Value::Bool(false), Some(Ordering::Greater))]
        #[case(Value::Float(0.0), Value::Bool(true), Some(Ordering::Less))]
        #[case(Value::Float(0.0), Value::Bool(false), Some(Ordering::Equal))]
        #[case(Value::Float(-1.0), Value::Bool(true), Some(Ordering::Less))]
        #[case(Value::Float(-1.0), Value::Bool(false), Some(Ordering::Less))]
        #[case(Value::Float(1.0), Value::Int(2), Some(Ordering::Less))]
        #[case(Value::Float(1.0), Value::Int(1), Some(Ordering::Equal))]
        #[case(Value::Float(1.0), Value::Int(0), Some(Ordering::Greater))]
        #[case(Value::Float(1.0), Value::Int(-1), Some(Ordering::Greater))]
        #[case(Value::Float(1.0), Value::String("string".to_string()), None)]
        #[case(Value::Float(-1.0), Value::String("string".to_string()), None)]
        fn partial_cmp_float(
            #[case] left: Value,
            #[case] right: Value,
            #[case] expected: Option<Ordering>,
        ) {
            let result = left.partial_cmp(&right);
            assert_eq!(result, expected);
        }

        #[rstest]
        #[case(Value::String("a".to_string()), Value::String("a".to_string()), Some(Ordering::Equal))]
        #[case(Value::String("a".to_string()), Value::String("b".to_string()), Some(Ordering::Less))]
        #[case(Value::String("b".to_string()), Value::String("a".to_string()), Some(Ordering::Greater))]
        #[case(Value::String("a".to_string()), Value::Bool(true), None)]
        #[case(Value::String("a".to_string()), Value::Bool(false), None)]
        #[case(Value::String("a".to_string()), Value::Int(1), None)]
        #[case(Value::String("a".to_string()), Value::Float(1.0), None)]
        fn partial_cmp_string(
            #[case] left: Value,
            #[case] right: Value,
            #[case] expected: Option<Ordering>,
        ) {
            let result = left.partial_cmp(&right);
            assert_eq!(result, expected);
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
    }
}
