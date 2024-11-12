// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

/// A value that can be used in a condition.
#[derive(Debug, Clone)]
pub(crate) enum Value {
    Bool(bool),
    Int(i64),
    Float(f64),
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
    pub(crate) fn parse(value: &str) -> Option<Value> {
        if let Ok(value) = value.parse::<bool>() {
            Some(Value::Bool(value))
        } else if let Ok(value) = value.parse::<i64>() {
            Some(Value::Int(value))
        } else if let Ok(value) = value.parse::<f64>() {
            Some(Value::Float(value))
        } else {
            None
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
        }
    }
}

impl Value {
    /// Converts the value to a boolean.
    pub fn as_bool(&self) -> bool {
        match self {
            Value::Bool(value) => *value,
            Value::Int(value) => value != &0,
            Value::Float(value) => value != &0.0,
        }
    }

    /// Converts the value to an integer.
    pub fn as_int(&self) -> i64 {
        match self {
            Value::Bool(value) => *value as i64,
            Value::Int(value) => *value,
            Value::Float(value) => *value as i64,
        }
    }

    /// Converts the value to a float.
    pub fn as_float(&self) -> f64 {
        match self {
            Value::Bool(value) => *value as i64 as f64,
            Value::Int(value) => *value as f64,
            Value::Float(value) => *value,
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
            assert_eq!(result, Some(Value::Bool(true)));
        }

        #[test]
        fn parse_int() {
            let result = Value::parse("42");
            assert_eq!(result, Some(Value::Int(42)));

            let result = Value::parse("-42");
            assert_eq!(result, Some(Value::Int(-42)));
        }

        #[test]
        fn parse_float() {
            let result = Value::parse("42.0");
            assert_eq!(result, Some(Value::Float(42.0)));

            let result = Value::parse("2000.0");
            assert_eq!(result, Some(Value::Float(2000.0)));
        }

        #[test]
        fn parse_invalid() {
            let result = Value::parse("invalid");
            assert_eq!(result, None);
        }
    }

    mod partial_cmp {
        use super::*;
        use std::cmp::Ordering;

        #[rstest]
        #[case(Value::Bool(true), Value::Bool(true), Ordering::Equal)]
        #[case(Value::Bool(true), Value::Bool(false), Ordering::Greater)]
        #[case(Value::Bool(false), Value::Bool(true), Ordering::Less)]
        #[case(Value::Bool(true), Value::Int(2), Ordering::Less)]
        #[case(Value::Bool(true), Value::Int(1), Ordering::Equal)]
        #[case(Value::Bool(true), Value::Int(0), Ordering::Greater)]
        #[case(Value::Bool(true), Value::Int(-1), Ordering::Greater)]
        #[case(Value::Bool(false), Value::Int(1), Ordering::Less)]
        #[case(Value::Bool(false), Value::Int(0), Ordering::Equal)]
        #[case(Value::Bool(false), Value::Int(-1), Ordering::Greater)]
        #[case(Value::Bool(true), Value::Float(2.0), Ordering::Less)]
        #[case(Value::Bool(true), Value::Float(1.0), Ordering::Equal)]
        #[case(Value::Bool(true), Value::Float(0.0), Ordering::Greater)]
        #[case(Value::Bool(true), Value::Float(-1.0), Ordering::Greater)]
        #[case(Value::Bool(false), Value::Float(1.0), Ordering::Less)]
        #[case(Value::Bool(false), Value::Float(0.0), Ordering::Equal)]
        #[case(Value::Bool(false), Value::Float(-1.0), Ordering::Greater)]
        fn partial_cmp_bool(#[case] left: Value, #[case] right: Value, #[case] expected: Ordering) {
            let result = left.partial_cmp(&right);
            assert_eq!(result, Some(expected));
        }

        #[rstest]
        #[case(Value::Int(1), Value::Int(1), Ordering::Equal)]
        #[case(Value::Int(1), Value::Int(-1), Ordering::Greater)]
        #[case(Value::Int(-1), Value::Int(-1), Ordering::Equal)]
        #[case(Value::Int(-1), Value::Int(1), Ordering::Less)]
        #[case(Value::Int(1), Value::Bool(true), Ordering::Equal)]
        #[case(Value::Int(1), Value::Bool(false), Ordering::Greater)]
        #[case(Value::Int(0), Value::Bool(true), Ordering::Less)]
        #[case(Value::Int(0), Value::Bool(false), Ordering::Equal)]
        #[case(Value::Int(-1), Value::Bool(true), Ordering::Less)]
        #[case(Value::Int(-1), Value::Bool(false), Ordering::Less)]
        #[case(Value::Int(1), Value::Float(2.0), Ordering::Less)]
        #[case(Value::Int(1), Value::Float(1.0), Ordering::Equal)]
        #[case(Value::Int(1), Value::Float(0.0), Ordering::Greater)]
        #[case(Value::Int(1), Value::Float(-1.0), Ordering::Greater)]
        fn partial_cmp_int(#[case] left: Value, #[case] right: Value, #[case] expected: Ordering) {
            let result = left.partial_cmp(&right);
            assert_eq!(result, Some(expected));
        }

        #[rstest]
        #[case(Value::Float(1.0), Value::Float(1.0), Ordering::Equal)]
        #[case(Value::Float(1.0), Value::Float(-1.0), Ordering::Greater)]
        #[case(Value::Float(-1.0), Value::Float(-1.0), Ordering::Equal)]
        #[case(Value::Float(-1.0), Value::Float(1.0), Ordering::Less)]
        #[case(Value::Float(1.0), Value::Bool(true), Ordering::Equal)]
        #[case(Value::Float(1.0), Value::Bool(false), Ordering::Greater)]
        #[case(Value::Float(0.0), Value::Bool(true), Ordering::Less)]
        #[case(Value::Float(0.0), Value::Bool(false), Ordering::Equal)]
        #[case(Value::Float(-1.0), Value::Bool(true), Ordering::Less)]
        #[case(Value::Float(-1.0), Value::Bool(false), Ordering::Less)]
        #[case(Value::Float(1.0), Value::Int(2), Ordering::Less)]
        #[case(Value::Float(1.0), Value::Int(1), Ordering::Equal)]
        #[case(Value::Float(1.0), Value::Int(0), Ordering::Greater)]
        #[case(Value::Float(1.0), Value::Int(-1), Ordering::Greater)]
        fn partial_cmp_float(
            #[case] left: Value,
            #[case] right: Value,
            #[case] expected: Ordering,
        ) {
            let result = left.partial_cmp(&right);
            assert_eq!(result, Some(expected));
        }
    }

    mod as_bool {
        use super::*;

        #[rstest]
        #[case(Value::Bool(true), true)]
        #[case(Value::Bool(false), false)]
        #[case(Value::Int(1), true)]
        #[case(Value::Int(0), false)]
        #[case(Value::Int(-1), true)]
        #[case(Value::Int(2), true)]
        #[case(Value::Float(1.5), true)]
        #[case(Value::Float(0.0), false)]
        #[case(Value::Float(-1.5), true)]
        fn as_bool(#[case] value: Value, #[case] expected: bool) {
            let result = value.as_bool();
            assert_eq!(result, expected);
        }

        #[rstest]
        #[case(Value::Bool(true), 1)]
        #[case(Value::Bool(false), 0)]
        #[case(Value::Int(1), 1)]
        #[case(Value::Int(0), 0)]
        #[case(Value::Int(-1), -1)]
        #[case(Value::Float(1.5), 1)]
        #[case(Value::Float(0.0), 0)]
        #[case(Value::Float(-1.5), -1)]
        fn as_int(#[case] value: Value, #[case] expected: i64) {
            let result = value.as_int();
            assert_eq!(result, expected);
        }

        #[rstest]
        #[case(Value::Bool(true), 1.0)]
        #[case(Value::Bool(false), 0.0)]
        #[case(Value::Int(1), 1.0)]
        #[case(Value::Int(0), 0.0)]
        #[case(Value::Int(-1), -1.0)]
        #[case(Value::Float(1.5), 1.5)]
        #[case(Value::Float(0.0), 0.0)]
        #[case(Value::Float(-1.5), -1.5)]
        fn as_float(#[case] value: Value, #[case] expected: f64) {
            let result = value.as_float();
            assert_eq!(result, expected);
        }
    }
}
