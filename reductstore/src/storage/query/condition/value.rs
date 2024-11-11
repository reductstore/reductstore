// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

/// A value that can be used in a condition.
#[derive(Debug, Clone)]
pub(crate) enum Value {
    Bool(bool),
}

impl Value {
    /// Parses a string into a value.
    pub(crate) fn parse(value: &String) -> Option<Value> {
        if let Ok(value) = value.parse::<bool>() {
            Some(Value::Bool(value))
        } else {
            None
        }
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Bool(value)
    }
}

impl Into<bool> for Value {
    fn into(self) -> bool {
        match self {
            Value::Bool(value) => value,
        }
    }
}

impl PartialEq<Self> for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Bool(left), Value::Bool(right)) => left == right,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Value::Bool(left), Value::Bool(right)) => left.partial_cmp(right),
        }
    }
}

impl Value {
    pub fn as_bool(&self) -> Option<&bool> {
        match self {
            Value::Bool(value) => Some(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod parse {
        use super::*;

        #[test]
        fn parse_bool() {
            let result = Value::parse(&"true".to_string());
            assert_eq!(result, Some(Value::Bool(true)));
        }

        #[test]
        fn parse_invalid() {
            let result = Value::parse(&"invalid".to_string());
            assert_eq!(result, None);
        }
    }

    mod from {
        use super::*;

        #[test]
        fn from_bool() {
            let value: Value = true.into();
            assert_eq!(value, Value::Bool(true));
        }
    }

    mod into_ {
        use super::*;

        #[test]
        fn into_bool() {
            let value = Value::Bool(true);
            let result: bool = value.into();
            assert_eq!(result, true);
        }
    }

    mod eq {
        use super::*;

        #[test]
        fn eq_bool() {
            let left = Value::Bool(true);
            let right = Value::Bool(true);
            assert_eq!(left, right);
        }
    }

    mod partial_cmp {
        use super::*;

        #[test]
        fn partial_cmp_bool() {
            let left = Value::Bool(true);
            let right = Value::Bool(true);
            assert_eq!(left.partial_cmp(&right), Some(std::cmp::Ordering::Equal));
        }
    }

    mod as_bool {
        use super::*;

        #[test]
        fn as_bool() {
            let value = Value::Bool(true);
            let result = value.as_bool();
            assert_eq!(result, Some(&true));
        }
    }
}
