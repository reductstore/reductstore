// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;

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

#[cfg(test)]
mod tests {
    use super::Value;
    use rstest::rstest;
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
}
