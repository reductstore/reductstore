// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

#[derive(Debug, Clone)]
pub(crate) enum Value {
    Bool(bool),
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
