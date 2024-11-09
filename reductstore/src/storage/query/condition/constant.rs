// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Context, Node};

pub(super) struct Constant {
    value: Value,
}

impl Node for Constant {
    fn apply(&mut self, context: &Context) -> &Value {
        &self.value
    }
}

impl Constant {
    pub fn new(value: Value) -> Self {
        Constant { value }
    }

    pub fn boxed(value: Value) -> Box<Self> {
        Box::new(Constant::new(value))
    }
}

impl From<bool> for Constant {
    fn from(value: bool) -> Self {
        Constant {
            value: Value::Bool(value),
        }
    }
}
