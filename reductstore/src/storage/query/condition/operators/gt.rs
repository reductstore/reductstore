// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Context, Node};

pub(crate) struct GreaterThan {
    left: Box<dyn Node>,
    right: Box<dyn Node>,
    holder: Value,
}

impl Node for GreaterThan {
    fn apply(&mut self, context: &Context) -> &Value {
        let left = self.left.apply(context);
        let right = self.right.apply(context);
        self.holder = (left > right).into();

        &self.holder
    }
}

impl GreaterThan {
    pub fn new(left: Box<dyn Node>, right: Box<dyn Node>) -> Self {
        GreaterThan {
            left,
            right,
            holder: Value::Bool(false),
        }
    }

    pub fn boxed(left: Box<dyn Node>, right: Box<dyn Node>) -> Box<Self> {
        Box::new(GreaterThan::new(left, right))
    }
}
