// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{BoxedNode, Context, Node};
use reduct_base::error::ReductError;

/// A node representing a logical AND operation.
pub(crate) struct And {
    operands: Vec<BoxedNode>,
}

impl Node for And {
    fn apply(&self, context: &Context) -> Result<Value, ReductError> {
        for operand in self.operands.iter() {
            let value = operand.apply(context)?;
            if !*value.as_bool() {
                return Ok(Value::Bool(false));
            }
        }

        Ok(Value::Bool(true))
    }

    fn print(&self) -> String {
        format!("And({:?})", self.operands)
    }
}

impl And {
    pub fn new(operands: Vec<BoxedNode>) -> Self {
        And { operands }
    }

    pub fn boxed(operands: Vec<BoxedNode>) -> BoxedNode {
        Box::new(And::new(operands))
    }
}
