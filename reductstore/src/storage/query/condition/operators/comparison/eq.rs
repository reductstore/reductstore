// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Boxed, BoxedNode, Context, Node};
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// A node representing a logical equality operation.
pub(crate) struct Eq {
    op_1: BoxedNode,
    op_2: BoxedNode,
}

impl Node for Eq {
    fn apply(&self, context: &Context) -> Result<Value, ReductError> {
        let value_1 = self.op_1.apply(context)?;
        let value_2 = self.op_2.apply(context)?;
        Ok(Value::Bool(value_1 == value_2))
    }

    fn print(&self) -> String {
        format!("Eq({:?}, {:?})", self.op_1, self.op_2)
    }
}

impl Eq {
    pub fn new(op_1: BoxedNode, op_2: BoxedNode) -> Self {
        Self { op_1, op_2 }
    }
}

impl Boxed for Eq {
    fn boxed(mut operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        if operands.len() != 2 {
            return Err(unprocessable_entity!("Eq requires exactly two operands"));
        }

        Ok(Box::new(Eq::new(
            operands.pop().unwrap(),
            operands.pop().unwrap(),
        )))
    }
}

#[cfg(test)]
mod tests {}
