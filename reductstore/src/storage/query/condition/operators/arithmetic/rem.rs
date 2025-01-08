// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Rem as RemTrait;
use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Boxed, BoxedNode, Context, Node};
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// A node representing a remainder operation.
pub(crate) struct Rem {
    op_1: BoxedNode,
    op_2: BoxedNode,
}

impl Node for Rem {
    fn apply(&self, context: &Context) -> Result<Value, ReductError> {
        let value_1 = self.op_1.apply(context)?;
        let value_2 = self.op_2.apply(context)?;
        value_1.remainder(value_2)
    }

    fn print(&self) -> String {
        format!("Rem({:?}, {:?})", self.op_1, self.op_2)
    }
}

impl Boxed for Rem {
    fn boxed(mut operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        if operands.len() != 2 {
            return Err(unprocessable_entity!("$rem requires exactly two operands"));
        }
        let op_2 = operands.pop().unwrap();
        let op_1 = operands.pop().unwrap();
        Ok(Box::new(Rem::new(op_1, op_2)))
    }
}

impl Rem {
    pub fn new(op_1: BoxedNode, op_2: BoxedNode) -> Self {
        Self { op_1, op_2 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::query::condition::constant::Constant;
    use crate::storage::query::condition::value::Value;
    use rstest::rstest;

    #[rstest]
    fn apply_ok() {
        let op = Rem::new(
            Constant::boxed(Value::Int(1)),
            Constant::boxed(Value::Int(2)),
        );
        assert_eq!(op.apply(&Context::default()).unwrap(), Value::Int(1));
    }

    #[rstest]
    fn apply_bad() {
        let op = Rem::new(
            Constant::boxed(Value::Int(1)),
            Constant::boxed(Value::String("foo".to_string())),
        );
        assert_eq!(
            op.apply(&Context::default()),
            Err(unprocessable_entity!("Cannot divide integer by string"))
        );
    }

    #[rstest]
    fn print() {
        let op = Rem::new(
            Constant::boxed(Value::Bool(true)),
            Constant::boxed(Value::Bool(true)),
        );
        assert_eq!(op.print(), "Rem(Bool(true), Bool(true))");
    }
}
