// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Sub as SubTrait;
use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Boxed, BoxedNode, Context, Node};
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// A node representing an arithmetic subtraction operation.
pub(crate) struct Sub {
    operands: Vec<BoxedNode>,
}

impl Node for Sub {
    fn apply(&mut self, context: &Context) -> Result<Value, ReductError> {
        let value_1 = self.operands[0].apply(context)?;
        let value_2 = self.operands[1].apply(context)?;
        value_1.subtract(value_2)
    }

    fn print(&self) -> String {
        format!("Sub({:?}, {:?})", self.operands[0], self.operands[1])
    }
}

impl Boxed for Sub {
    fn boxed(operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        if operands.len() != 2 {
            return Err(unprocessable_entity!("$sub requires exactly two operands"));
        }

        Ok(Box::new(Sub::new(operands)))
    }
}

impl Sub {
    pub fn new(operands: Vec<BoxedNode>) -> Self {
        Self { operands }
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
        let mut sub = Sub::new(vec![
            Constant::boxed(Value::Int(1)),
            Constant::boxed(Value::Int(2)),
        ]);
        assert_eq!(sub.apply(&Context::default()).unwrap(), Value::Int(-1));
    }

    #[rstest]
    fn apply_bad() {
        let mut sub = Sub::new(vec![
            Constant::boxed(Value::Int(1)),
            Constant::boxed(Value::String("foo".to_string())),
        ]);
        assert_eq!(
            sub.apply(&Context::default()),
            Err(unprocessable_entity!("Cannot subtract string from integer"))
        );
    }

    #[rstest]
    fn apply_empty() {
        let op = Sub::boxed(vec![]);
        assert_eq!(
            op.err(),
            Some(unprocessable_entity!("$sub requires exactly two operands"))
        );
    }

    #[rstest]
    fn print() {
        let op = Sub::new(vec![
            Constant::boxed(Value::Bool(true)),
            Constant::boxed(Value::Bool(true)),
        ]);
        assert_eq!(op.print(), "Sub(Bool(true), Bool(true))");
    }
}
