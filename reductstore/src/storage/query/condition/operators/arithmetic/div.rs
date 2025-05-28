// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Div as DivTrait;
use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Boxed, BoxedNode, Context, Node};
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// A node representing an arithmetic division operation.
pub(crate) struct Div {
    operands: Vec<BoxedNode>,
}

impl Node for Div {
    fn apply(&mut self, context: &Context) -> Result<Value, ReductError> {
        let value_1 = self.operands[0].apply(context)?;
        let value_2 = self.operands[1].apply(context)?;
        value_1.divide(value_2)
    }

    fn print(&self) -> String {
        format!("Div({:?}, {:?})", self.operands[0], self.operands[1])
    }
}

impl Boxed for Div {
    fn boxed(operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        if operands.len() != 2 {
            return Err(unprocessable_entity!("$div requires exactly two operands"));
        }

        Ok(Box::new(Div::new(operands)))
    }
}

impl Div {
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
        let mut sub = Div::new(vec![
            Constant::boxed(Value::Int(1)),
            Constant::boxed(Value::Int(2)),
        ]);
        assert_eq!(sub.apply(&Context::default()).unwrap(), Value::Float(0.5));
    }

    #[rstest]
    fn apply_bad() {
        let mut sub = Div::new(vec![
            Constant::boxed(Value::Int(1)),
            Constant::boxed(Value::String("foo".to_string())),
        ]);
        assert_eq!(
            sub.apply(&Context::default()),
            Err(unprocessable_entity!("Cannot divide by string"))
        );
    }

    #[rstest]
    fn apply_empty() {
        let op = Div::boxed(vec![]);
        assert_eq!(
            op.err(),
            Some(unprocessable_entity!("$div requires exactly two operands"))
        );
    }

    #[rstest]
    fn print() {
        let op = Div::new(vec![
            Constant::boxed(Value::Bool(true)),
            Constant::boxed(Value::Bool(true)),
        ]);
        assert_eq!(op.print(), "Div(Bool(true), Bool(true))");
    }
}
