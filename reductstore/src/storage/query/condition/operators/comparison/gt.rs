// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Boxed, BoxedNode, Context, Node};
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// Gt node representing a logical greater than operation.
pub(crate) struct Gt {
    operands: Vec<BoxedNode>,
}

impl Node for Gt {
    fn apply(&mut self, context: &Context) -> Result<Value, ReductError> {
        let value_1 = self.operands[0].apply(context)?;
        let value_2 = self.operands[1].apply(context)?;
        Ok(Value::Bool(value_1 > value_2))
    }

    fn print(&self) -> String {
        format!("Gt({:?}, {:?})", self.operands[0], self.operands[1])
    }
}

impl Gt {
    pub fn new(operands: Vec<BoxedNode>) -> Self {
        Self { operands }
    }
}

impl Boxed for Gt {
    fn boxed(operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        if operands.len() != 2 {
            return Err(unprocessable_entity!("$gt requires exactly two operands"));
        }
        Ok(Box::new(Gt::new(operands)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::query::condition::constant::Constant;
    use rstest::rstest;

    #[rstest]
    #[case(Value::Int(1), Value::Int(1), Value::Bool(false))]
    #[case(Value::Int(1), Value::Int(2), Value::Bool(false))]
    #[case(Value::Int(2), Value::Int(1), Value::Bool(true))]
    fn apply(#[case] op_1: Value, #[case] op_2: Value, #[case] expected: Value) {
        let mut eq = Gt::new(vec![Constant::boxed(op_1), Constant::boxed(op_2)]);
        assert_eq!(eq.apply(&Context::default()).unwrap(), expected);
    }

    #[rstest]
    fn only_two_operands() {
        let operands: Vec<BoxedNode> = vec![Constant::boxed(Value::Bool(true))];
        let result = Gt::boxed(operands);
        assert_eq!(
            result.err().unwrap(),
            unprocessable_entity!("$gt requires exactly two operands")
        );
    }

    #[rstest]
    fn print() {
        let eq = Gt::new(vec![
            Constant::boxed(Value::Bool(true)),
            Constant::boxed(Value::Bool(false)),
        ]);
        assert_eq!(eq.print(), "Gt(Bool(true), Bool(false))");
    }
}
