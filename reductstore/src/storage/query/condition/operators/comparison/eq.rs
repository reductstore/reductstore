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
mod tests {
    use super::*;
    use rstest::rstest;
    use crate::storage::query::condition::constant::Constant;

    #[rstest]
    #[case(Value::Bool(true), Value::Bool(true), Value::Bool(true))]
    #[case(Value::Bool(true), Value::Bool(false), Value::Bool(false))]
    #[case(Value::Int(1), Value::Int(1), Value::Bool(true))]
    #[case(Value::Int(1), Value::Int(2), Value::Bool(false))]
    #[case(Value::Float(1.0), Value::Float(1.0), Value::Bool(true))]
    #[case(Value::Float(1.0), Value::Float(2.0), Value::Bool(false))]
    #[case(Value::String("a".to_string()), Value::String("a".to_string()), Value::Bool(true))]
    #[case(Value::String("a".to_string()), Value::String("b".to_string()), Value::Bool(false))]
    #[case(Value::Int(1), Value::Float(1.0), Value::Bool(true))]
    #[case(Value::Int(1), Value::Float(2.0), Value::Bool(false))]
    #[case(Value::Bool(true), Value::Int(1), Value::Bool(true))]
    #[case(Value::Bool(true), Value::Int(2), Value::Bool(false))]
    #[case(Value::Bool(true), Value::Float(0.0), Value::Bool(false))]
    fn apply(
        #[case] op_1: Value,
        #[case] op_2: Value,
        #[case] expected: Value,
    ) {
        let eq = Eq::new(Constant::boxed(op_1), Constant::boxed(op_2));
        assert_eq!(eq.apply(&Context::default()).unwrap(), expected);
    }

    #[rstest]
    fn print() {
        let eq = Eq::new(Constant::boxed(Value::Bool(true)), Constant::boxed(Value::Bool(false)));
        assert_eq!(eq.print(), "Eq(Bool(true), Bool(false))");
    }

}
