// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Boxed, BoxedNode, Context, Node};
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// Ne node representing a logical inequality operation.
pub(crate) struct Ne {
    op_1: BoxedNode,
    op_2: BoxedNode,
}

impl Node for Ne {
    fn apply(&self, context: &Context) -> Result<Value, ReductError> {
        let value_1 = self.op_1.apply(context)?;
        let value_2 = self.op_2.apply(context)?;
        Ok(Value::Bool(value_1 != value_2))
    }

    fn print(&self) -> String {
        format!("Ne({:?}, {:?})", self.op_1, self.op_2)
    }
}

impl Ne {
    pub fn new(op_1: BoxedNode, op_2: BoxedNode) -> Self {
        Self { op_1, op_2 }
    }
}

impl Boxed for Ne {
    fn boxed(mut operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        if operands.len() != 2 {
            return Err(unprocessable_entity!("$ne requires exactly two operands"));
        }
        let op_2 = operands.pop().unwrap();
        let op_1 = operands.pop().unwrap();
        Ok(Box::new(Ne::new(op_1, op_2)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use crate::storage::query::condition::constant::Constant;

    #[rstest]
    #[case(Value::Int(1), Value::Int(1), Value::Bool(false))]
    #[case(Value::Int(1), Value::Int(2), Value::Bool(true))]
    #[case(Value::Int(2), Value::Int(1), Value::Bool(true))]
    fn apply(
        #[case] op_1: Value,
        #[case] op_2: Value,
        #[case] expected: Value,
    ) {
        let eq = Ne::new(Constant::boxed(op_1), Constant::boxed(op_2));
        assert_eq!(eq.apply(&Context::default()).unwrap(), expected);
    }

    #[rstest]
    fn only_two_operands() {
        let operands: Vec<BoxedNode> = vec![Constant::boxed(Value::Bool(true))];
        let result = Ne::boxed(operands);
        assert_eq!(result.err().unwrap(), unprocessable_entity!("$ne requires exactly two operands"));
    }

    #[rstest]
    fn print() {
        let eq = Ne::new(Constant::boxed(Value::Bool(true)), Constant::boxed(Value::Bool(false)));
        assert_eq!(eq.print(), "Ne(Bool(true), Bool(false))");
    }

}
