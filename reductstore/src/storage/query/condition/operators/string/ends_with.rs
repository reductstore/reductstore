// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::EndsWith as EndsWithValue;
use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Boxed, BoxedNode, Context, Node};
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

pub(crate) struct EndsWith {
    op_1: BoxedNode,
    op_2: BoxedNode,
}

impl Node for EndsWith {
    fn apply(&self, context: &Context) -> Result<Value, ReductError> {
        let value_1 = self.op_1.apply(context)?;
        let value_2 = self.op_2.apply(context)?;
        Ok(Value::Bool(value_1.ends_with(value_2)?))
    }

    fn print(&self) -> String {
        format!("EndsWith({:?}, {:?})", self.op_1, self.op_2)
    }
}

impl EndsWith {
    pub fn new(op_1: BoxedNode, op_2: BoxedNode) -> Self {
        Self { op_1, op_2 }
    }
}

impl Boxed for EndsWith {
    fn boxed(mut operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        if operands.len() != 2 {
            return Err(unprocessable_entity!(
                "$ends_with requires exactly two operands"
            ));
        }
        let op_2 = operands.pop().unwrap();
        let op_1 = operands.pop().unwrap();
        Ok(Box::new(EndsWith::new(op_1, op_2)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::query::condition::constant::Constant;
    use rstest::rstest;

    #[rstest]
    #[case(Value::String("test".to_string()), Value::String("es".to_string()), Value::Bool(false))]
    #[case(Value::String("test".to_string()), Value::String("st".to_string()), Value::Bool(true))]
    fn apply(#[case] op_1: Value, #[case] op_2: Value, #[case] expected: Value) {
        let contains = EndsWith::new(Constant::boxed(op_1), Constant::boxed(op_2));
        assert_eq!(contains.apply(&Context::default()).unwrap(), expected);
    }

    #[rstest]
    fn only_two_operands() {
        let operands: Vec<BoxedNode> = vec![Constant::boxed(Value::String("test".to_string()))];
        assert_eq!(
            EndsWith::boxed(operands).err().unwrap(),
            unprocessable_entity!("$ends_with requires exactly two operands")
        );
    }
}
