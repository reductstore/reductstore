// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::DivNum as DivNumTrait;
use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Boxed, BoxedNode, Context, Node};
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// A node representing an integer division operation.
pub(crate) struct DivNum {
    operands: Vec<BoxedNode>,
}

impl Node for DivNum {
    fn apply(&self, context: &Context) -> Result<Value, ReductError> {
        let value_1 = self.operands[0].apply(context)?;
        let value_2 = self.operands[1].apply(context)?;
        value_1.divide_num(value_2)
    }

    fn operands(&self) -> &Vec<BoxedNode> {
        &self.operands
    }

    fn print(&self) -> String {
        format!("DivNum({:?}, {:?})", self.operands[0], self.operands[1])
    }
}

impl Boxed for DivNum {
    fn boxed(mut operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        if operands.len() != 2 {
            return Err(unprocessable_entity!(
                "$div_num requires exactly two operands"
            ));
        }

        Ok(Box::new(DivNum::new(operands)))
    }
}

impl DivNum {
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
        let sub = DivNum::new(
            Constant::boxed(Value::Int(1)),
            Constant::boxed(Value::Int(2)),
        );
        assert_eq!(sub.apply(&Context::default()).unwrap(), Value::Int(0));
    }

    #[rstest]
    fn apply_bad() {
        let sub = DivNum::new(
            Constant::boxed(Value::Int(1)),
            Constant::boxed(Value::String("foo".to_string())),
        );
        assert_eq!(
            sub.apply(&Context::default()),
            Err(unprocessable_entity!("Cannot divide by string"))
        );
    }

    #[rstest]
    fn apply_empty() {
        let op = DivNum::boxed(vec![]);
        assert_eq!(
            op.err(),
            Some(unprocessable_entity!(
                "$div_num requires exactly two operands"
            ))
        );
    }

    #[rstest]
    fn print() {
        let op = DivNum::new(
            Constant::boxed(Value::Bool(true)),
            Constant::boxed(Value::Bool(true)),
        );
        assert_eq!(op.print(), "DivNum(Bool(true), Bool(true))");
    }
}
