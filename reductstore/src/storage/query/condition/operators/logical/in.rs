// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Boxed, BoxedNode, Context, Node};
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// A node representing an `in` operation.
pub(crate) struct In {
    op: BoxedNode,
    list: Vec<BoxedNode>,
}

impl Node for In {
    fn apply(&self, context: &Context) -> Result<Value, ReductError> {
        let op_value = self.op.apply(context)?;
        for item in self.list.iter() {
            if item.apply(context)? == op_value {
                return Ok(Value::Bool(true));
            }
        }

        Ok(Value::Bool(false))
    }

    fn print(&self) -> String {
        format!("In({:?}, {:?})", self.op, self.list)
    }
}

impl Boxed for In {
    fn boxed(operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        if operands.len() < 2 {
            return Err(unprocessable_entity!(
                "$in operator requires at least two operands"
            ));
        }
        Ok(Box::new(Self::new(operands)))
    }
}

impl In {
    pub fn new(mut operands: Vec<BoxedNode>) -> Self {
        let op = operands.remove(0);
        Self { op, list: operands }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::query::condition::constant::Constant;
    use crate::storage::query::condition::value::Value;
    use rstest::rstest;

    #[rstest]
    #[case(Value::Int(1), vec![Value::Int(0), Value::Int(1)], Value::Bool(true))]
    #[case(Value::Int(1), vec![Value::Int(0), Value::Int(2)], Value::Bool(false))]
    #[case(Value::Int(1), vec![Value::Int(0), Value::Int(2), Value::Float(1.0)], Value::Bool(true))]
    fn apply_ok(#[case] op: Value, #[case] list: Vec<Value>, #[case] expected: Value) {
        let mut operands: Vec<BoxedNode> = vec![Constant::boxed(op)];
        operands.extend(list.iter().map(|v| Constant::boxed(v.clone()) as BoxedNode));

        let op = In::new(operands);
        assert_eq!(op.apply(&Context::default()).unwrap(), expected);
    }

    #[rstest]
    fn apply_empty() {
        let op = In::boxed(vec![]);
        assert_eq!(
            op.err().unwrap(),
            unprocessable_entity!("$in operator requires at least two operands")
        );
    }

    #[rstest]
    fn print() {
        let op = In::boxed(vec![
            Constant::boxed(Value::Bool(true)),
            Constant::boxed(Value::Bool(false)),
        ])
        .unwrap();
        assert_eq!(op.print(), "In(Bool(true), [Bool(false)])");
    }
}
