// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Boxed, BoxedNode, Context, Node};
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// A node representing an `nin` operation.
pub(crate) struct Nin {
    operands: Vec<BoxedNode>,
}

impl Node for Nin {
    fn apply(&self, context: &Context) -> Result<Value, ReductError> {
        let op_value = self.operands[0].apply(context)?;
        for item in self.operands[1..].iter() {
            if item.apply(context)? == op_value {
                return Ok(Value::Bool(false));
            }
        }

        Ok(Value::Bool(true))
    }
    fn operands(&self) -> &Vec<BoxedNode> {
        &self.operands
    }

    fn print(&self) -> String {
        format!(
            "Nin({:?}, [{:}])",
            self.operands[0],
            self.operands[1..]
                .iter()
                .map(|op| format!("{:?}", op))
                .reduce(|a, b| format!("{:?}, {:?}", a, b))
                .unwrap()
        )
    }
}

impl Boxed for Nin {
    fn boxed(operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        if operands.len() < 2 {
            return Err(unprocessable_entity!(
                "$nin operator requires at least two operands"
            ));
        }
        Ok(Box::new(Self::new(operands)))
    }
}

impl Nin {
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
    #[case(Value::Int(1), vec![Value::Int(0), Value::Int(1)], Value::Bool(false))]
    #[case(Value::Int(1), vec![Value::Int(0), Value::Int(2)], Value::Bool(true))]
    #[case(Value::Int(1), vec![Value::Int(0), Value::Int(2), Value::Float(1.0)], Value::Bool(false))]
    fn apply_ok(#[case] op: Value, #[case] list: Vec<Value>, #[case] expected: Value) {
        let mut operands: Vec<BoxedNode> = vec![Constant::boxed(op)];
        operands.extend(list.iter().map(|v| Constant::boxed(v.clone()) as BoxedNode));

        let op = Nin::new(operands);
        assert_eq!(op.apply(&Context::default()).unwrap(), expected);
    }

    #[rstest]
    fn apply_empty() {
        let op = Nin::boxed(vec![]);
        assert_eq!(
            op.err().unwrap(),
            unprocessable_entity!("$nin operator requires at least two operands")
        );
    }

    #[rstest]
    fn print() {
        let op = Nin::boxed(vec![
            Constant::boxed(Value::Bool(true)),
            Constant::boxed(Value::Bool(false)),
        ])
        .unwrap();
        assert_eq!(op.print(), "Nin(Bool(true), [Bool(false)])");
    }
}
