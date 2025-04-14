// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Add as AddTrait;
use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Boxed, BoxedNode, Context, Node};
use reduct_base::error::ReductError;

/// A node representing an arithmetic addition operation.
pub(crate) struct Add {
    operands: Vec<BoxedNode>,
}

impl Node for Add {
    fn apply(&mut self, context: &Context) -> Result<Value, ReductError> {
        let mut sum: Option<Value> = None;

        for operand in self.operands.iter_mut() {
            let value = operand.apply(context)?;
            match sum {
                Some(s) => sum = Some(s.add(value)?),
                None => sum = Some(value),
            }
        }

        Ok(sum.unwrap_or(Value::Int(0)))
    }

    fn operands(&self) -> &Vec<BoxedNode> {
        &self.operands
    }

    fn print(&self) -> String {
        format!("Add({:?})", self.operands)
    }
}

impl Boxed for Add {
    fn boxed(operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        Ok(Box::new(Self::new(operands)))
    }
}

impl Add {
    pub fn new(operands: Vec<BoxedNode>) -> Self {
        Self { operands }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::query::condition::constant::Constant;
    use crate::storage::query::condition::value::Value;
    use reduct_base::unprocessable_entity;
    use rstest::rstest;

    #[rstest]
    fn apply_ok() {
        let mut add = Add::new(vec![
            Constant::boxed(Value::Bool(true)),
            Constant::boxed(Value::Bool(false)),
        ]);

        assert_eq!(add.apply(&Context::default()).unwrap(), Value::Int(1));
    }

    #[rstest]
    fn apply_bad() {
        let mut add = Add::new(vec![
            Constant::boxed(Value::Bool(true)),
            Constant::boxed(Value::String("xxx".to_string())),
        ]);

        assert_eq!(
            add.apply(&Context::default()),
            Err(unprocessable_entity!("Cannot add boolean to string"))
        );
    }

    #[rstest]
    fn apply_empty() {
        let result = Add::new(vec![]).apply(&Context::default()).unwrap();
        assert_eq!(result, Value::Int(0));
    }

    #[rstest]
    fn print() {
        let and = Add::new(vec![Constant::boxed(Value::Bool(true))]);
        assert_eq!(and.print(), "Add([Bool(true)])");
    }
}
