// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{BoxedNode, Context, Node};
use reduct_base::error::ReductError;

/// A node representing a logical AND or ALL_OF operation.
pub(crate) struct AllOf {
    operands: Vec<BoxedNode>,
}

impl Node for AllOf {
    fn apply(&self, context: &Context) -> Result<Value, ReductError> {
        for operand in self.operands.iter() {
            let value = operand.apply(context)?;
            if !value.as_bool()? {
                return Ok(Value::Bool(false));
            }
        }

        Ok(Value::Bool(true))
    }

    fn print(&self) -> String {
        format!("AllOf({:?})", self.operands)
    }
}

impl AllOf {
    pub fn new(operands: Vec<BoxedNode>) -> Self {
        Self { operands }
    }

    pub fn boxed(operands: Vec<BoxedNode>) -> BoxedNode {
        Box::new(Self::new(operands))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::query::condition::constant::Constant;
    use crate::storage::query::condition::value::Value;
    use rstest::rstest;

    #[rstest]
    fn apply() {
        let and = AllOf::new(vec![
            Constant::boxed(Value::Bool(true)),
            Constant::boxed(Value::Int(1)),
            Constant::boxed(Value::Float(-2.0)),
            Constant::boxed(Value::String("xxxx".to_string())),
        ]);
        assert_eq!(and.apply(&Context::default()).unwrap(), Value::Bool(true));

        let and = AllOf::new(vec![
            Constant::boxed(Value::Bool(true)),
            Constant::boxed(Value::Bool(false)),
            Constant::boxed(Value::Bool(true)),
        ]);
        assert_eq!(and.apply(&Context::default()).unwrap(), Value::Bool(false));
    }

    #[rstest]
    fn apply_empty() {
        let result = AllOf::new(vec![]).apply(&Context::default()).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[rstest]
    fn print() {
        let and = AllOf::new(vec![Constant::boxed(Value::Bool(true))]);
        assert_eq!(and.print(), "AllOf([Bool(true)])");
    }
}
