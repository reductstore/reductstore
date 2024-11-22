// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{BoxedNode, Context, Node};
use reduct_base::error::ReductError;

/// A node representing a logical Or operation.
pub(crate) struct Or {
    operands: Vec<BoxedNode>,
}

impl Node for Or {
    fn apply(&self, context: &Context) -> Result<Value, ReductError> {
        for operand in self.operands.iter() {
            let value = operand.apply(context)?;
            if value.as_bool()? {
                return Ok(Value::Bool(true));
            }
        }

        Ok(Value::Bool(false))
    }

    fn print(&self) -> String {
        format!("Or({:?})", self.operands)
    }
}

impl Or {
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
        let and = Or::new(vec![
            Constant::boxed(Value::Bool(false)),
            Constant::boxed(Value::Int(1)),
            Constant::boxed(Value::Float(-2.0)),
            Constant::boxed(Value::String("xxxx".to_string())),
        ]);

        let result = and.apply(&Context::default()).unwrap();
        assert_eq!(result, Value::Bool(true));

        let and = Or::new(vec![
            Constant::boxed(Value::Bool(false)),
            Constant::boxed(Value::Bool(false)),
            Constant::boxed(Value::Bool(false)),
        ]);

        let result = and.apply(&Context::default()).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[rstest]
    fn apply_empty() {
        let and = Or::new(vec![]);

        let result = and.apply(&Context::default()).unwrap();
        assert_eq!(result, Value::Bool(false));
    }
}
