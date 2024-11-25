// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Boxed, BoxedNode, Context, Node};
use reduct_base::error::ReductError;

/// A node representing a logical OR or ANY_OF operation.
pub(crate) struct AnyOf {
    operands: Vec<BoxedNode>,
}

impl Node for AnyOf {
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
        format!("AnyOf({:?})", self.operands)
    }
}

impl Boxed for AnyOf {
    fn boxed(operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        Ok(Box::new(Self::new(operands)))
    }
}

impl AnyOf {
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
    fn apply() {
        let or = AnyOf::new(vec![
            Constant::boxed(Value::Bool(false)),
            Constant::boxed(Value::Int(1)),
            Constant::boxed(Value::Float(-2.0)),
            Constant::boxed(Value::String("xxxx".to_string())),
        ]);
        assert_eq!(or.apply(&Context::default()).unwrap(), Value::Bool(true));

        let or = AnyOf::new(vec![
            Constant::boxed(Value::Bool(false)),
            Constant::boxed(Value::Bool(false)),
            Constant::boxed(Value::Bool(false)),
        ]);
        assert_eq!(or.apply(&Context::default()).unwrap(), Value::Bool(false));
    }

    #[rstest]
    fn apply_empty() {
        let or = AnyOf::new(vec![]);
        assert_eq!(or.apply(&Context::default()).unwrap(), Value::Bool(false));
    }

    #[rstest]
    fn print() {
        let or = AnyOf::new(vec![Constant::boxed(Value::Bool(true))]);
        assert_eq!(or.print(), "AnyOf([Bool(true)])");
    }
}
