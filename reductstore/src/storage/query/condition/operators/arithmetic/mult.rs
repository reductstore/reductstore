// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Mult as MultTrait;
use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Boxed, BoxedNode, Context, Node};
use reduct_base::error::ReductError;

/// A node representing an arithmetic multiplication operation.
pub(crate) struct Mult {
    operands: Vec<BoxedNode>,
}

impl Node for Mult {
    fn apply(&self, context: &Context) -> Result<Value, ReductError> {
        let mut prod: Option<Value> = None;

        for operand in self.operands.iter() {
            let value = operand.apply(context)?;
            match prod {
                Some(s) => prod = Some(s.multiply(value)?),
                None => prod = Some(value),
            }
        }

        Ok(prod.unwrap_or(Value::Int(0)))
    }

    fn print(&self) -> String {
        format!("Mult({:?})", self.operands)
    }
}

impl Boxed for Mult {
    fn boxed(operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        Ok(Box::new(Self::new(operands)))
    }
}

impl Mult {
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
        let op = Mult::new(vec![
            Constant::boxed(Value::Bool(true)),
            Constant::boxed(Value::Int(1)),
            Constant::boxed(Value::Float(2.0)),
        ]);

        assert_eq!(op.apply(&Context::default()).unwrap(), Value::Float(2.0));
    }

    #[rstest]
    fn apply_bad() {
        let op = Mult::new(vec![
            Constant::boxed(Value::Bool(true)),
            Constant::boxed(Value::String("xxx".to_string())),
        ]);

        assert_eq!(
            op.apply(&Context::default()),
            Err(unprocessable_entity!("Cannot multiply boolean by string"))
        );
    }

    #[rstest]
    fn apply_empty() {
        let result = Mult::new(vec![]).apply(&Context::default()).unwrap();
        assert_eq!(result, Value::Int(0));
    }

    #[rstest]
    fn print() {
        let and = Mult::new(vec![Constant::boxed(Value::Bool(true))]);
        assert_eq!(and.print(), "Mult([Bool(true)])");
    }
}
