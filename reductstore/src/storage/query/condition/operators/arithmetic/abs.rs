// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Abs as AbsTrait;
use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Boxed, BoxedNode, Context, Node};
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// A node representing an absolute value operation.
pub(crate) struct Abs {
    op: BoxedNode,
}

impl Node for Abs {
    fn apply(&self, context: &Context) -> Result<Value, ReductError> {
        let value = self.op.apply(context)?;
        value.abs()
    }

    fn print(&self) -> String {
        format!("Abs({:?})", self.op)
    }
}

impl Boxed for Abs {
    fn boxed(mut operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        if operands.len() != 1 {
            return Err(unprocessable_entity!("Abs requires exactly one operand"));
        }

        let op = operands.pop().unwrap();
        Ok(Box::new(Self::new(op)))
    }
}

impl Abs {
    pub fn new(op: BoxedNode) -> Self {
        Self { op }
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
        let op = Abs::new(Constant::boxed(Value::Int(-1)));
        assert_eq!(op.apply(&Context::default()).unwrap(), Value::Int(1));
    }

    #[rstest]
    fn apply_bad() {
        let op = Abs::new(Constant::boxed(Value::String("foo".to_string())));
        assert_eq!(
            op.apply(&Context::default()).unwrap_err(),
            unprocessable_entity!("Cannot calculate absolute value of a string")
        );
    }

    #[rstest]
    fn apply_empty() {
        let result = Abs::boxed(vec![]);
        assert_eq!(
            result.err().unwrap(),
            unprocessable_entity!("Abs requires exactly one operand")
        );
    }

    #[rstest]
    fn print() {
        let and = Abs::new(Constant::boxed(Value::Bool(true)));
        assert_eq!(and.print(), "Abs(Bool(true))");
    }
}
