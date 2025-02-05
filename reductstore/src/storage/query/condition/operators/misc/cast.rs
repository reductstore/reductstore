// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::{Cast as CastTrait, Value};
use crate::storage::query::condition::{Boxed, BoxedNode, Context, Node};
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// A node representing a cast operation.
pub(crate) struct Cast {
    op_1: BoxedNode,
    op_2: BoxedNode,
}

impl Node for Cast {
    fn apply(&self, context: &Context) -> Result<Value, ReductError> {
        let op = self.op_1.apply(context)?;
        let type_name = self.op_2.apply(context)?.as_string()?;
        op.cast(type_name.as_str())
    }

    fn print(&self) -> String {
        format!("Cast({:?}, {:?})", self.op_1, self.op_2)
    }
}

impl Boxed for Cast {
    fn boxed(mut operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        if operands.len() != 2 {
            return Err(unprocessable_entity!("$cast requires exactly two operands"));
        }
        let op_2 = operands.pop().unwrap();
        let op_1 = operands.pop().unwrap();
        Ok(Box::new(Cast::new(op_1, op_2)))
    }
}

impl Cast {
    pub fn new(op_1: BoxedNode, op_2: BoxedNode) -> Self {
        Self { op_1, op_2 }
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
        let sub = Cast::new(
            Constant::boxed(Value::Int(1)),
            Constant::boxed(Value::String("float".to_string())),
        );
        assert_eq!(sub.apply(&Context::default()).unwrap(), Value::Float(1.0));
    }

    #[rstest]
    fn apply_bad() {
        let sub = Cast::new(
            Constant::boxed(Value::Int(1)),
            Constant::boxed(Value::String("foo".to_string())),
        );
        assert_eq!(
            sub.apply(&Context::default()),
            Err(unprocessable_entity!("Unknown type 'foo'"))
        );
    }

    #[rstest]
    fn apply_empty() {
        let op = Cast::boxed(vec![]);
        assert_eq!(
            op.err(),
            Some(unprocessable_entity!("$cast requires exactly two operands"))
        );
    }

    #[rstest]
    fn print() {
        let op = Cast::new(
            Constant::boxed(Value::Bool(true)),
            Constant::boxed(Value::String("bool".to_string())),
        );
        assert_eq!(op.print(), "Cast(Bool(true), String(\"bool\"))");
    }
}
