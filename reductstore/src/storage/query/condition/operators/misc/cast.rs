// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::{Cast as CastTrait, Value};
use crate::storage::query::condition::{Boxed, BoxedNode, Context, Node};
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// A node representing a cast operation.
pub(crate) struct Cast {
    operands: Vec<BoxedNode>,
}

impl Node for Cast {
    fn apply(&mut self, context: &Context) -> Result<Value, ReductError> {
        let op = self.operands[0].apply(context)?;
        let type_name = self.operands[1].apply(context)?.as_string()?;
        op.cast(type_name.as_str())
    }

    fn print(&self) -> String {
        format!("Cast({:?}, {:?})", self.operands[0], self.operands[1])
    }
}

impl Boxed for Cast {
    fn boxed(operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        if operands.len() != 2 {
            return Err(unprocessable_entity!("$cast requires exactly two operands"));
        }
        Ok(Box::new(Cast::new(operands)))
    }
}

impl Cast {
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
        let mut sub = Cast::new(vec![
            Constant::boxed(Value::Int(1)),
            Constant::boxed(Value::String("float".to_string())),
        ]);
        assert_eq!(sub.apply(&Context::default()).unwrap(), Value::Float(1.0));
    }

    #[rstest]
    fn apply_bad() {
        let mut sub = Cast::new(vec![
            Constant::boxed(Value::Int(1)),
            Constant::boxed(Value::String("foo".to_string())),
        ]);
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
        let op = Cast::new(vec![
            Constant::boxed(Value::Bool(true)),
            Constant::boxed(Value::String("bool".to_string())),
        ]);
        assert_eq!(op.print(), "Cast(Bool(true), String(\"bool\"))");
    }
}
