// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Boxed, BoxedNode, Context, Node};
use reduct_base::error::ReductError;
use reduct_base::{not_found, unprocessable_entity};

/// A node representing a ref operation that accesses a label in the context.
pub(crate) struct Ref {
    op: BoxedNode,
}

impl Node for Ref {
    fn apply(&self, context: &Context) -> Result<Value, ReductError> {
        let label = self.op.apply(context)?.as_string()?;
        context.labels.get(label.as_str()).map_or_else(
            || Err(not_found!("Label '{:?}' not found", label)),
            |v| Ok(Value::parse(v)),
        )
    }

    fn print(&self) -> String {
        format!("Ref({:?})", self.op)
    }
}

impl Boxed for Ref {
    fn boxed(mut operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        if operands.len() != 1 {
            return Err(unprocessable_entity!("$ref requires exactly one operand"));
        }

        let op = operands.pop().unwrap();
        Ok(Box::new(Ref::new(op)))
    }
}

impl Ref {
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
        let op = Ref::new(Constant::boxed(Value::String("foo".to_string())));

        let mut context = Context::default();
        context.labels.insert("foo", "bar");
        assert_eq!(
            op.apply(&context).unwrap(),
            Value::String("bar".to_string())
        );
    }

    #[rstest]
    fn apply_empty() {
        let result = Ref::boxed(vec![]);
        assert_eq!(
            result.err().unwrap(),
            unprocessable_entity!("$ref requires exactly one operand")
        );
    }

    #[rstest]
    fn print() {
        let and = Ref::new(Constant::boxed(Value::String("foo".to_string())));
        assert_eq!(and.print(), "Ref(String(\"foo\"))");
    }
}
