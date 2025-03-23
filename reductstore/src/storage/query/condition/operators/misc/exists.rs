// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Boxed, BoxedNode, Context, Node};
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// A node representing an exists operation that checks if a label exists in the context.
pub(crate) struct Exists {
    operands: Vec<BoxedNode>,
}

impl Node for Exists {
    fn apply(&self, context: &Context) -> Result<Value, ReductError> {
        let value = self.operands[0].apply(context)?.as_string()?;
        Ok(Value::Bool(context.labels.contains_key(value.as_str())))
    }

    fn operands(&self) -> &Vec<BoxedNode> {
        &self.operands
    }

    fn print(&self) -> String {
        format!("Exists({:?})", self.operands[0])
    }
}

impl Boxed for Exists {
    fn boxed(mut operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        if operands.len() != 1 {
            return Err(unprocessable_entity!(
                "$exists requires exactly one operand"
            ));
        }

        Ok(Box::new(Exists::new(operands)))
    }
}

impl Exists {
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
        let op = Exists::new(vec![Constant::boxed(Value::String("foo".to_string()))]);
        assert_eq!(op.apply(&Context::default()).unwrap(), Value::Bool(false));

        let mut context = Context::default();
        context.labels.insert("foo", "bar");
        assert_eq!(op.apply(&context).unwrap(), Value::Bool(true));
    }

    #[rstest]
    fn apply_empty() {
        let result = Exists::boxed(vec![]);
        assert_eq!(
            result.err().unwrap(),
            unprocessable_entity!("$exists requires exactly one operand")
        );
    }

    #[rstest]
    fn print() {
        let and = Exists::new(vec![Constant::boxed(Value::Bool(true))]);
        assert_eq!(and.print(), "Exists(Bool(true))");
    }
}
