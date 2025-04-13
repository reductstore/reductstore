// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Boxed, BoxedNode, Context, Node};
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// A node representing an exists operation that checks if all labels in the operands exist in the context.
pub(crate) struct Exists {
    operands: Vec<BoxedNode>,
}

impl Node for Exists {
    fn apply(&self, context: &Context) -> Result<Value, ReductError> {
        for operand in &self.operands {
            let value = operand.apply(context)?;
            if !context.labels.contains_key(value.as_string()?.as_str()) {
                return Ok(Value::Bool(false));
            }
        }
        Ok(Value::Bool(true))
    }

    fn operands(&self) -> &Vec<BoxedNode> {
        &self.operands
    }

    fn print(&self) -> String {
        format!("Exists({:?})", self.operands)
    }
}

impl Boxed for Exists {
    fn boxed(operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        if operands.is_empty() {
            return Err(unprocessable_entity!(
                "$exists requires at least one operand"
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
    #[case(vec!["foo".to_string()], true)]
    #[case(vec!["foo".to_string(), "bazz".to_string()], false)]
    fn apply_ok(#[case] labels: Vec<String>, #[case] expected: bool) {
        let op = Exists::new(
            labels
                .iter()
                .map(|label| Constant::boxed(Value::String(label.clone())))
                .collect(),
        );

        let mut context = Context::default();
        context.labels.insert("foo", "bar");
        assert_eq!(op.apply(&context).unwrap(), Value::Bool(expected));
    }

    #[rstest]
    fn apply_empty() {
        let result = Exists::boxed(vec![]);
        assert_eq!(
            result.err().unwrap(),
            unprocessable_entity!("$exists requires at least one operand")
        );
    }

    #[rstest]
    fn print() {
        let and = Exists::new(vec![Constant::boxed(Value::Bool(true))]);
        assert_eq!(and.print(), "Exists([Bool(true)])");
    }
}
