// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{BoxedNode, Context, Node};
use reduct_base::error::ReductError;
use reduct_base::not_found;

/// A node representing a reference to a label in the context.
pub(super) struct ComputedReference {
    name: String,
    operands: Vec<BoxedNode>,
}

impl Node for ComputedReference {
    fn apply(&mut self, context: &Context) -> Result<Value, ReductError> {
        let label_value = context
            .computed_labels
            .get(self.name.as_str())
            .ok_or(not_found!("Reference '@{}' not found", self.name))?;

        let value = Value::parse(label_value);
        Ok(value)
    }

    fn operands(&self) -> &Vec<BoxedNode> {
        &self.operands
    }
    fn print(&self) -> String {
        format!("CompRef({})", self.name)
    }
}

impl ComputedReference {
    pub fn new(name: String) -> Self {
        ComputedReference {
            name,
            operands: vec![],
        }
    }

    pub fn boxed(name: String) -> BoxedNode {
        Box::new(ComputedReference::new(name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::query::condition::value::Value;
    use rstest::rstest;

    #[test]
    fn apply() {
        let mut reference = ComputedReference::new("label".to_string());
        let mut context = Context::default();
        context.computed_labels.insert("label", "true");
        let result = reference.apply(&context).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn apply_not_found() {
        let mut reference = ComputedReference::new("label".to_string());
        let context = Context::default();
        let result = reference.apply(&context);
        assert!(result
            .err()
            .unwrap()
            .to_string()
            .contains("Reference '@label' not found"));
    }

    #[test]
    fn print() {
        let reference = ComputedReference::new("label".to_string());
        let result = reference.print();
        assert_eq!(result, "CompRef(label)");
    }

    #[test]
    fn operands() {
        let reference = ComputedReference::new("label".to_string());
        let result = reference.operands();
        assert_eq!(result.len(), 0);
    }
}
