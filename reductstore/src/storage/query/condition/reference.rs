// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{BoxedNode, Context, EvaluationStage, Node};
use reduct_base::error::ReductError;
use reduct_base::not_found;

/// A node representing a reference to a label in the context.
pub(super) struct Reference {
    name: String,
    stage: EvaluationStage,
    operands: Vec<BoxedNode>,
}

impl Node for Reference {
    fn apply(&self, context: &Context) -> Result<Value, ReductError> {
        let label_value = context
            .labels
            .get(self.name.as_str())
            .ok_or(not_found!("Reference '{}' not found", self.name))?;

        let value = Value::parse(label_value);
        Ok(value)
    }

    fn operands(&self) -> &Vec<BoxedNode> {
        &self.operands
    }

    fn stage(&self) -> &EvaluationStage {
        &self.stage
    }

    fn print(&self) -> String {
        format!("Ref({})", self.name)
    }
}

impl Reference {
    pub fn new(name: String, stage: EvaluationStage) -> Self {
        Reference {
            name,
            stage,
            operands: vec![],
        }
    }

    pub fn boxed(name: String, stage: EvaluationStage) -> BoxedNode {
        Box::new(Reference::new(name, stage))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::query::condition::value::Value;

    #[test]
    fn apply() {
        let reference = Reference::new("label".to_string(), EvaluationStage::Retrieve);
        let mut context = Context::default();
        context.labels.insert("label", "true");
        let result = reference.apply(&context).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn apply_not_found() {
        let reference = Reference::new("label".to_string(), EvaluationStage::Retrieve);
        let context = Context::default();
        let result = reference.apply(&context);
        assert!(result
            .err()
            .unwrap()
            .to_string()
            .contains("Reference 'label' not found"));
    }

    #[test]
    fn print() {
        let reference = Reference::new("label".to_string(), EvaluationStage::Retrieve);
        let result = reference.print();
        assert_eq!(result, "Ref(label)");
    }
}
