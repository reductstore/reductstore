// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Context, Node};
use reduct_base::error::ReductError;
use reduct_base::{not_found, unprocessable_entity};

/// A node representing a reference to a label in the context.
pub(super) struct Reference {
    name: String,
    holder: Option<Value>,
}

impl Node for Reference {
    fn apply(&mut self, context: &Context) -> Result<&Value, ReductError> {
        let value = context
            .labels
            .get(&self.name)
            .ok_or(not_found!("Reference '{}' not found"))?;

        self.holder = Value::parse(value);
        if let Some(holder) = &self.holder {
            Ok(holder)
        } else {
            Err(unprocessable_entity!(
                "Reference '{}' with value '{}' could not be parsed",
                self.name,
                value
            ))
        }
    }

    fn print(&self) -> String {
        format!("Ref({})", self.name)
    }
}

impl Reference {
    pub fn new(name: String) -> Self {
        Reference { name, holder: None }
    }

    pub fn boxed(name: String) -> Box<Self> {
        Box::new(Reference::new(name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::query::condition::value::Value;

    #[test]
    fn apply() {
        let mut reference = Reference::new("label".to_string());
        let mut context = Context::default();
        context
            .labels
            .insert("label".to_string(), "true".to_string());
        let result = reference.apply(&context).unwrap();
        assert_eq!(result, &Value::Bool(true));
    }

    #[test]
    fn apply_not_found() {
        let mut reference = Reference::new("label".to_string());
        let context = Context::default();
        let result = reference.apply(&context);
        assert!(result.is_err());
    }

    #[test]
    fn apply_invalid_value() {
        let mut reference = Reference::new("label".to_string());
        let mut context = Context::default();
        context
            .labels
            .insert("label".to_string(), "invalid".to_string());
        let result = reference.apply(&context);
        assert!(result.is_err());
    }

    #[test]
    fn print() {
        let reference = Reference::new("label".to_string());
        let result = reference.print();
        assert_eq!(result, "Ref(label)");
    }
}
