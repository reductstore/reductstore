// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Context, Node};
use reduct_base::error::ReductError;
use reduct_base::{not_found, unprocessable_entity};

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

    fn debug(&self) -> String {
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
