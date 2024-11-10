// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Context, Node};
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;
use std::fmt::Debug;

pub(crate) struct And {
    operands: Vec<Box<dyn Node>>,
    holder: Value,
}

impl Node for And {
    fn apply(&mut self, context: &Context) -> Result<&Value, ReductError> {
        self.holder = true.into();
        for operand in self.operands.iter_mut() {
            let value = operand.apply(context)?;
            if !*value
                .as_bool()
                .ok_or(unprocessable_entity!("Expected boolean value"))?
            {
                self.holder = false.into();
                break;
            }
        }

        Ok(&self.holder)
    }

    fn debug(&self) -> String {
        format!("And({:?})", self.operands)
    }
}

impl And {
    pub fn new(operands: Vec<Box<dyn Node>>) -> Self {
        And {
            operands,
            holder: Value::Bool(false),
        }
    }

    pub fn boxed(operands: Vec<Box<dyn Node>>) -> Box<Self> {
        Box::new(And::new(operands))
    }
}
