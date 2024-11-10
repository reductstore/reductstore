// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use reduct_base::error::ReductError;
use std::collections::HashMap;
use std::fmt::Debug;

mod constant;
mod operators;
mod parser;
mod reference;
mod value;

#[derive(Debug, Default)]
struct Context {
    labels: HashMap<String, String>,
}

impl Context {
    pub fn new(labels: HashMap<String, String>) -> Self {
        Context { labels }
    }
}

trait Node {
    fn apply(&mut self, context: &Context) -> Result<&Value, ReductError>;

    fn debug(&self) -> String;
}

impl Debug for dyn Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.debug())
    }
}
