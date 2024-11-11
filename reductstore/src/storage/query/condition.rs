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

/// A context for evaluating conditions.
///
/// The context contains a set of labels that can be referenced by conditions.
#[derive(Debug, Default)]
struct Context {
    labels: HashMap<String, String>,
}

impl Context {
    pub fn new(labels: HashMap<String, String>) -> Self {
        Context { labels }
    }
}

/// A node in a condition tree.
///
/// Nodes can be evaluated in a context to produce a value.
trait Node {
    /// Evaluates the node in the given context.
    fn apply(&mut self, context: &Context) -> Result<&Value, ReductError>;

    /// Returns a string representation of the node.
    fn print(&self) -> String;
}

impl Debug for dyn Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.print())
    }
}
