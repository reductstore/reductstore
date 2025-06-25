// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use reduct_base::error::ReductError;
use std::collections::HashMap;
use std::fmt::Debug;

mod computed_reference;
mod constant;
mod operators;
mod parser;
mod reference;
mod value;

/// A context for evaluating conditions.
///
/// The context contains a set of labels that can be referenced by conditions.
#[derive(Debug, Default)]
pub(crate) struct Context<'a> {
    timestamp: u64,
    labels: HashMap<&'a str, &'a str>,
    computed_labels: HashMap<&'a str, &'a str>,
}
impl<'a> Context<'a> {
    pub fn new(
        timestamp: u64,
        labels: HashMap<&'a str, &'a str>,
        computed_labels: HashMap<&'a str, &'a str>,
    ) -> Self {
        Context {
            timestamp,
            labels,
            computed_labels,
        }
    }
}

/// A node in a condition tree.
///
/// Nodes can be evaluated in a context to produce a value.
pub(crate) trait Node {
    /// Evaluates the node in the given context.
    fn apply(&mut self, context: &Context) -> Result<Value, ReductError>;

    /// Returns a string representation of the node.
    fn print(&self) -> String;
}

pub(crate) trait Boxed: Node {
    fn boxed(operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError>;
}

impl Debug for dyn Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.print())
    }
}

pub(crate) type BoxedNode = Box<dyn Node + Send + Sync>;

impl Debug for BoxedNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.print())
    }
}

pub(crate) use parser::Directives;
pub(crate) use parser::Parser;
