// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;

mod constant;
mod operators;
mod parser;
mod value;

struct Context {}

trait Node {
    fn apply(&mut self, context: &Context) -> &Value;
}
