// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Boxed, BoxedNode, Context, Node};
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// A node representing the timestamp of a current record in a query.
pub(crate) struct Timestamp {}

impl Node for Timestamp {
    fn apply(&mut self, context: &Context) -> Result<Value, ReductError> {
        Ok(Value::Int(context.timestamp as i64))
    }

    fn print(&self) -> String {
        "Timestamp()".to_string()
    }
}

impl Boxed for Timestamp {
    fn boxed(operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        if !operands.is_empty() {
            return Err(unprocessable_entity!("$timestamp requires no operands"));
        }

        Ok(Box::new(Timestamp::new()))
    }
}

impl Timestamp {
    pub fn new() -> Self {
        Self {}
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
    fn apply_ok() {
        let mut op = Timestamp::new();

        let mut context = Context::default();
        context.timestamp = 1234567890;
        assert_eq!(op.apply(&context).unwrap(), Value::Int(1234567890));
    }

    #[rstest]
    fn apply_not_empty() {
        let result = Timestamp::boxed(vec![Constant::boxed(Value::String("foo".to_string()))]);
        assert_eq!(
            result.err().unwrap(),
            unprocessable_entity!("$timestamp requires no operands")
        );
    }

    #[rstest]
    fn print() {
        let and = Timestamp::new();
        assert_eq!(and.print(), "Timestamp()".to_string());
    }
}
