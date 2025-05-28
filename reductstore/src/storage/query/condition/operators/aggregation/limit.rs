// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Boxed, BoxedNode, Context, Node};
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::unprocessable_entity;

/// A node representing an aggregation operation limiting the number of records to process.
pub(crate) struct Limit {
    operands: Vec<BoxedNode>,
    count: i64,
}

impl Limit {
    pub fn new(operands: Vec<BoxedNode>) -> Self {
        Self { operands, count: 0 }
    }
}

impl Boxed for Limit {
    fn boxed(operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        if operands.len() != 1 {
            return Err(unprocessable_entity!("$limit requires exactly one operand"));
        }
        Ok(Box::new(Self::new(operands)))
    }
}

impl Node for Limit {
    fn apply(&mut self, context: &Context) -> Result<Value, ReductError> {
        self.count += 1;
        let value = self.operands[0].apply(context)?;
        if self.count > value.as_int()? {
            return Err(ReductError::new(ErrorCode::Interrupt, ""));
        }

        Ok(Value::Bool(true))
    }

    fn print(&self) -> String {
        format!("Limit({:?})", self.operands[0])
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
        let mut op = Limit::new(vec![Constant::boxed(Value::Int(2))]);
        assert_eq!(op.apply(&Context::default()).unwrap(), Value::Bool(true));
        assert_eq!(op.apply(&Context::default()).unwrap(), Value::Bool(true));
        assert_eq!(
            op.apply(&Context::default()).err().unwrap().status,
            ErrorCode::Interrupt
        );
    }

    #[rstest]
    fn apply_bad() {
        let mut op = Limit::new(vec![Constant::boxed(Value::String("foo".to_string()))]);
        assert_eq!(
            op.apply(&Context::default()).unwrap_err(),
            unprocessable_entity!("Value 'foo' could not be parsed as integer")
        );
    }

    #[rstest]
    fn apply_empty() {
        let result = Limit::boxed(vec![]);
        assert_eq!(
            result.err().unwrap(),
            unprocessable_entity!("$limit requires exactly one operand")
        );
    }

    #[rstest]
    fn print() {
        let and = Limit::new(vec![Constant::boxed(Value::Int(5))]);
        assert_eq!(and.print(), "Limit(Int(5))");
    }
}
