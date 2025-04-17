// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Boxed, BoxedNode, Context, Node};
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// A node representing an aggregation operation that keeps every n-th element.
pub(crate) struct EachN {
    operands: Vec<BoxedNode>,
    count: i64,
}

impl EachN {
    pub fn new(operands: Vec<BoxedNode>) -> Self {
        Self { operands, count: 0 }
    }
}

impl Boxed for EachN {
    fn boxed(operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        if operands.len() != 1 {
            return Err(unprocessable_entity!(
                "$each_n requires exactly one operand"
            ));
        }
        Ok(Box::new(Self::new(operands)))
    }
}

impl Node for EachN {
    fn apply(&mut self, context: &Context) -> Result<Value, ReductError> {
        self.count += 1;

        let value = self.operands[0].apply(context)?;
        let n = value.as_int()?;
        if n == 0 {
            return Err(unprocessable_entity!(
                "Value '0' is not a valid operand for $each_n"
            ));
        }

        if self.count % n == 0 {
            Ok(Value::Bool(true))
        } else {
            Ok(Value::Bool(false))
        }
    }

    fn operands(&self) -> &Vec<BoxedNode> {
        &self.operands
    }

    fn print(&self) -> String {
        format!("EachN({:?})", self.operands[0])
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
        let mut op = EachN::new(vec![Constant::boxed(Value::Int(2))]);
        assert_eq!(op.apply(&Context::default()).unwrap(), Value::Bool(false));
        assert_eq!(op.apply(&Context::default()).unwrap(), Value::Bool(true));
        assert_eq!(op.apply(&Context::default()).unwrap(), Value::Bool(false));
    }

    #[rstest]
    fn apply_bad() {
        let mut op = EachN::new(vec![Constant::boxed(Value::String("foo".to_string()))]);
        assert_eq!(
            op.apply(&Context::default()).unwrap_err(),
            unprocessable_entity!("Value 'foo' could not be parsed as integer")
        );
    }

    #[rstest]
    fn apply_zero() {
        let mut op = EachN::new(vec![Constant::boxed(Value::Int(0))]);
        assert_eq!(
            op.apply(&Context::default()).unwrap_err(),
            unprocessable_entity!("Value '0' is not a valid operand for $each_n")
        );
    }

    #[rstest]
    fn apply_empty() {
        let result = EachN::boxed(vec![]);
        assert_eq!(
            result.err().unwrap(),
            unprocessable_entity!("$each_n requires exactly one operand")
        );
    }

    #[rstest]
    fn print() {
        let and = EachN::new(vec![Constant::boxed(Value::Int(5))]);
        assert_eq!(and.print(), "EachN(Int(5))");
    }
}
