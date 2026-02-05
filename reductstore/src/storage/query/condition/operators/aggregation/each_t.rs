// Copyright 2025-2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Boxed, BoxedNode, Context, Node};
use log::warn;
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// A node representing an aggregation operation that keeps a record for the given duration.
pub(crate) struct EachT {
    operands: Vec<BoxedNode>,
    last_timestamp: Option<u64>,
}

impl EachT {
    pub fn new(operands: Vec<BoxedNode>) -> Self {
        Self {
            operands,
            last_timestamp: None,
        }
    }
}

impl Boxed for EachT {
    fn boxed(operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        if operands.len() != 1 {
            return Err(unprocessable_entity!(
                "$each_t requires exactly one operand"
            ));
        }
        Ok(Box::new(Self::new(operands)))
    }
}

impl Node for EachT {
    fn apply(&mut self, context: &Context) -> Result<Value, ReductError> {
        if self.last_timestamp.is_none() {
            self.last_timestamp = Some(context.timestamp);
        }

        let last_time = self.last_timestamp.unwrap();
        let value = self.operands[0].apply(context)?;
        let s = if value.is_duration() {
            value.as_float()? / 1_000_000.0
        } else {
            value.as_float()?
        };

        if context.timestamp < last_time {
            warn!(
                "Time went backwards (from {} to {}), resetting $each_t",
                last_time, context.timestamp
            );
            self.last_timestamp = Some(context.timestamp);
            return Ok(Value::Bool(false));
        }

        let ret = context.timestamp - last_time >= (s * 1_000_000.0) as u64;
        if ret {
            self.last_timestamp = Some(context.timestamp);
        }

        Ok(Value::Bool(ret))
    }

    fn print(&self) -> String {
        format!("EachT({:?})", self.operands[0])
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
    #[case(Value::Float(0.1))]
    #[case(Value::Duration(100_000))]
    fn apply_ok(#[case] value: Value) {
        let mut op = EachT::new(vec![Constant::boxed(value)]);

        let mut context = Context::default();
        assert_eq!(op.apply(&context).unwrap(), Value::Bool(false));
        context.timestamp += 1;
        assert_eq!(op.apply(&context).unwrap(), Value::Bool(false));
        context.timestamp += 100_000;
        assert_eq!(op.apply(&context).unwrap(), Value::Bool(true));
        context.timestamp += 1;
        assert_eq!(op.apply(&context).unwrap(), Value::Bool(false));
    }

    #[rstest]
    fn apply_bad() {
        let mut op = EachT::new(vec![Constant::boxed(Value::String("foo".to_string()))]);
        assert_eq!(
            op.apply(&Context::default()).unwrap_err(),
            unprocessable_entity!("Value 'foo' could not be parsed as float")
        );
    }

    #[rstest]
    fn apply_zero() {
        let mut op = EachT::new(vec![Constant::boxed(Value::Int(0))]);
        assert_eq!(op.apply(&Context::default()).unwrap(), Value::Bool(true));
    }

    #[rstest]
    fn apply_empty() {
        let result = EachT::boxed(vec![]);
        assert_eq!(
            result.err().unwrap(),
            unprocessable_entity!("$each_t requires exactly one operand")
        );
    }

    #[rstest]
    fn print() {
        let and = EachT::new(vec![Constant::boxed(Value::Int(5))]);
        assert_eq!(and.print(), "EachT(Int(5))");
    }

    #[rstest]
    fn apply_time_goes_backwards_resets() {
        let mut op = EachT::new(vec![Constant::boxed(Value::Float(0.1))]);

        let mut context = Context::default();
        context.timestamp = 200_000;
        assert_eq!(op.apply(&context).unwrap(), Value::Bool(false));

        context.timestamp = 100_000;
        assert_eq!(op.apply(&context).unwrap(), Value::Bool(false));

        context.timestamp = 200_000;
        assert_eq!(op.apply(&context).unwrap(), Value::Bool(true));
    }
}
