// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Boxed, BoxedNode, Context, Node};
use reduct_base::error::ReductError;

/// A node representing a logical XOR or ONE_OF operation.
pub(crate) struct OneOf {
    operands: Vec<BoxedNode>,
}

impl Node for OneOf {
    fn apply(&self, context: &Context) -> Result<Value, ReductError> {
        let mut count = 0;
        for operand in self.operands.iter() {
            let value = operand.apply(context)?;
            if value.as_bool()? {
                count += 1;
            }
        }

        Ok(Value::Bool(count == 1))
    }

    fn operands(&self) -> &Vec<BoxedNode> {
        &self.operands
    }

    fn print(&self) -> String {
        format!("OneOf({:?})", self.operands)
    }
}

impl Boxed for OneOf {
    fn boxed(operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        Ok(Box::new(Self::new(operands)))
    }
}

impl OneOf {
    pub fn new(operands: Vec<BoxedNode>) -> Self {
        Self { operands }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::query::condition::constant::Constant;
    use crate::storage::query::condition::value::Value;
    use rstest::rstest;

    #[rstest]
    fn apply() {
        let xor = OneOf::new(vec![
            Constant::boxed(Value::Bool(false)),
            Constant::boxed(Value::Int(1)),
            Constant::boxed(Value::Float(-2.0)),
            Constant::boxed(Value::String("xxxx".to_string())),
        ]);

        assert_eq!(xor.apply(&Context::default()).unwrap(), Value::Bool(false));

        let xor = OneOf::new(vec![
            Constant::boxed(Value::Bool(false)),
            Constant::boxed(Value::Bool(true)),
            Constant::boxed(Value::Bool(false)),
        ]);

        assert_eq!(xor.apply(&Context::default()).unwrap(), Value::Bool(true));
    }

    #[rstest]
    fn apply_empty() {
        let xor = OneOf::new(vec![]);
        assert_eq!(xor.apply(&Context::default()).unwrap(), Value::Bool(false));
    }

    #[rstest]
    fn print() {
        let xor = OneOf::new(vec![Constant::boxed(Value::Bool(false))]);
        assert_eq!(xor.print(), "OneOf([Bool(false)])");
    }
}
