// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Contains as ContainsTrait;
use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Boxed, BoxedNode, Context, Node};
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

pub(crate) struct Contains {
    operands: Vec<BoxedNode>,
}

impl Node for Contains {
    fn apply(&mut self, context: &Context) -> Result<Value, ReductError> {
        let value_1 = self.operands[0].apply(context)?;
        let value_2 = self.operands[1].apply(context)?;
        Ok(Value::Bool(value_1.contains(value_2)?))
    }

    fn print(&self) -> String {
        format!("Contains({:?}, {:?})", self.operands[0], self.operands[1])
    }
}

impl Contains {
    pub fn new(operands: Vec<BoxedNode>) -> Self {
        Self { operands }
    }
}

impl Boxed for Contains {
    fn boxed(operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        if operands.len() != 2 {
            return Err(unprocessable_entity!(
                "$contains requires exactly two operands"
            ));
        }

        Ok(Box::new(Contains::new(operands)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::query::condition::constant::Constant;

    use rstest::rstest;

    #[rstest]
    #[case(Value::String("test".to_string()), Value::String("es".to_string()), Value::Bool(true))]
    #[case(Value::String("test".to_string()), Value::String("esx".to_string()), Value::Bool(false))]
    #[case(Value::String("test".to_string()), Value::String("test".to_string()), Value::Bool(true))]
    fn apply(#[case] op_1: Value, #[case] op_2: Value, #[case] expected: Value) {
        let mut contains = Contains::new(vec![Constant::boxed(op_1), Constant::boxed(op_2)]);
        assert_eq!(contains.apply(&Context::default()).unwrap(), expected);
    }

    #[rstest]
    fn only_two_operands() {
        let operands: Vec<BoxedNode> = vec![Constant::boxed(Value::String("test".to_string()))];
        assert_eq!(
            Contains::boxed(operands).err().unwrap(),
            unprocessable_entity!("$contains requires exactly two operands")
        );
    }

    #[rstest]
    fn print() {
        let op = Contains::new(vec![
            Constant::boxed(Value::String("test".to_string())),
            Constant::boxed(Value::String("es".to_string())),
        ]);
        assert_eq!(op.print(), "Contains(String(\"test\"), String(\"es\"))");
    }
}
