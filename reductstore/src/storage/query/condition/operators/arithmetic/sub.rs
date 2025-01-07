// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Boxed, BoxedNode, Context, Node};
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// A node representing an arithmetic subtraction operation.
pub(crate) struct Sub {
    operands: Vec<BoxedNode>,
}

impl Node for Sub {
    fn apply(&self, context: &Context) -> Result<Value, ReductError> {
        let mut first = None;

        for operand in self.operands.iter() {
            let value = operand.apply(context)?;
            match first {
                Some(Value::Bool(s)) => match value {
                    Value::Bool(v) => first = Some(Value::Int(s as i64 - v as i64)),
                    Value::Int(v) => first = Some(Value::Int(s as i64 - v)),
                    Value::Float(v) => first = Some(Value::Float(s as i8 as f64 - v)),
                    Value::String(_) => {
                        return Err(unprocessable_entity!("Cannot subtract string from boolean"));
                    }
                },

                Some(Value::Int(s)) => match value {
                    Value::Bool(v) => first = Some(Value::Int(s - v as i64)),
                    Value::Int(v) => first = Some(Value::Int(s - v)),
                    Value::Float(v) => {
                        first = Some(Value::Float(s as f64 - v));
                    }
                    Value::String(_) => {
                        return Err(unprocessable_entity!("Cannot subtract string from integer"));
                    }
                },

                Some(Value::Float(s)) => match value {
                    Value::Bool(v) => first = Some(Value::Float(s - v as i8 as f64)),
                    Value::Int(v) => first = Some(Value::Float(s - v as f64)),
                    Value::Float(v) => first = Some(Value::Float(s - v)),
                    Value::String(_) => {
                        return Err(unprocessable_entity!("Cannot subtract string from float"));
                    }
                },

                Some(Value::String(_)) => {
                    return Err(unprocessable_entity!("Cannot subtract string"))
                }

                None => first = Some(value),
            }
        }

        Ok(first.unwrap_or(Value::Int(0)))
    }

    fn print(&self) -> String {
        format!("Sub({:?})", self.operands)
    }
}

impl Boxed for Sub {
    fn boxed(operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        Ok(Box::new(Self::new(operands)))
    }
}

impl Sub {
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
    fn apply_bool() {
        let op = Sub::new(vec![
            Constant::boxed(Value::Bool(true)),
            Constant::boxed(Value::Bool(true)),
        ]);
        assert_eq!(op.apply(&Context::default()).unwrap(), Value::Int(0));

        let op = Sub::new(vec![
            Constant::boxed(Value::Bool(true)),
            Constant::boxed(Value::Int(2)),
        ]);
        assert_eq!(op.apply(&Context::default()).unwrap(), Value::Int(-1));

        let op = Sub::new(vec![
            Constant::boxed(Value::Bool(true)),
            Constant::boxed(Value::Float(2.0)),
        ]);
        assert_eq!(op.apply(&Context::default()).unwrap(), Value::Float(-1.0));

        let op = Sub::new(vec![
            Constant::boxed(Value::Bool(true)),
            Constant::boxed(Value::String("xxx".to_string())),
        ]);

        assert_eq!(
            op.apply(&Context::default()),
            Err(unprocessable_entity!("Cannot subtract string from boolean"))
        );
    }

    #[rstest]
    fn apply_int() {
        let op = Sub::new(vec![
            Constant::boxed(Value::Int(1)),
            Constant::boxed(Value::Bool(true)),
        ]);
        assert_eq!(op.apply(&Context::default()).unwrap(), Value::Int(0));

        let op = Sub::new(vec![
            Constant::boxed(Value::Int(1)),
            Constant::boxed(Value::Int(2)),
        ]);
        assert_eq!(op.apply(&Context::default()).unwrap(), Value::Int(-1));

        let op = Sub::new(vec![
            Constant::boxed(Value::Int(1)),
            Constant::boxed(Value::Float(2.0)),
        ]);
        assert_eq!(op.apply(&Context::default()).unwrap(), Value::Float(-1.0));

        let op = Sub::new(vec![
            Constant::boxed(Value::Int(1)),
            Constant::boxed(Value::String("xxx".to_string())),
        ]);

        assert_eq!(
            op.apply(&Context::default()),
            Err(unprocessable_entity!("Cannot subtract string from integer"))
        );
    }

    #[rstest]
    fn apply_float() {
        let op = Sub::new(vec![
            Constant::boxed(Value::Float(1.0)),
            Constant::boxed(Value::Bool(true)),
        ]);
        assert_eq!(op.apply(&Context::default()).unwrap(), Value::Float(0.0));

        let op = Sub::new(vec![
            Constant::boxed(Value::Float(1.0)),
            Constant::boxed(Value::Int(2)),
        ]);
        assert_eq!(op.apply(&Context::default()).unwrap(), Value::Float(-1.0));

        let op = Sub::new(vec![
            Constant::boxed(Value::Float(1.0)),
            Constant::boxed(Value::Float(2.0)),
        ]);
        assert_eq!(op.apply(&Context::default()).unwrap(), Value::Float(-1.0));

        let op = Sub::new(vec![
            Constant::boxed(Value::Float(1.0)),
            Constant::boxed(Value::String("xxx".to_string())),
        ]);

        assert_eq!(
            op.apply(&Context::default()),
            Err(unprocessable_entity!("Cannot subtract string from float"))
        );
    }

    #[rstest]
    fn apply_string() {
        let op = Sub::new(vec![
            Constant::boxed(Value::String("a".to_string())),
            Constant::boxed(Value::Bool(true)),
        ]);

        assert_eq!(
            op.apply(&Context::default()),
            Err(unprocessable_entity!("Cannot subtract string"))
        );

        let op = Sub::new(vec![
            Constant::boxed(Value::String("a".to_string())),
            Constant::boxed(Value::Int(1)),
        ]);

        assert_eq!(
            op.apply(&Context::default()),
            Err(unprocessable_entity!("Cannot subtract string"))
        );

        let op = Sub::new(vec![
            Constant::boxed(Value::String("a".to_string())),
            Constant::boxed(Value::Float(1.0)),
        ]);

        assert_eq!(
            op.apply(&Context::default()),
            Err(unprocessable_entity!("Cannot subtract string"))
        );

        let op = Sub::new(vec![
            Constant::boxed(Value::String("a".to_string())),
            Constant::boxed(Value::String("b".to_string())),
        ]);

        assert_eq!(
            op.apply(&Context::default()),
            Err(unprocessable_entity!("Cannot subtract string"))
        );
    }

    #[rstest]
    fn apply_empty() {
        let result = Sub::new(vec![]).apply(&Context::default()).unwrap();
        assert_eq!(result, Value::Int(0));
    }

    #[rstest]
    fn print() {
        let and = Sub::new(vec![Constant::boxed(Value::Bool(true))]);
        assert_eq!(and.print(), "Sub([Bool(true)])");
    }
}
