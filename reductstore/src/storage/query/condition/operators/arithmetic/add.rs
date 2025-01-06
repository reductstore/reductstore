// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Boxed, BoxedNode, Context, Node};
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// A node representing an arithmetic addition operation.
pub(crate) struct Add {
    operands: Vec<BoxedNode>,
}

impl Node for Add {
    fn apply(&self, context: &Context) -> Result<Value, ReductError> {
        let mut sum = None;

        for operand in self.operands.iter() {
            let value = operand.apply(context)?;
            match sum {
                Some(Value::Bool(s)) => match value {
                    Value::Bool(v) => sum = Some(Value::Int(s as i64 + v as i64)),
                    Value::Int(v) => sum = Some(Value::Int(s as i64 + v)),
                    Value::Float(v) => sum = Some(Value::Float(s as i8 as f64 + v)),
                    Value::String(_) => {
                        return Err(unprocessable_entity!("Cannot add string to boolean"));
                    }
                },

                Some(Value::Int(s)) => match value {
                    Value::Bool(v) => sum = Some(Value::Int(s + v as i64)),
                    Value::Int(v) => sum = Some(Value::Int(s + v)),
                    Value::Float(v) => {
                        sum = Some(Value::Float(s as f64 + v));
                    }
                    Value::String(v) => {
                        return Err(unprocessable_entity!("Cannot add string to integer"));
                    }
                },

                Some(Value::Float(s)) => match value {
                    Value::Bool(v) => sum = Some(Value::Float(s + v as i8 as f64)),
                    Value::Int(v) => sum = Some(Value::Float(s + v as f64)),
                    Value::Float(v) => sum = Some(Value::Float(s + v)),
                    Value::String(_) => {
                        return Err(unprocessable_entity!("Cannot add string to float"));
                    }
                },

                Some(Value::String(s)) => match value {
                    Value::Bool(v) => {
                        return Err(unprocessable_entity!("Cannot add boolean to string"));
                    }
                    Value::Int(v) => {
                        return Err(unprocessable_entity!("Cannot add integer to string"));
                    }

                    Value::Float(v) => {
                        return Err(unprocessable_entity!("Cannot add float to string"));
                    }
                    Value::String(v) => sum = Some(Value::String(format!("{}{}", s, v))),
                },

                None => sum = Some(value),
            }
        }

        Ok(sum.unwrap_or(Value::Int(0)))
    }

    fn print(&self) -> String {
        format!("Add({:?})", self.operands)
    }
}

impl Boxed for Add {
    fn boxed(operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        Ok(Box::new(Self::new(operands)))
    }
}

impl Add {
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
        let add = Add::new(vec![
            Constant::boxed(Value::Bool(true)),
            Constant::boxed(Value::Bool(false)),
        ]);
        assert_eq!(add.apply(&Context::default()).unwrap(), Value::Int(1));

        let add = Add::new(vec![
            Constant::boxed(Value::Bool(true)),
            Constant::boxed(Value::Int(2)),
        ]);
        assert_eq!(add.apply(&Context::default()).unwrap(), Value::Int(3));

        let add = Add::new(vec![
            Constant::boxed(Value::Bool(true)),
            Constant::boxed(Value::Float(2.0)),
        ]);
        assert_eq!(add.apply(&Context::default()).unwrap(), Value::Float(3.0));

        let add = Add::new(vec![
            Constant::boxed(Value::Bool(true)),
            Constant::boxed(Value::String("xxx".to_string())),
        ]);

        assert_eq!(
            add.apply(&Context::default()),
            Err(unprocessable_entity!("Cannot add string to boolean"))
        );
    }

    #[rstest]
    fn apply_int() {
        let add = Add::new(vec![
            Constant::boxed(Value::Int(1)),
            Constant::boxed(Value::Bool(true)),
        ]);
        assert_eq!(add.apply(&Context::default()).unwrap(), Value::Int(2));

        let add = Add::new(vec![
            Constant::boxed(Value::Int(1)),
            Constant::boxed(Value::Int(2)),
        ]);
        assert_eq!(add.apply(&Context::default()).unwrap(), Value::Int(3));

        let add = Add::new(vec![
            Constant::boxed(Value::Int(1)),
            Constant::boxed(Value::Float(2.0)),
        ]);
        assert_eq!(add.apply(&Context::default()).unwrap(), Value::Float(3.0));

        let add = Add::new(vec![
            Constant::boxed(Value::Int(1)),
            Constant::boxed(Value::String("xxx".to_string())),
        ]);

        assert_eq!(
            add.apply(&Context::default()),
            Err(unprocessable_entity!("Cannot add string to integer"))
        );
    }

    #[rstest]
    fn apply_float() {
        let add = Add::new(vec![
            Constant::boxed(Value::Float(1.0)),
            Constant::boxed(Value::Bool(true)),
        ]);
        assert_eq!(add.apply(&Context::default()).unwrap(), Value::Float(2.0));

        let add = Add::new(vec![
            Constant::boxed(Value::Float(1.0)),
            Constant::boxed(Value::Int(2)),
        ]);
        assert_eq!(add.apply(&Context::default()).unwrap(), Value::Float(3.0));

        let add = Add::new(vec![
            Constant::boxed(Value::Float(1.0)),
            Constant::boxed(Value::Float(2.0)),
        ]);
        assert_eq!(add.apply(&Context::default()).unwrap(), Value::Float(3.0));

        let add = Add::new(vec![
            Constant::boxed(Value::Float(1.0)),
            Constant::boxed(Value::String("xxx".to_string())),
        ]);

        assert_eq!(
            add.apply(&Context::default()),
            Err(unprocessable_entity!("Cannot add string to float"))
        );
    }

    #[rstest]
    fn apply_string() {
        let add = Add::new(vec![
            Constant::boxed(Value::String("a".to_string())),
            Constant::boxed(Value::Bool(true)),
        ]);

        assert_eq!(
            add.apply(&Context::default()),
            Err(unprocessable_entity!("Cannot add boolean to string"))
        );

        let add = Add::new(vec![
            Constant::boxed(Value::String("a".to_string())),
            Constant::boxed(Value::Int(2)),
        ]);

        assert_eq!(
            add.apply(&Context::default()),
            Err(unprocessable_entity!("Cannot add integer to string"))
        );

        let add = Add::new(vec![
            Constant::boxed(Value::String("a".to_string())),
            Constant::boxed(Value::Float(2.0)),
        ]);

        assert_eq!(
            add.apply(&Context::default()),
            Err(unprocessable_entity!("Cannot add float to string"))
        );

        let add = Add::new(vec![
            Constant::boxed(Value::String("a".to_string())),
            Constant::boxed(Value::String("b".to_string())),
        ]);

        assert_eq!(
            add.apply(&Context::default()).unwrap(),
            Value::String("ab".to_string())
        );
    }

    #[rstest]
    fn apply_empty() {
        let result = Add::new(vec![]).apply(&Context::default()).unwrap();
        assert_eq!(result, Value::Int(0));
    }

    #[rstest]
    fn print() {
        let and = Add::new(vec![Constant::boxed(Value::Bool(true))]);
        assert_eq!(and.print(), "Add([Bool(true)])");
    }
}
