// Copyright 2024-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{BoxedNode, Context, Node};
use reduct_base::error::ReductError;

/// A node representing a constant value.
pub(super) struct Constant {
    value: Value,
}

impl Node for Constant {
    fn apply(&mut self, _context: &Context) -> Result<Value, ReductError> {
        Ok(self.value.clone())
    }

    fn print(&self) -> String {
        format!("{:?}", self.value)
    }
}

impl Constant {
    pub fn new(value: Value) -> Self {
        Constant { value }
    }

    pub fn boxed(value: Value) -> BoxedNode {
        Box::new(Constant::new(value))
    }
}

impl From<bool> for Constant {
    fn from(value: bool) -> Self {
        Constant {
            value: Value::Bool(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::query::condition::value::Value;

    mod bool {
        use super::*;

        #[test]
        fn apply() {
            let mut constant = Constant::new(Value::Bool(true));
            let context = Context::default();
            let result = constant.apply(&context).unwrap();
            assert_eq!(result, Value::Bool(true));
        }

        #[test]
        fn print() {
            let constant = Constant::new(Value::Bool(true));
            let result = constant.print();
            assert_eq!(result, "Bool(true)");
        }

        #[test]
        fn from_bool() {
            let mut constant = Constant::from(true);
            let result = constant.apply(&Context::default()).unwrap();
            assert_eq!(result, Value::Bool(true));
        }
    }
}
