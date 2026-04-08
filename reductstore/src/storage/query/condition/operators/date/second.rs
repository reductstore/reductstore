// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use super::parse_timestamp_and_timezone;
use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Boxed, BoxedNode, Context, Node};
use chrono::Timelike;
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

pub(crate) struct Second {
    operands: Vec<BoxedNode>,
}

impl Node for Second {
    fn apply(&mut self, context: &Context) -> Result<Value, ReductError> {
        let (date_time, timezone) =
            parse_timestamp_and_timezone("$second", &mut self.operands, context)?;
        Ok(Value::Int(
            date_time.with_timezone(&timezone).second() as i64
        ))
    }

    fn print(&self) -> String {
        format!("Second({:?})", self.operands)
    }
}

impl Boxed for Second {
    fn boxed(operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        if operands.len() != 1 && operands.len() != 2 {
            return Err(unprocessable_entity!(
                "$second requires one or two operands"
            ));
        }

        Ok(Box::new(Self::new(operands)))
    }
}

impl Second {
    pub fn new(operands: Vec<BoxedNode>) -> Self {
        Self { operands }
    }
}
