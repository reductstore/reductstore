// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use super::parse_timestamp_and_timezone;
use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Boxed, BoxedNode, Context, Node};
use chrono::Datelike;
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

pub(crate) struct Month {
    operands: Vec<BoxedNode>,
}

impl Node for Month {
    fn apply(&mut self, context: &Context) -> Result<Value, ReductError> {
        let (date_time, timezone) =
            parse_timestamp_and_timezone("$month", &mut self.operands, context)?;
        Ok(Value::Int(date_time.with_timezone(&timezone).month() as i64))
    }

    fn print(&self) -> String {
        format!("Month({:?})", self.operands)
    }
}

impl Boxed for Month {
    fn boxed(operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        if operands.len() != 1 && operands.len() != 2 {
            return Err(unprocessable_entity!("$month requires one or two operands"));
        }

        Ok(Box::new(Self::new(operands)))
    }
}

impl Month {
    pub fn new(operands: Vec<BoxedNode>) -> Self {
        Self { operands }
    }
}
