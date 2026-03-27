// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Boxed, BoxedNode, Context, Node};
use log::warn;
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// A node representing an edge-triggered gate.
///
/// It opens on a false->true edge of input condition for a given duration and mirrors
/// the input while open. After timeout, output is forced to false until input resets to false
/// and triggers again.
pub(crate) struct Gate {
    operands: Vec<BoxedNode>,
    /// Absolute timestamp (in microseconds) when the current gate window closes.
    /// `None` means there is no active gate window.
    gate_deadline: Option<u64>,
    /// Last seen input value, used for false->true edge detection.
    previous_input: bool,
    /// Latch set after timeout. While true, output is forced to false until input
    /// is observed as false once (reset phase), preventing immediate retrigger if
    /// the input remains true.
    reset_required: bool,
    /// Last timestamp used to detect non-monotonic time and reset internal state.
    last_timestamp: Option<u64>,
}

impl Gate {
    pub fn new(operands: Vec<BoxedNode>) -> Self {
        Self {
            operands,
            gate_deadline: None,
            previous_input: false,
            reset_required: false,
            last_timestamp: None,
        }
    }

    /// Convert configured gate duration to microseconds.
    ///
    /// Accepts either a parsed duration literal (`"10s"` -> `Value::Duration`) or
    /// a float/integer in seconds. Negative values are clamped to 0.
    fn duration_us(value: &Value) -> Result<u64, ReductError> {
        let duration = if value.is_duration() {
            value.as_int()?
        } else {
            (value.as_float()? * 1_000_000.0) as i64
        };

        Ok(std::cmp::max(0, duration) as u64)
    }

    /// Drop all runtime state (used when time goes backwards).
    fn reset_state(&mut self) {
        self.gate_deadline = None;
        self.previous_input = false;
        self.reset_required = false;
    }
}

impl Boxed for Gate {
    fn boxed(operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        if operands.len() != 2 {
            return Err(unprocessable_entity!("$gate requires exactly two operands"));
        }

        Ok(Box::new(Self::new(operands)))
    }
}

impl Node for Gate {
    fn apply(&mut self, context: &Context) -> Result<Value, ReductError> {
        if let Some(last_timestamp) = self.last_timestamp {
            if context.timestamp < last_timestamp {
                warn!(
                    "Time went backwards (from {} to {}), resetting $gate",
                    last_timestamp, context.timestamp
                );
                self.reset_state();
            }
        }
        self.last_timestamp = Some(context.timestamp);

        let duration = Self::duration_us(&self.operands[0].apply(context)?)?;
        let input = self.operands[1].apply(context)?.as_bool()?;

        // 1) Expire active window and arm reset latch.
        if let Some(deadline) = self.gate_deadline {
            if context.timestamp >= deadline {
                self.gate_deadline = None;
                self.reset_required = true;
            }
        }

        // 2) After timeout, force false until input is seen false once.
        //    This guarantees a full reset cycle before the next trigger.
        if self.reset_required {
            if !input {
                self.reset_required = false;
            }
            self.previous_input = input;
            return Ok(Value::Bool(false));
        }

        // 3) Open a new window only on a rising edge.
        if self.gate_deadline.is_none() && !self.previous_input && input {
            self.gate_deadline = Some(context.timestamp.saturating_add(duration));
        }

        // 4) While the gate is open, mirror input; otherwise force false.
        let output = match self.gate_deadline {
            Some(deadline) if context.timestamp < deadline => input,
            _ => false,
        };

        self.previous_input = input;
        Ok(Value::Bool(output))
    }

    fn print(&self) -> String {
        format!("Gate({:?}, {:?})", self.operands[0], self.operands[1])
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
    fn apply_ok_with_duration_literal() {
        let mut op = Gate::new(vec![
            Constant::boxed(Value::Duration(10_000_000)),
            Constant::boxed(Value::Bool(true)),
        ]);

        let mut context = Context::default();
        context.timestamp = 100;
        assert_eq!(op.apply(&context).unwrap(), Value::Bool(true));

        context.timestamp += 9_999_899;
        assert_eq!(op.apply(&context).unwrap(), Value::Bool(true));

        context.timestamp += 101;
        assert_eq!(op.apply(&context).unwrap(), Value::Bool(false));

        context.timestamp += 1;
        assert_eq!(op.apply(&context).unwrap(), Value::Bool(false));
    }

    #[rstest]
    fn apply_ok_with_float_seconds() {
        let mut op = Gate::new(vec![
            Constant::boxed(Value::Float(0.1)),
            Constant::boxed(Value::Bool(true)),
        ]);

        let mut context = Context::default();
        assert_eq!(op.apply(&context).unwrap(), Value::Bool(true));

        context.timestamp += 99_999;
        assert_eq!(op.apply(&context).unwrap(), Value::Bool(true));

        context.timestamp += 1;
        assert_eq!(op.apply(&context).unwrap(), Value::Bool(false));
    }

    #[rstest]
    fn apply_requires_reset_after_timeout() {
        let mut op = Gate::new(vec![
            Constant::boxed(Value::Duration(1)),
            Constant::boxed(Value::Bool(true)),
        ]);

        let mut context = Context::default();
        assert_eq!(op.apply(&context).unwrap(), Value::Bool(true));

        context.timestamp += 1;
        assert_eq!(op.apply(&context).unwrap(), Value::Bool(false));

        context.timestamp += 1;
        assert_eq!(op.apply(&context).unwrap(), Value::Bool(false));
    }

    #[rstest]
    fn apply_reset_cycle() {
        let mut op = Gate::new(vec![
            Constant::boxed(Value::Duration(1)),
            Constant::boxed(Value::Bool(true)),
        ]);

        let mut context = Context::default();
        assert_eq!(op.apply(&context).unwrap(), Value::Bool(true));

        context.timestamp += 1;
        assert_eq!(op.apply(&context).unwrap(), Value::Bool(false));

        context.timestamp += 1;
        assert_eq!(op.apply(&context).unwrap(), Value::Bool(false));

        op.operands[1] = Constant::boxed(Value::Bool(false));
        context.timestamp += 1;
        assert_eq!(op.apply(&context).unwrap(), Value::Bool(false));

        op.operands[1] = Constant::boxed(Value::Bool(true));
        context.timestamp += 1;
        assert_eq!(op.apply(&context).unwrap(), Value::Bool(true));
    }

    #[rstest]
    fn apply_time_goes_backwards_resets() {
        let mut op = Gate::new(vec![
            Constant::boxed(Value::Duration(100_000)),
            Constant::boxed(Value::Bool(true)),
        ]);

        let mut context = Context::default();
        context.timestamp = 200_000;
        assert_eq!(op.apply(&context).unwrap(), Value::Bool(true));

        context.timestamp = 100_000;
        assert_eq!(op.apply(&context).unwrap(), Value::Bool(true));
    }

    #[rstest]
    fn apply_bad() {
        let mut op = Gate::new(vec![
            Constant::boxed(Value::String("foo".to_string())),
            Constant::boxed(Value::Bool(true)),
        ]);
        assert_eq!(
            op.apply(&Context::default()).unwrap_err(),
            unprocessable_entity!("Value 'foo' could not be parsed as float")
        );
    }

    #[rstest]
    fn apply_empty() {
        let result = Gate::boxed(vec![]);
        assert_eq!(
            result.err().unwrap(),
            unprocessable_entity!("$gate requires exactly two operands")
        );
    }

    #[rstest]
    fn print() {
        let gate = Gate::new(vec![
            Constant::boxed(Value::Duration(100_000)),
            Constant::boxed(Value::Bool(true)),
        ]);
        assert_eq!(gate.print(), "Gate(Duration(100000), Bool(true))");
    }
}
