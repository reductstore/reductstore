// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::Value;
use crate::storage::query::filters::when::Padding;
use crate::storage::query::filters::when::Padding::{Duration, Records};
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// Context for managing records after a condition is checked in a `when` filter.
pub(super) struct CtxAfter {
    after: Padding,
    count: i64,
    last_ts: Option<u64>,
}

impl CtxAfter {
    pub fn try_new(directive: Option<Vec<Value>>) -> Result<Self, ReductError> {
        let after = match directive {
            Some(after) => {
                if after.len() != 1 {
                    return Err(unprocessable_entity!("#ctx_after must be a single value"));
                }
                let after_val = after.first().unwrap();

                let val = after_val.as_int().map_err(|e| {
                    unprocessable_entity!(
                        "#ctx_after must be an integer or duration: {}",
                        e.message
                    )
                })?;

                if val < 0 {
                    return Err(unprocessable_entity!("#ctx_after must be non-negative",));
                }
                if after_val.is_duration() {
                    Duration(val as u64)
                } else {
                    Records(val as usize)
                }
            }
            None => Records(0), // Default to 0 records after
        };

        Ok(CtxAfter {
            after,
            count: 0,
            last_ts: None,
        })
    }

    /// Checks the condition against the context, updating the state based on the padding type.
    pub(crate) fn check(&mut self, condition: bool, time: u64) -> bool {
        match self.after {
            Records(n) => {
                self.count -= 1;
                if condition {
                    self.count = n as i64;
                }
                self.count >= 0
            }
            Duration(us) => {
                if condition {
                    self.last_ts = Some(time);
                }

                self.last_ts.is_some_and(|last_ts| last_ts + us >= time)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ctx_after_records() {
        let mut ctx = CtxAfter::try_new(Some(vec![Value::Int(3)])).unwrap();
        assert!(ctx.check(true, 1000));
        assert!(ctx.check(false, 2000));
        assert!(ctx.check(false, 3000));
        assert!(ctx.check(false, 4000));
        assert!(!ctx.check(false, 5000)); // Should be false after 3 records
    }

    #[test]
    fn test_ctx_after_duration() {
        let mut ctx = CtxAfter::try_new(Some(vec![Value::Duration(5000)])).unwrap();
        assert!(ctx.check(true, 1000));
        assert!(ctx.check(false, 2000));
        assert!(ctx.check(false, 6000)); // Should still be true due to duration
        assert!(!ctx.check(false, 7000)); // Should be false after duration expires
    }

    #[test]
    fn test_ctx_after_invalid() {
        let result = CtxAfter::try_new(Some(vec![Value::Int(-1)]));
        assert_eq!(
            result.err().unwrap(),
            unprocessable_entity!("#ctx_after must be non-negative")
        );

        let result = CtxAfter::try_new(Some(vec![Value::String("invalid".to_string())]));
        assert_eq!(result.err().unwrap(), unprocessable_entity!("#ctx_after must be an integer or duration: Value 'invalid' could not be parsed as integer"));

        let result = CtxAfter::try_new(Some(vec![Value::Int(1), Value::Int(2)]));
        assert_eq!(
            result.err().unwrap(),
            unprocessable_entity!("#ctx_after must be a single value")
        );

        let result = CtxAfter::try_new(Some(vec![]));
        assert_eq!(
            result.err().unwrap(),
            unprocessable_entity!("#ctx_after must be a single value")
        );
    }
}
