// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::filters::when::Padding;
use crate::storage::query::filters::when::Padding::{Duration, Records};

/// Context for managing records after a condition is checked in a `when` filter.
pub(super) struct CtxAfter {
    after: Padding,
    count: i64,
    last_ts: Option<u64>,
}

impl CtxAfter {
    pub fn new(after: Padding) -> Self {
        CtxAfter {
            after,
            count: 0,
            last_ts: None,
        }
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
        let mut ctx = CtxAfter::new(Records(3));
        assert!(ctx.check(true, 1000));
        assert!(ctx.check(false, 2000));
        assert!(ctx.check(false, 3000));
        assert!(ctx.check(false, 4000));
        assert!(!ctx.check(false, 5000)); // Should be false after 3 records
    }

    #[test]
    fn test_ctx_after_duration() {
        let mut ctx = CtxAfter::new(Duration(5000));
        assert!(ctx.check(true, 1000));
        assert!(ctx.check(false, 2000));
        assert!(ctx.check(false, 6000)); // Should still be true due to duration
        assert!(!ctx.check(false, 7000)); // Should be false after duration expires
    }
}
