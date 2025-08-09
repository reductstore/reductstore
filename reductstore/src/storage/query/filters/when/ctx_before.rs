// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::Value;
use crate::storage::query::filters::when::Padding;
use crate::storage::query::filters::when::Padding::{Duration, Records};
use crate::storage::query::filters::FilterRecord;
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;
use std::collections::VecDeque;

/// Context for managing records before a condition is checked in a `when` filter.
pub(super) struct CtxBefore {
    before: Padding,
}

impl CtxBefore {
    pub fn try_new(directive: Option<Vec<Value>>) -> Result<Self, ReductError> {
        let before = match directive {
            Some(before) => {
                if before.len() != 1 {
                    return Err(unprocessable_entity!("#ctx_before must be a single value"));
                }

                let before_val = before.first().unwrap();
                let val = before_val.as_int().map_err(|e| {
                    unprocessable_entity!(
                        "#ctx_before must be an integer or duration: {}",
                        e.message
                    )
                })?;
                if val < 0 {
                    return Err(unprocessable_entity!("#ctx_before must be non-negative",));
                }

                if before_val.is_duration() {
                    Duration(val as u64)
                } else {
                    Records(val as usize)
                }
            }

            None => {
                // Default to 0 records before if no padding is specified
                Records(0)
            }
        };

        Ok(CtxBefore { before })
    }

    /// Queues a record into the context buffer, managing the size based on the `before` padding.
    ///
    /// Note: we need to keep the buffer outside of the filter to allow use reference to the first record
    ///
    /// # Arguments
    ///
    /// * `ctx_buffer` - A mutable reference to the buffer where records are stored.
    /// * `record` - The record to be queued.
    pub(crate) fn queue_record<R>(&self, ctx_buffer: &mut VecDeque<R>, record: R)
    where
        R: FilterRecord,
    {
        ctx_buffer.push_back(record);
        match self.before {
            Records(n) => {
                if ctx_buffer.len() > n + 1 {
                    ctx_buffer.pop_front();
                }
            }
            Duration(us) => {
                let mut first_record_ts = ctx_buffer.front().unwrap().timestamp();
                let last_record_ts = ctx_buffer.back().unwrap().timestamp();
                while last_record_ts - first_record_ts > us {
                    ctx_buffer.pop_front().unwrap();
                    first_record_ts = ctx_buffer.front().map_or(0, |r| r.timestamp());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::query::filters::tests::TestFilterRecord;
    use crate::storage::query::filters::when::ctx_after::CtxAfter;
    use reduct_base::io::RecordMeta;
    use rstest::*;

    #[rstest]
    fn test_ctx_before_records() {
        let ctx = CtxBefore::try_new(Some(vec![Value::Int(2)])).unwrap();
        let mut buffer = VecDeque::new();
        let record: TestFilterRecord = RecordMeta::builder().build().into();

        ctx.queue_record(&mut buffer, record.clone());
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.front(), Some(&record));

        ctx.queue_record(&mut buffer, record.clone());
        assert_eq!(buffer.len(), 2);

        ctx.queue_record(&mut buffer, record.clone());
        assert_eq!(buffer.len(), 3);

        ctx.queue_record(&mut buffer, record.clone());
        assert_eq!(buffer.len(), 3, "Should not exceed 3 records");
    }

    #[rstest]
    fn test_ctx_before_duration() {
        let ctx = CtxBefore::try_new(Some(vec![Value::Duration(5000)])).unwrap();
        let mut buffer = VecDeque::new();
        let record1: TestFilterRecord = RecordMeta::builder().timestamp(1000).build().into();
        let record2: TestFilterRecord = RecordMeta::builder().timestamp(6000).build().into();
        let record3: TestFilterRecord = RecordMeta::builder().timestamp(6001).build().into();

        ctx.queue_record(&mut buffer, record1.clone());
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.front(), Some(&record1));

        ctx.queue_record(&mut buffer, record2.clone());
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.front(), Some(&record1));

        ctx.queue_record(&mut buffer, record3.clone());
        assert_eq!(
            buffer.len(),
            2,
            "Should remove the first record after 5000ms"
        );
        assert_eq!(
            buffer.front(),
            Some(&record2),
            "Should keep the second record"
        );
    }

    #[test]
    fn test_ctx_after_invalid() {
        let result = CtxBefore::try_new(Some(vec![Value::Int(-1)]));
        assert_eq!(
            result.err().unwrap(),
            unprocessable_entity!("#ctx_before must be non-negative")
        );

        let result = CtxBefore::try_new(Some(vec![Value::String("invalid".to_string())]));
        assert_eq!(result.err().unwrap(), unprocessable_entity!("#ctx_before must be an integer or duration: Value 'invalid' could not be parsed as integer"));

        let result = CtxBefore::try_new(Some(vec![Value::Int(1), Value::Int(2)]));
        assert_eq!(
            result.err().unwrap(),
            unprocessable_entity!("#ctx_before must be a single value")
        );

        let result = CtxBefore::try_new(Some(vec![]));
        assert_eq!(
            result.err().unwrap(),
            unprocessable_entity!("#ctx_before must be a single value")
        );
    }
}
