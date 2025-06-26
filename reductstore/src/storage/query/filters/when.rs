// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::{BoxedNode, Context, Directives};
use crate::storage::query::filters::when::Padding::Records;
use crate::storage::query::filters::{FilterRecord, RecordFilter};
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::internal_server_error;
use std::collections::VecDeque;

enum Padding {
    Records(usize),
}

/// A node representing a when filter with a condition.
pub struct WhenFilter<R> {
    condition: BoxedNode,
    strict: bool,
    before: Padding,
    after: Padding,

    ctx_buffer: VecDeque<R>,
    ctx_after_count: i64,
}

impl<R> WhenFilter<R> {
    pub fn try_new(
        condition: BoxedNode,
        directives: Directives,
        strict: bool,
    ) -> Result<Self, ReductError> {
        let before = if let Some(before) = directives.get("#ctx_before") {
            let before = before.as_int()?;
            if before < 0 {
                return Err(ReductError::unprocessable_entity(
                    "Padding before must be non-negative",
                ));
            }
            Records(before as usize)
        } else {
            Records(0) // Default to 0 records before
        };

        let after = if let Some(after) = directives.get("#ctx_after") {
            let after = after.as_int()?;
            if after < 0 {
                return Err(ReductError::unprocessable_entity(
                    "Padding after must be non-negative",
                ));
            }
            Records(after as usize)
        } else {
            Records(0) // Default to 0 records after
        };

        Ok(Self {
            condition,
            strict,
            before,
            after,
            ctx_buffer: VecDeque::new(),
            ctx_after_count: 0,
        })
    }
}

impl<R: FilterRecord> RecordFilter<R> for WhenFilter<R> {
    fn filter(&mut self, record: R) -> Result<Option<Vec<R>>, ReductError> {
        // Put the record into the context buffer
        self.ctx_buffer.push_back(record);
        match self.before {
            Padding::Records(n) => {
                if self.ctx_buffer.len() > n + 1 {
                    self.ctx_buffer.pop_front();
                }
            }
        }

        let record = self.ctx_buffer.back().ok_or(internal_server_error!(
            "Buffer is empty, but we expected at least one record to be present."
        ))?;

        // Prepare the context for the condition evaluation
        let context = Context::new(
            record.timestamp(),
            record
                .labels()
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect(),
            record
                .computed_labels()
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect(),
        );

        let result = match self.condition.apply(&context) {
            Ok(value) => value.as_bool()?,
            Err(err) => {
                if err.status == ErrorCode::Interrupt {
                    return Ok(None);
                }

                if self.strict {
                    // in strict mode, we return an error if a filter fails
                    return Err(err);
                }

                false
            }
        };

        if self.check_after_ctx(result) {
            Ok(Some(self.ctx_buffer.drain(..).collect()))
        } else {
            Ok(Some(vec![]))
        }
    }
}

impl<R: FilterRecord> WhenFilter<R> {
    fn check_after_ctx(&mut self, condition: bool) -> bool {
        match self.after {
            Padding::Records(n) => {
                self.ctx_after_count -= 1;
                if condition {
                    self.ctx_after_count = n as i64;
                }
                self.ctx_after_count >= 0
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use crate::storage::query::condition::Parser;
    use crate::storage::query::filters::tests::TestFilterRecord;
    use reduct_base::io::RecordMeta;
    use rstest::{fixture, rstest};
    use serde_json::json;

    #[rstest]
    fn filter(parser: Parser, record_true: TestFilterRecord) {
        let (condition, directives) = parser
            .parse(json!({
                "$and": [true, "&label"]
            }))
            .unwrap();

        let mut filter = WhenFilter::try_new(condition, directives, true).unwrap();

        let result = filter.filter(record_true.clone()).unwrap();
        assert_eq!(result, Some(vec![record_true]));
    }

    #[rstest]
    fn filter_ctx_before_n(
        parser: Parser,
        record_true: TestFilterRecord,
        record_false: TestFilterRecord,
    ) {
        let (condition, directives) = parser
            .parse(json!({
                "#ctx_before": 2,
                "$and": [true, "&label"]
            }))
            .unwrap();

        let mut filter = WhenFilter::try_new(condition, directives, true).unwrap();

        let result = filter.filter(record_false.clone()).unwrap();
        assert_eq!(result, Some(vec![]));

        let result = filter.filter(record_false.clone()).unwrap();
        assert_eq!(result, Some(vec![]));

        let result = filter.filter(record_false.clone()).unwrap();
        assert_eq!(result, Some(vec![]));

        let result = filter.filter(record_true.clone()).unwrap();
        assert_eq!(
            result,
            Some(vec![
                record_false.clone(),
                record_false,
                record_true.clone()
            ])
        );

        let result = filter.filter(record_true.clone()).unwrap();
        assert_eq!(result, Some(vec![record_true]));
    }

    #[rstest]
    fn filter_ctx_after_n(
        parser: Parser,
        record_true: TestFilterRecord,
        record_false: TestFilterRecord,
    ) {
        let (condition, directives) = parser
            .parse(json!({
                "#ctx_after": 2,
                "$and": [true, "&label"]
            }))
            .unwrap();

        let mut filter = WhenFilter::try_new(condition, directives, true).unwrap();

        let result = filter.filter(record_true.clone()).unwrap();
        assert_eq!(result, Some(vec![record_true.clone()]));

        let result = filter.filter(record_false.clone()).unwrap();
        assert_eq!(result, Some(vec![record_false.clone()]));

        let result = filter.filter(record_false.clone()).unwrap();
        assert_eq!(result, Some(vec![record_false]));

        let result = filter.filter(record_true.clone()).unwrap();
        assert_eq!(result, Some(vec![record_true]));
    }

    #[fixture]
    fn parser() -> Parser {
        Parser::new()
    }

    #[fixture]
    fn record_true() -> TestFilterRecord {
        RecordMeta::builder()
            .labels(HashMap::from_iter(vec![("label", "true")]))
            .build()
            .into()
    }

    #[fixture]
    fn record_false() -> TestFilterRecord {
        RecordMeta::builder()
            .labels(HashMap::from_iter(vec![("label", "false")]))
            .build()
            .into()
    }
}
