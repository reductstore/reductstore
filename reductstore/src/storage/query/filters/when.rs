// Copyright 2024-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

mod ctx_after;
mod ctx_before;

use crate::storage::query::condition::{BoxedNode, Context, Directives};
use crate::storage::query::filters::when::ctx_after::CtxAfter;
use crate::storage::query::filters::when::ctx_before::CtxBefore;
use crate::storage::query::filters::when::Padding::{Duration, Records};
use crate::storage::query::filters::{FilterRecord, RecordFilter};
use reduct_base::error::{ErrorCode, ReductError};
use std::collections::VecDeque;

pub(super) enum Padding {
    Records(usize),
    Duration(u64),
}

/// A node representing a when filter with a condition.
pub struct WhenFilter<R> {
    condition: BoxedNode,
    strict: bool,

    ctx_before: CtxBefore,
    ctx_after: CtxAfter,
    ctx_buffer: VecDeque<R>,
}

impl<R> WhenFilter<R> {
    pub fn try_new(
        condition: BoxedNode,
        directives: Directives,
        strict: bool,
    ) -> Result<Self, ReductError> {
        let before = if let Some(before) = directives.get("#ctx_before") {
            let val = before.as_int()?;
            if val < 0 {
                return Err(ReductError::unprocessable_entity(
                    "#ctx_before must be non-negative",
                ));
            }

            if before.is_duration() {
                Duration(val as u64)
            } else {
                Records(val as usize)
            }
        } else {
            Records(0) // Default to 0 records before
        };

        let after = if let Some(after) = directives.get("#ctx_after") {
            let val = after.as_int()?;
            if val < 0 {
                return Err(ReductError::unprocessable_entity(
                    "#ctx_after must be non-negative",
                ));
            }
            if after.is_duration() {
                Duration(val as u64)
            } else {
                Records(val as usize)
            }
        } else {
            Records(0) // Default to 0 records after
        };

        Ok(Self {
            condition,
            strict,
            ctx_before: CtxBefore::new(before),
            ctx_after: CtxAfter::new(after),
            ctx_buffer: VecDeque::new(),
        })
    }
}

impl<R: FilterRecord> RecordFilter<R> for WhenFilter<R> {
    fn filter(&mut self, record: R) -> Result<Option<Vec<R>>, ReductError> {
        self.ctx_before.queue_record(&mut self.ctx_buffer, record);

        let record = self.ctx_buffer.back().unwrap();

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

        if self.ctx_after.check(result, record.timestamp()) {
            Ok(Some(self.ctx_buffer.drain(..).collect()))
        } else {
            Ok(Some(vec![]))
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

    mod context_n {
        use super::*;

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

        #[rstest]
        fn filter_ctx_before_negative(parser: Parser) {
            let (condition, directives) = parser
                .parse(json!({
                    "#ctx_before": -2,
                    "$and": [true, "&label"]
                }))
                .unwrap();

            let err = WhenFilter::<TestFilterRecord>::try_new(condition, directives, true)
                .err()
                .unwrap();
            assert_eq!(
                err,
                ReductError::unprocessable_entity("#ctx_before must be non-negative")
            );
        }

        #[rstest]
        fn filter_ctx_after_negative(parser: Parser) {
            let (condition, directives) = parser
                .parse(json!({
                    "#ctx_after": -2,
                    "$and": [true, "&label"]
                }))
                .unwrap();

            let err = WhenFilter::<TestFilterRecord>::try_new(condition, directives, true)
                .err()
                .unwrap();
            assert_eq!(
                err,
                ReductError::unprocessable_entity("#ctx_after must be non-negative")
            );
        }

        #[fixture]
        fn record_false() -> TestFilterRecord {
            RecordMeta::builder()
                .labels(HashMap::from_iter(vec![("label", "false")]))
                .build()
                .into()
        }
    }

    mod context_dur {
        use super::*;

        #[rstest]
        fn filter_ctx_before_duration(
            parser: Parser,
            record_false_3: TestFilterRecord,
            record_false_4: TestFilterRecord,
            record_true_5: TestFilterRecord,
        ) {
            let (condition, directives) = parser
                .parse(json!({
                    "#ctx_before": "2ms",
                    "$and": [true, "&label"]
                }))
                .unwrap();

            let mut filter = WhenFilter::try_new(condition, directives, true).unwrap();
            let result = filter.filter(record_false_3.clone()).unwrap();
            assert_eq!(result, Some(vec![]));

            let result = filter.filter(record_false_4.clone()).unwrap();
            assert_eq!(result, Some(vec![]));

            let result = filter.filter(record_true_5.clone()).unwrap();
            assert_eq!(
                result,
                Some(vec![record_false_3, record_false_4, record_true_5])
            );
        }

        #[rstest]
        fn filter_ctx_after_duration(
            parser: Parser,
            record_true_5: TestFilterRecord,
            record_false_6: TestFilterRecord,
            record_false_7: TestFilterRecord,
        ) {
            let (condition, directives) = parser
                .parse(json!({
                    "#ctx_after": "2ms",
                    "$and": [true, "&label"]
                }))
                .unwrap();

            let mut filter = WhenFilter::try_new(condition, directives, true).unwrap();

            let result = filter.filter(record_true_5.clone()).unwrap();
            assert_eq!(result, Some(vec![record_true_5]));

            let result = filter.filter(record_false_6.clone()).unwrap();
            assert_eq!(result, Some(vec![record_false_6]));

            let result = filter.filter(record_false_7.clone()).unwrap();
            assert_eq!(result, Some(vec![record_false_7]));
        }

        #[fixture]
        fn record_false_3() -> TestFilterRecord {
            RecordMeta::builder()
                .timestamp(3000)
                .labels(HashMap::from_iter(vec![("label", "false")]))
                .build()
                .into()
        }

        #[fixture]
        fn record_false_7() -> TestFilterRecord {
            RecordMeta::builder()
                .timestamp(7000)
                .labels(HashMap::from_iter(vec![("label", "false")]))
                .build()
                .into()
        }

        #[fixture]
        fn record_false_6() -> TestFilterRecord {
            RecordMeta::builder()
                .timestamp(6000)
                .labels(HashMap::from_iter(vec![("label", "false")]))
                .build()
                .into()
        }

        #[fixture]
        fn record_true_5() -> TestFilterRecord {
            RecordMeta::builder()
                .timestamp(5000)
                .labels(HashMap::from_iter(vec![("label", "true")]))
                .build()
                .into()
        }

        #[fixture]
        fn record_false_4() -> TestFilterRecord {
            RecordMeta::builder()
                .timestamp(4000)
                .labels(HashMap::from_iter(vec![("label", "false")]))
                .build()
                .into()
        }
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
}
