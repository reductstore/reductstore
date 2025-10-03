// Copyright 2024-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

mod ctx_after;
mod ctx_before;
mod io_cfg;
mod select_labels;

use crate::cfg::io::IoConfig;
use crate::storage::query::condition::{BoxedNode, Context, Directives};
use crate::storage::query::filters::when::ctx_after::CtxAfter;
use crate::storage::query::filters::when::ctx_before::CtxBefore;
use crate::storage::query::filters::when::io_cfg::merge_io_config_from_directives;
use crate::storage::query::filters::when::select_labels::LabelSelector;
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

    label_selector: LabelSelector,
    io_config: IoConfig,
}

impl<R> WhenFilter<R> {
    pub fn try_new(
        condition: BoxedNode,
        mut directives: Directives,
        io_config: IoConfig,
        strict: bool,
    ) -> Result<Self, ReductError> {
        Ok(Self {
            condition,
            strict,
            ctx_before: CtxBefore::try_new(directives.remove("#ctx_before"))?,
            ctx_after: CtxAfter::try_new(directives.remove("#ctx_after"))?,
            label_selector: LabelSelector::try_new(directives.remove("#select_labels"))?,
            ctx_buffer: VecDeque::new(),
            io_config: merge_io_config_from_directives(&mut directives, io_config)?,
        })
    }

    pub fn io_config(&self) -> &IoConfig {
        &self.io_config
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
            let drained = self.ctx_buffer.drain(..);
            let filtered = drained
                .map(|record| self.label_selector.select_labels(record))
                .collect();
            Ok(Some(filtered))
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
    use serde_json::Value as JsonValue;

    #[rstest]
    fn filter(record_true: TestFilterRecord) {
        let mut filter = build_filter(json!({
            "$and": [true, "&label"]
        }));

        let result = filter.filter(record_true.clone()).unwrap();
        assert_eq!(result, Some(vec![record_true]));
    }

    mod context_n {
        use super::*;

        #[rstest]
        fn filter_ctx_before_n(record_true: TestFilterRecord, record_false: TestFilterRecord) {
            let mut filter = build_filter(json!({
                "#ctx_before": 2,
                "$and": [true, "&label"]
            }));

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
        fn filter_ctx_before_with_limit(
            record_true: TestFilterRecord,
            record_false: TestFilterRecord,
        ) {
            let mut filter = build_filter(json!({
                "#ctx_before": 2,
                "$and": [true, "&label"],
                "$limit": [1]
            }));

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
            assert_eq!(result, None);
        }

        #[rstest]
        fn filter_ctx_after_n(record_true: TestFilterRecord, record_false: TestFilterRecord) {
            let mut filter = build_filter(json!({
                "#ctx_after": 2,
                "$and": [true, "&label"]
            }));

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
            record_false_3: TestFilterRecord,
            record_false_4: TestFilterRecord,
            record_true_5: TestFilterRecord,
        ) {
            let mut filter = build_filter(json!({
                "#ctx_before": "2ms",
                "$and": [true, "&label"]
            }));

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
            record_true_5: TestFilterRecord,
            record_false_6: TestFilterRecord,
            record_false_7: TestFilterRecord,
        ) {
            let mut filter = build_filter(json!({
                "#ctx_after": "2ms",
                "$and": [true, "&label"]
            }));

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

    mod select_labels {
        use super::*;

        #[rstest]
        fn filter_with_select_labels(record_select_labels: TestFilterRecord) {
            let mut filter = build_filter(json!({
                "#select_labels": ["label1", "label3"],
                "$and": [true, "&label"]
            }));

            let result = filter.filter(record_select_labels).unwrap();
            assert!(result.is_some());
            let filtered_records = result.unwrap();
            assert_eq!(filtered_records.len(), 1);

            let record_labels = filtered_records[0].labels();
            assert_eq!(record_labels.len(), 2);
            assert!(record_labels.contains_key(&"label1".to_string()));
            assert!(record_labels.contains_key(&"label3".to_string()));

            assert!(!record_labels.contains_key(&"label".to_string()));
            assert!(!record_labels.contains_key(&"label2".to_string()));
        }

        #[rstest]
        fn filter_without_select_labels(record_select_labels: TestFilterRecord) {
            let mut filter = build_filter(json!({
                "$and": [true, "&label"]
            }));

            let result = filter.filter(record_select_labels).unwrap();
            assert!(result.is_some());
            let filtered_records = result.unwrap();
            assert_eq!(filtered_records.len(), 1);

            let record_labels = filtered_records[0].labels();
            assert_eq!(record_labels.len(), 4);
            assert!(record_labels.contains_key(&"label".to_string()));
            assert!(record_labels.contains_key(&"label1".to_string()));
            assert!(record_labels.contains_key(&"label2".to_string()));
            assert!(record_labels.contains_key(&"label3".to_string()));
        }

        #[fixture]
        fn record_select_labels() -> TestFilterRecord {
            RecordMeta::builder()
                .labels(HashMap::from_iter(vec![
                    ("label".to_string(), "true".to_string()),
                    ("label1".to_string(), "value1".to_string()),
                    ("label2".to_string(), "value2".to_string()),
                    ("label3".to_string(), "value3".to_string()),
                ]))
                .build()
                .into()
        }
    }

    fn build_filter(condition: JsonValue) -> WhenFilter<TestFilterRecord> {
        let parser = Parser::new();
        let (condition, directives) = parser.parse(condition).unwrap();

        let filter = WhenFilter::try_new(condition, directives, IoConfig::default(), true).unwrap();
        filter
    }

    #[fixture]
    fn record_true() -> TestFilterRecord {
        RecordMeta::builder()
            .labels(HashMap::from_iter(vec![("label", "true")]))
            .build()
            .into()
    }
}
