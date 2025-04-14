// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::{BoxedNode, Context, EvaluationStage};
use reduct_base::conflict;
use reduct_base::error::ReductError;
use reduct_base::ext::{BoxedReadRecord, ProcessStatus};
use std::collections::HashMap;

pub(super) struct ExtWhenFilter {
    condition: Option<BoxedNode>,
}

/// This filter is used to filter records based on a condition.
///
/// It is used in the `ext` module to filter records after they processed by extension,
/// and it puts computed labels into the context.
impl ExtWhenFilter {
    pub fn new(condition: Option<BoxedNode>) -> Self {
        ExtWhenFilter { condition }
    }

    pub fn filter_record(&mut self, status: ProcessStatus, strict: bool) -> ProcessStatus {
        if self.condition.is_none() {
            return status;
        }

        // filter with computed labels
        if let ProcessStatus::Ready(record) = &status {
            match self.filter_with_computed(&record.as_ref().unwrap()) {
                Ok(true) => status,
                Ok(false) => ProcessStatus::NotReady,
                Err(e) => {
                    if strict {
                        ProcessStatus::Ready(Err(e))
                    } else {
                        status
                    }
                }
            }
        } else {
            status
        }
    }

    fn filter_with_computed(&mut self, reader: &BoxedReadRecord) -> Result<bool, ReductError> {
        let mut labels = reader
            .labels()
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect::<HashMap<_, _>>();

        for (k, v) in reader.computed_labels() {
            if labels.insert(k, v).is_some() {
                return Err(conflict!("Computed label '@{}' already exists", k));
            }
        }

        let context = Context::new(labels, EvaluationStage::Compute);
        Ok(self
            .condition
            .as_mut()
            .unwrap()
            .apply(&context)?
            .as_bool()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ext::ext_repository::tests::{mocked_record, MockRecord};
    use crate::storage::query::condition::Parser;
    use assert_matches::assert_matches;
    use rstest::rstest;
    use serde_json::json;

    #[rstest]
    fn pass_status_if_condition_none(mocked_record: Box<MockRecord>) {
        let mut filter = ExtWhenFilter::new(None);
        let status = ProcessStatus::Ready(Ok(mocked_record));
        assert_matches!(
            filter.filter_record(status, false),
            ProcessStatus::Ready(Ok(_))
        )
    }

    #[rstest]
    fn not_ready_if_condition_false(mocked_record: Box<MockRecord>) {
        let mut filter = ExtWhenFilter::new(Some(
            Parser::new()
                .parse(&json!({"$and": [false, "@key1"]}))
                .unwrap(),
        ));
        let status = ProcessStatus::Ready(Ok(mocked_record));
        assert_matches!(filter.filter_record(status, true), ProcessStatus::NotReady)
    }

    #[rstest]
    fn ready_if_condition_true(mocked_record: Box<MockRecord>) {
        let mut filter = ExtWhenFilter::new(Some(
            Parser::new()
                .parse(&json!({"$and": [true, "@key1"]}))
                .unwrap(),
        ));
        let status = ProcessStatus::Ready(Ok(mocked_record));
        assert_matches!(filter.filter_record(status, true), ProcessStatus::Ready(_))
    }

    #[rstest]
    fn ready_with_error_strict(mocked_record: Box<MockRecord>) {
        let mut filter = ExtWhenFilter::new(Some(
            Parser::new()
                .parse(&json!({"$and": [true, "@not-exit"]}))
                .unwrap(),
        ));
        let status = ProcessStatus::Ready(Ok(mocked_record));
        assert_matches!(
            filter.filter_record(status, true),
            ProcessStatus::Ready(Err(_))
        )
    }

    #[rstest]
    fn ready_without_error(mocked_record: Box<MockRecord>) {
        let mut filter = ExtWhenFilter::new(Some(
            Parser::new()
                .parse(&json!({"$and": [true, "@not-exit"]}))
                .unwrap(),
        ));
        let status = ProcessStatus::Ready(Ok(mocked_record));
        assert_matches!(filter.filter_record(status, false), ProcessStatus::Ready(_))
    }

    #[rstest]
    fn conflict(mut mocked_record: Box<MockRecord>) {
        let mut filter = ExtWhenFilter::new(Some(
            Parser::new()
                .parse(&json!({"$and": [true, "@key1"]}))
                .unwrap(),
        ));

        mocked_record
            .labels_mut()
            .insert("key1".to_string(), "value1".to_string()); // conflicts with computed key1
        let status = ProcessStatus::Ready(Ok(mocked_record));
        let ProcessStatus::Ready(result) = filter.filter_record(status, true) else {
            panic!("Expected ProcessStatus::Ready");
        };

        assert_eq!(
            result.err().unwrap(),
            conflict!("Computed label '@key1' already exists")
        )
    }
}
