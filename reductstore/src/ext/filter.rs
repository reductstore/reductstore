use crate::storage::proto::record::State::Finished;
use crate::storage::query::filters::{GetMeta, RecordFilter};
use reduct_base::error::ReductError;
use reduct_base::ext::BoxedReadRecord;
use std::collections::HashMap;

// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1
pub(super) struct DummyFilter {}

impl<R: GetMeta> RecordFilter<R> for DummyFilter {
    fn filter(&mut self, record: R) -> Result<Option<Vec<R>>, ReductError> {
        Ok(Some(vec![record]))
    }
}

impl DummyFilter {
    pub fn boxed() -> Box<dyn RecordFilter<BoxedReadRecord> + Send + Sync> {
        Box::new(DummyFilter {})
    }
}

impl GetMeta for BoxedReadRecord {
    fn state(&self) -> i32 {
        Finished as i32
    }

    fn timestamp(&self) -> u64 {
        self.meta().timestamp()
    }

    fn labels(&self) -> HashMap<&String, &String> {
        self.meta().labels().iter().map(|(k, v)| (k, v)).collect()
    }

    fn computed_labels(&self) -> HashMap<&String, &String> {
        self.meta()
            .computed_labels()
            .iter()
            .map(|(k, v)| (k, v))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ext::ext_repository::tests::{mocked_record, MockRecord};
    use crate::storage::query::condition::Parser;

    use rstest::rstest;
    use serde_json::json;

    #[rstest]
    fn pass_status_if_condition_none(mocked_record: Box<MockRecord>) {
        let mut filter = ExtWhenFilter::new(None, false);
        assert!(filter.filter_record(mocked_record).unwrap().is_ok())
    }

    #[rstest]
    fn not_ready_if_condition_false(mocked_record: Box<MockRecord>) {
        let mut filter = ExtWhenFilter::new(
            Some(
                Parser::new()
                    .parse(&json!({"$and": [false, "@key1"]}))
                    .unwrap(),
            ),
            true,
        );
        assert!(filter.filter_record(mocked_record).is_none())
    }

    #[rstest]
    fn ready_if_condition_true(mocked_record: Box<MockRecord>) {
        let mut filter = ExtWhenFilter::new(
            Some(
                Parser::new()
                    .parse(&json!({"$and": [true, "@key1"]}))
                    .unwrap(),
            ),
            true,
        );
        assert!(filter.filter_record(mocked_record).unwrap().is_ok())
    }

    #[rstest]
    fn ready_with_error_strict(mocked_record: Box<MockRecord>) {
        let mut filter = ExtWhenFilter::new(
            Some(
                Parser::new()
                    .parse(&json!({"$and": [true, "@not-exit"]}))
                    .unwrap(),
            ),
            true,
        );
        assert!(filter.filter_record(mocked_record).unwrap().is_err())
    }

    #[rstest]
    fn ready_without_error(mocked_record: Box<MockRecord>) {
        let mut filter = ExtWhenFilter::new(
            Some(
                Parser::new()
                    .parse(&json!({"$and": [true, "@not-exit"]}))
                    .unwrap(),
            ),
            false,
        );
        assert!(
            filter.filter_record(mocked_record).is_none(),
            "ignore bad condition"
        )
    }
}
