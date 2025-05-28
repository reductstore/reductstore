// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::{BoxedNode, Context};
use reduct_base::error::ReductError;
use reduct_base::ext::BoxedReadRecord;
use std::collections::HashMap;

pub(super) struct ExtWhenFilter {
    condition: Option<BoxedNode>,
    strict: bool,
}

/// This filter is used to filter records based on a condition.
///
/// It is used in the `ext` module to filter records after they processed by extension,
/// and it puts computed labels into the context.
impl ExtWhenFilter {
    pub fn new(condition: Option<BoxedNode>, strict: bool) -> Self {
        ExtWhenFilter { condition, strict }
    }

    pub fn filter_record(
        &mut self,
        record: BoxedReadRecord,
    ) -> Option<Result<BoxedReadRecord, ReductError>> {
        if self.condition.is_none() {
            return Some(Ok(record));
        }

        // filter with computed labels
        match self.filter_with_computed(&record) {
            Ok(true) => Some(Ok(record)),
            Ok(false) => None,
            Err(e) => {
                if self.strict {
                    Some(Err(e))
                } else {
                    None
                }
            }
        }
    }

    fn filter_with_computed(&mut self, reader: &BoxedReadRecord) -> Result<bool, ReductError> {
        let meta = reader.meta();
        let labels = meta
            .labels()
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect::<HashMap<_, _>>();

        let computed_labels = meta
            .computed_labels()
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect::<HashMap<_, _>>();

        let context = Context::new(meta.timestamp(), labels, computed_labels);
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
