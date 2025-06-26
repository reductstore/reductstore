use crate::storage::proto::record::State::Finished;
use crate::storage::query::filters::{FilterRecord, RecordFilter};
use reduct_base::error::ReductError;
use reduct_base::ext::BoxedReadRecord;
use std::collections::HashMap;

// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1
pub(super) struct DummyFilter {}

impl<R: FilterRecord> RecordFilter<R> for DummyFilter {
    fn filter(&mut self, record: R) -> Result<Option<Vec<R>>, ReductError> {
        Ok(Some(vec![record]))
    }
}

impl DummyFilter {
    pub fn boxed() -> Box<dyn RecordFilter<BoxedReadRecord> + Send + Sync> {
        Box::new(DummyFilter {})
    }
}

impl FilterRecord for BoxedReadRecord {
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
    use crate::ext::ext_repository::tests::mocked_record;

    use rstest::rstest;

    #[rstest]
    fn test_dummy_filter(mocked_record: BoxedReadRecord) {
        let mut filter = DummyFilter::boxed();
        let result = filter.filter(mocked_record).unwrap();
        assert_eq!(
            result.unwrap().len(),
            1,
            "Dummy filter should pass all records"
        );
    }
}
