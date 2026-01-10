// Copyright 2023-2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::proto::record::State;
use reduct_base::error::ReductError;

use crate::storage::query::filters::{FilterRecord, RecordFilter};

/// Filter that passes records with a specific state
pub struct RecordStateFilter {
    state: State,
}

impl RecordStateFilter {
    /// Create a new filter that passes records with the specified state
    ///
    /// # Arguments
    ///
    /// * `state` - The state to pass
    ///
    /// # Returns
    ///
    /// A new `RecordSta()teFilter` instance
    pub fn new(state: State) -> RecordStateFilter {
        RecordStateFilter { state }
    }
}

impl<R: FilterRecord> RecordFilter<R> for RecordStateFilter {
    fn filter(&mut self, record: R) -> Result<Option<Vec<R>>, ReductError> {
        let result = record.state() == self.state as i32;
        if result {
            Ok(Some(vec![record]))
        } else {
            Ok(Some(vec![]))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::proto::record::State;

    use crate::storage::query::filters::tests::TestFilterRecord;
    use reduct_base::io::RecordMeta;
    use rstest::*;

    #[rstest]
    fn test_record_state_filter() {
        let mut filter = RecordStateFilter::new(State::Finished);

        let record: TestFilterRecord = RecordMeta::builder()
            .state(State::Finished as i32)
            .build()
            .into();
        assert_eq!(
            filter.filter(record.clone()).unwrap(),
            Some(vec![record]),
            "Record should pass"
        );
    }

    #[rstest]
    fn test_record_state_filter_no_records() {
        let mut filter = RecordStateFilter::new(State::Finished);
        let record: TestFilterRecord = RecordMeta::builder()
            .state(State::Started as i32)
            .build()
            .into();
        assert_eq!(
            filter.filter(record).unwrap(),
            Some(vec![]),
            "Record should not pass"
        );
    }
}
