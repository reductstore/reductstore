// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::proto::record::State;
use reduct_base::error::ReductError;

use crate::storage::query::filters::{RecordFilter, RecordMeta};

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

impl RecordFilter for RecordStateFilter {
    fn filter(&mut self, record: &RecordMeta) -> Result<bool, ReductError> {
        let result = record.state() == self.state as i32;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::proto::record::State;
    use crate::storage::proto::Record;
    use crate::storage::query::filters::tests::RecordWrapper;
    use rstest::*;

    #[rstest]
    fn test_record_state_filter() {
        let mut filter = RecordStateFilter::new(State::Finished);
        let record = Record {
            state: State::Finished as i32,
            ..Default::default()
        };

        let wrapper = RecordWrapper::from(record.clone());
        assert!(filter.filter(&wrapper).unwrap(), "Record should pass");
    }

    #[rstest]
    fn test_record_state_filter_no_records() {
        let mut filter = RecordStateFilter::new(State::Finished);
        let record = Record {
            state: State::Started as i32,
            ..Default::default()
        };

        let wrapper = RecordWrapper::from(record.clone());
        assert!(!filter.filter(&wrapper).unwrap(), "Record should not pass");
    }
}
