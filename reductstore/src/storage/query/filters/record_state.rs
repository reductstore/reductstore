// Copyright 2023-2024 ReductStore
// Licensed under the Business Source License 1.1

use crate::storage::proto::record::State;
use crate::storage::proto::Record;
use crate::storage::query::filters::RecordFilter;

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
    /// A new `RecordStateFilter` instance
    pub fn new(state: State) -> RecordStateFilter {
        RecordStateFilter { state }
    }
}

impl RecordFilter for RecordStateFilter {
    fn filter(&mut self, record: &Record) -> bool {
        record.state == self.state as i32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::proto::record::State;
    use crate::storage::proto::Record;
    use rstest::*;

    #[rstest]
    fn test_record_state_filter() {
        let mut filter = RecordStateFilter::new(State::Finished);
        let record = Record {
            state: State::Finished as i32,
            ..Default::default()
        };

        assert!(filter.filter(&record), "Record should pass");
    }

    #[rstest]
    fn test_record_state_filter_no_records() {
        let mut filter = RecordStateFilter::new(State::Finished);
        let record = Record {
            state: State::Started as i32,
            ..Default::default()
        };

        assert!(!filter.filter(&record), "Record should not pass");
    }
}
