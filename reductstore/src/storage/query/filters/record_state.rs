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

impl<R: Into<RecordMeta> + Clone> RecordFilter<R> for RecordStateFilter {
    fn filter(&mut self, record: R) -> Result<Option<Vec<R>>, ReductError> {
        let meta = record.clone().into();
        let result = meta.state() == self.state as i32;
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

    use rstest::*;

    #[rstest]
    fn test_record_state_filter() {
        let mut filter = RecordStateFilter::new(State::Finished);

        let meta = RecordMeta::builder().state(State::Finished as i32).build();
        assert!(filter.filter(&meta).unwrap(), "Record should pass");
    }

    #[rstest]
    fn test_record_state_filter_no_records() {
        let mut filter = RecordStateFilter::new(State::Finished);
        let meta = RecordMeta::builder().state(State::Started as i32).build();
        assert!(!filter.filter(&meta).unwrap(), "Record should not pass");
    }
}
