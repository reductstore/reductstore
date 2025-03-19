// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::filters::{RecordFilter, RecordMeta};
use reduct_base::error::ReductError;

/// Filter that passes records with a timestamp within a specific range
pub struct TimeRangeFilter {
    start: u64,
    stop: u64,
}

impl TimeRangeFilter {
    /// Create a new filter that passes records with a timestamp within the specified range
    ///
    /// # Arguments
    ///
    /// * `start` - The start of the range (inclusive)
    /// * `stop` - The end of the range (exclusive)
    ///
    /// # Returns
    ///
    /// A new `TimeRangeFilter` instance
    pub fn new(start: u64, stop: u64) -> TimeRangeFilter {
        TimeRangeFilter { start, stop }
    }
}

impl RecordFilter for TimeRangeFilter {
    fn filter(&mut self, record: &dyn RecordMeta) -> Result<bool, ReductError> {
        let ts = record.timestamp() as u64;
        let ret = ts >= self.start && ts < self.stop;
        if ret {
            // Ensure that we don't return the same record twice
            self.start = ts + 1;
        }

        Ok(ret)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::proto::{us_to_ts, Record};
    use rstest::*;

    #[rstest]
    fn test_time_range_filter() {
        let mut filter = TimeRangeFilter::new(0, 10);
        let record = Record {
            timestamp: Some(us_to_ts(&5)),
            ..Default::default()
        };

        assert!(filter.filter(&record).unwrap(), "First time should pass");
        assert!(
            !filter.filter(&record).unwrap(),
            "Second time should not pass, as we have already returned the record"
        );
    }

    #[rstest]
    fn test_time_range_filter_no_records() {
        let mut filter = TimeRangeFilter::new(0, 10);
        let record = Record {
            timestamp: Some(us_to_ts(&15)),
            ..Default::default()
        };

        assert!(!filter.filter(&record).unwrap(), "Record should not pass");
    }

    #[rstest]
    fn test_time_include_start() {
        let mut filter = TimeRangeFilter::new(0, 10);
        let record = Record {
            timestamp: Some(us_to_ts(&0)),
            ..Default::default()
        };

        assert!(filter.filter(&record).unwrap(), "Record should pass");
    }

    #[rstest]
    fn test_time_exclude_end() {
        let mut filter = TimeRangeFilter::new(0, 10);
        let record = Record {
            timestamp: Some(us_to_ts(&10)),
            ..Default::default()
        };

        assert!(!filter.filter(&record).unwrap(), "Record should not pass");
    }
}
