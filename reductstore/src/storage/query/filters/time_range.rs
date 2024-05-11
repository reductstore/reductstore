// Copyright 2023-2024 ReductStore
// Licensed under the Business Source License 1.1

use crate::storage::proto::{ts_to_us, Record};
use crate::storage::query::filters::RecordFilter;

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
    fn filter(&mut self, record: &Record) -> bool {
        let ts = ts_to_us(record.timestamp.as_ref().unwrap());
        let ret = ts >= self.start && ts < self.stop;
        if ret {
            // Ensure that we don't return the same record twice
            self.start = ts + 1;
        }

        ret
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::proto::us_to_ts;
    use rstest::*;

    #[rstest]
    fn test_time_range_filter() {
        let mut filter = TimeRangeFilter::new(0, 10);
        let record = Record {
            timestamp: Some(us_to_ts(&5)),
            ..Default::default()
        };

        assert!(filter.filter(&record), "First time should pass");
        assert!(
            !filter.filter(&record),
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

        assert!(!filter.filter(&record), "Record should not pass");
    }

    #[rstest]
    fn test_time_include_start() {
        let mut filter = TimeRangeFilter::new(0, 10);
        let record = Record {
            timestamp: Some(us_to_ts(&0)),
            ..Default::default()
        };

        assert!(filter.filter(&record), "Record should pass");
    }

    #[rstest]
    fn test_time_exclude_end() {
        let mut filter = TimeRangeFilter::new(0, 10);
        let record = Record {
            timestamp: Some(us_to_ts(&10)),
            ..Default::default()
        };

        assert!(!filter.filter(&record), "Record should not pass");
    }
}
