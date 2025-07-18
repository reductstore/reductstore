// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::filters::{FilterRecord, RecordFilter};
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

impl<R: FilterRecord> RecordFilter<R> for TimeRangeFilter {
    fn filter(&mut self, record: R) -> Result<Option<Vec<R>>, ReductError> {
        let ts = record.timestamp();
        let ret = ts >= self.start && ts < self.stop;
        if ret {
            // Ensure that we don't return the same record twice
            self.start = ts + 1;
        }

        if ret {
            Ok(Some(vec![record]))
        } else {
            Ok(Some(vec![]))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::storage::query::filters::tests::TestFilterRecord;
    use reduct_base::io::RecordMeta;
    use rstest::*;

    #[rstest]
    fn test_time_range_filter() {
        let mut filter = TimeRangeFilter::new(0, 10);
        let record: TestFilterRecord = RecordMeta::builder().timestamp(5).build().into();
        assert_eq!(
            filter.filter(record.clone()).unwrap(),
            Some(vec![record.clone()]),
            "First time should pass"
        );
        assert_eq!(
            filter.filter(record.clone()).unwrap(),
            Some(vec![]),
            "Second time should not pass, as we have already returned the record"
        );
    }

    #[rstest]
    fn test_time_range_filter_no_records() {
        let mut filter = TimeRangeFilter::new(0, 10);
        let record: TestFilterRecord = RecordMeta::builder().timestamp(15).build().into();
        assert_eq!(
            filter.filter(record).unwrap(),
            Some(vec![]),
            "Record should not pass"
        );
    }

    #[rstest]
    fn test_time_include_start() {
        let mut filter = TimeRangeFilter::new(0, 10);

        let record: TestFilterRecord = RecordMeta::builder().timestamp(0).build().into();
        assert_eq!(
            filter.filter(record.clone()).unwrap(),
            Some(vec![record]),
            "Record should pass"
        );
    }

    #[rstest]
    fn test_time_exclude_end() {
        let mut filter = TimeRangeFilter::new(0, 10);

        let record: TestFilterRecord = RecordMeta::builder().timestamp(10).build().into();
        assert_eq!(
            filter.filter(record.clone()).unwrap(),
            Some(vec![]),
            "Record should not pass"
        );
    }
}
