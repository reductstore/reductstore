// Copyright 2023-2024 ReductStore
// Licensed under the Business Source License 1.1

use crate::storage::proto::{ts_to_us, Record};
use crate::storage::query::filters::RecordFilter;

pub struct TimeRangeFilter {
    start: u64,
    stop: u64,
}

impl TimeRangeFilter {
    pub fn new(start: u64, stop: u64) -> TimeRangeFilter {
        TimeRangeFilter { start, stop }
    }
}

impl RecordFilter for TimeRangeFilter {
    fn filter(&mut self, record: &Record) -> bool {
        let ts = ts_to_us(record.timestamp.as_ref().unwrap());
        let ret = ts >= self.start && ts < self.stop;
        if ret {
            self.start = ts + 1;
        }

        ret
    }
}
