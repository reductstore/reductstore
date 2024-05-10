// Copyright 2023-2024 ReductStore
// Licensed under the Business Source License 1.1

use crate::storage::proto::Record;
use crate::storage::query::filters::RecordFilter;

pub struct EachNFilter {
    n: usize,
    count: usize,
}

impl EachNFilter {
    pub fn new(n: usize) -> EachNFilter {
        EachNFilter { n, count: 0 }
    }
}

impl RecordFilter for EachNFilter {
    fn filter(&self, _: &Record) -> bool {
        self.count % self.n == 0
    }

    fn last_sent(&mut self, _: &Record) -> () {
        self.count += 1;
    }
}
