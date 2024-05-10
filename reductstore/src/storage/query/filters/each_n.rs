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
    fn filter(&mut self, _: &Record) -> bool {
        let ret = self.count % self.n == 0;
        self.count += 1;
        ret
    }
}
