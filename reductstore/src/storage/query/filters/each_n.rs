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

#[cfg(test)]

mod tests {
    use super::*;
    use rstest::*;

    #[rstest]
    fn test_each_n_filter() {
        let mut filter = EachNFilter::new(2);
        let record = Record::default();

        assert!(filter.filter(&record), "First time should pass");
        assert!(!filter.filter(&record), "Second time should not pass");
        assert!(filter.filter(&record), "Third time should pass");
        assert!(!filter.filter(&record), "Fourth time should not pass");
    }
}
