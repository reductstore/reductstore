// Copyright 2023-2024 ReductStore
// Licensed under the Business Source License 1.1

use crate::storage::query::filters::{FilterPoint, RecordFilter};
use log::debug;

/// Filter that passes every N-th record
pub struct EachNFilter {
    n: u64,
    count: u64,
}

impl EachNFilter {
    pub fn new(n: u64) -> EachNFilter {
        if n == 0 {
            panic!("N must be greater than 0");
        }
        EachNFilter { n, count: 0 }
    }
}

impl<P> RecordFilter<P> for EachNFilter
where
    P: FilterPoint,
{
    fn filter(&mut self, p: &P) -> bool {
        let ret = self.count % self.n == 0;
        self.count += 1;
        if ret {
            debug!("Point {:?} passed", p.timestamp());
        } else {
            debug!("Point {:?} did not pass", p.timestamp());
        }
        ret
    }
}

#[cfg(test)]

mod tests {
    use super::*;
    use crate::storage::proto::Record;
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
