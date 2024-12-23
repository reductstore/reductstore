// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::filters::{FilterPoint, RecordFilter};
use reduct_base::error::ReductError;

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
    fn filter(&mut self, _: &P) -> Result<bool, ReductError> {
        let ret = self.count % self.n == 0;
        self.count += 1;
        Ok(ret)
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

        assert!(filter.filter(&record).unwrap(), "First time should pass");
        assert!(
            !filter.filter(&record).unwrap(),
            "Second time should not pass"
        );
        assert!(filter.filter(&record).unwrap(), "Third time should pass");
        assert!(
            !filter.filter(&record).unwrap(),
            "Fourth time should not pass"
        );
    }
}
