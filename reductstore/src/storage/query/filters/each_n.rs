// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::filters::RecordFilter;
use reduct_base::error::ReductError;
use reduct_base::io::RecordMeta;

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

impl<R: Into<RecordMeta>> RecordFilter<R> for EachNFilter {
    fn filter(&mut self, record: R) -> Result<Option<Vec<R>>, ReductError> {
        let ret = self.count % self.n == 0;
        self.count += 1;
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

    use rstest::*;

    #[rstest]
    fn test_each_n_filter() {
        let mut filter = EachNFilter::new(2);
        let meta = RecordMeta::builder().build();

        assert!(filter.filter(&meta).unwrap(), "First time should pass");
        assert!(
            !filter.filter(&meta).unwrap(),
            "Second time should not pass"
        );
        assert!(filter.filter(&meta).unwrap(), "Third time should pass");
        assert!(
            !filter.filter(&meta).unwrap(),
            "Fourth time should not pass"
        );
    }
}
