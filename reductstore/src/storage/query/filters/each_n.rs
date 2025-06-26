// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::filters::{FilterRecord, RecordFilter};
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

impl<R: FilterRecord> RecordFilter<R> for EachNFilter {
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
    use crate::storage::query::filters::tests::TestFilterRecord;
    use reduct_base::io::RecordMeta;
    use rstest::*;

    #[rstest]
    fn test_each_n_filter() {
        let mut filter = EachNFilter::new(2);
        let record: TestFilterRecord = RecordMeta::builder().build().into();

        assert_eq!(
            filter.filter(record.clone()).unwrap(),
            Some(vec![record.clone()]),
            "First time should pass"
        );
        assert_eq!(
            filter.filter(record.clone()).unwrap(),
            Some(vec![]),
            "Second time should not pass"
        );
        assert_eq!(
            filter.filter(record.clone()).unwrap(),
            Some(vec![record.clone()]),
            "Third time should pass"
        );
        assert_eq!(
            filter.filter(record.clone()).unwrap(),
            Some(vec![]),
            "Fourth time should not pass"
        );
    }
}
