// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::filters::RecordFilter;
use reduct_base::error::ReductError;
use reduct_base::io::RecordMeta;

/// Filter that passes every N-th record
pub struct EachSecondFilter {
    s: f64,
    last_time: i64,
}

impl EachSecondFilter {
    pub fn new(s: f64) -> EachSecondFilter {
        if s <= 0.0 {
            panic!("Time must be greater than 0 seconds");
        }
        EachSecondFilter {
            s,
            last_time: -(s * 1_000_000.0) as i64, // take the first record
        }
    }
}

impl RecordFilter for EachSecondFilter {
    fn filter(&mut self, record: &RecordMeta) -> Result<bool, ReductError> {
        let ret = record.timestamp() as i64 - self.last_time >= (self.s * 1_000_000.0) as i64;
        if ret {
            self.last_time = record.timestamp().clone() as i64;
        }

        Ok(ret)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rstest::*;

    #[rstest]
    fn test_each_s_filter() {
        let mut filter = EachSecondFilter::new(2.0);
        let meta = RecordMeta::builder().timestamp(1000_000).build();

        assert!(filter.filter(&meta).unwrap());
        assert!(!filter.filter(&meta).unwrap());

        let meta = RecordMeta::builder().timestamp(2000_000).build();
        assert!(!filter.filter(&meta).unwrap());

        let meta = RecordMeta::builder().timestamp(3000_000).build();
        assert!(filter.filter(&meta).unwrap());
        assert!(!filter.filter(&meta).unwrap());
    }
}
