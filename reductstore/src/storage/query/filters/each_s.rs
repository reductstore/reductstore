// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::filters::{FilterPoint, RecordFilter};

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

impl<P> RecordFilter<P> for EachSecondFilter
where
    P: FilterPoint,
{
    fn filter(&mut self, point: &P) -> bool {
        let ret = point.timestamp() - self.last_time >= (self.s * 1_000_000.0) as i64;
        if ret {
            self.last_time = point.timestamp().clone();
        }

        ret
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::proto::Record;
    use prost_wkt_types::Timestamp;
    use rstest::*;

    #[rstest]
    fn test_each_s_filter() {
        let mut filter = EachSecondFilter::new(2.0);
        let mut record = Record::default();
        record.timestamp = Some(Timestamp {
            seconds: 1,
            nanos: 0,
        });

        assert!(filter.filter(&record));
        assert!(!filter.filter(&record));

        record.timestamp = Some(Timestamp {
            seconds: 2,
            nanos: 0,
        });

        assert!(!filter.filter(&record));

        record.timestamp = Some(Timestamp {
            seconds: 3,
            nanos: 0,
        });

        assert!(filter.filter(&record));
        assert!(!filter.filter(&record));
    }
}
