// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

pub mod base;
mod continuous;
mod historical;
mod limited;

use crate::storage::query::base::{Query, QueryOptions};
use reduct_base::error::ReductError;

/// Build a query.
pub fn build_query(
    start: u64,
    stop: u64,
    options: QueryOptions,
) -> Result<Box<dyn Query + Send + Sync>, ReductError> {
    if start > stop && !options.continuous {
        return Err(ReductError::unprocessable_entity(
            "Start time must be before stop time",
        ));
    }

    Ok(if let Some(_) = options.limit {
        Box::new(limited::LimitedQuery::new(start, stop, options))
    } else if options.continuous {
        Box::new(continuous::ContinuousQuery::new(start, options))
    } else {
        Box::new(historical::HistoricalQuery::new(start, stop, options))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    fn test_bad_start_stop() {
        let options = QueryOptions::default();
        assert_eq!(
            build_query(10, 5, options.clone()).err().unwrap(),
            ReductError::unprocessable_entity("Start time must be before stop time")
        );

        assert!(build_query(10, 10, options.clone()).is_ok());
        assert!(build_query(10, 11, options.clone()).is_ok());
    }

    #[rstest]
    fn test_ignore_stop_for_continuous() {
        let options = QueryOptions {
            continuous: true,
            ..Default::default()
        };
        assert!(build_query(10, 5, options.clone()).is_ok());
    }
}
