// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

pub mod base;
mod continuous;
mod historical;
mod limited;

use crate::storage::query::base::{Query, QueryOptions};

/// Build a query.
pub fn build_query(start: u64, stop: u64, options: QueryOptions) -> Box<dyn Query + Send + Sync> {
    if let Some(_) = options.limit {
        Box::new(limited::LimitedQuery::new(start, stop, options))
    } else if options.continuous {
        Box::new(continuous::ContinuousQuery::new(start, options))
    } else {
        Box::new(historical::HistoricalQuery::new(start, stop, options))
    }
}
