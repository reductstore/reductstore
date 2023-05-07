// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub mod base;
mod continuous;
mod historical;

use crate::storage::query::base::{Query, QueryOptions};

/// Build a query.
pub fn build_query(start: u64, stop: u64, options: QueryOptions) -> Box<dyn Query + Send + Sync> {
    if options.continuous {
        Box::new(continuous::ContinuousQuery::new(start, options))
    } else {
        Box::new(historical::HistoricalQuery::new(start, stop, options))
    }
}
