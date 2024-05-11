// Copyright 2023-2024 ReductStore
// Licensed under the Business Source License 1.1

mod each_n;
mod exclude;
mod include;
mod record_state;
mod time_range;

use crate::storage::proto::Record;

/// Trait for record filters in queries.
pub(super) trait RecordFilter {
    /// Filter the record by condition.
    fn filter(&mut self, record: &Record) -> bool;
}

pub(super) use exclude::ExcludeLabelFilter;
pub(super) use include::IncludeLabelFilter;
pub(super) use record_state::RecordStateFilter;
pub(super) use time_range::TimeRangeFilter;
