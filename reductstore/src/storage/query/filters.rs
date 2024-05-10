// Copyright 2023-2024 ReductStore
// Licensed under the Business Source License 1.1

mod each_n;
mod exclude;
mod include;
mod record_state;
mod time_range;

use crate::storage::proto::Record;

pub(super) trait RecordFilter {
    /// Filter the record by condition.
    fn filter(&self, record: &Record) -> bool;

    /// Notify the filter that a record has been sent.
    /// This is used to update the filter state.
    fn last_sent(&mut self, record: &Record) -> ();
}

pub(super) use each_n::EachNFilter;
pub(super) use exclude::ExcludeLabelFilter;
pub(super) use include::IncludeLabelFilter;
pub(super) use record_state::RecordStateFilter;
pub(super) use time_range::TimeRangeFilter;
