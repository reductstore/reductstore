// Copyright 2023-2024 ReductStore
// Licensed under the Business Source License 1.1

mod each_n;
mod each_s;
mod exclude;
mod include;
mod record_state;
mod time_range;

use crate::storage::proto::record::Label;

/// Trait for record filters in queries.
pub(super) trait RecordFilter<P>
where
    P: FilterPoint,
{
    /// Filter the record by condition.
    fn filter(&mut self, record: &P) -> bool;
}

pub(super) trait FilterPoint {
    fn timestamp(&self) -> i64;
    fn labels(&self) -> &Vec<Label>;
    fn state(&self) -> &i32;
}

pub(super) use each_n::EachNFilter;
pub(super) use each_s::EachSecondFilter;
pub(super) use exclude::ExcludeLabelFilter;
pub(super) use include::IncludeLabelFilter;
pub(super) use record_state::RecordStateFilter;
pub(super) use time_range::TimeRangeFilter;
