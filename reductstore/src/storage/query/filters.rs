// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

pub mod each_n;
pub mod each_s;
pub mod exclude;
pub mod include;
pub mod record_state;
pub mod time_range;

use crate::storage::proto::record::Label;

/// Trait for record filters in queries.
pub trait RecordFilter<P>
where
    P: FilterPoint,
{
    /// Filter the record by condition.
    fn filter(&mut self, record: &P) -> bool;
}

pub trait FilterPoint {
    fn timestamp(&self) -> i64;
    fn labels(&self) -> &Vec<Label>;
    fn state(&self) -> &i32;
}

pub use each_n::EachNFilter;
pub use each_s::EachSecondFilter;
pub use exclude::ExcludeLabelFilter;
pub use include::IncludeLabelFilter;
pub use record_state::RecordStateFilter;
pub use time_range::TimeRangeFilter;
