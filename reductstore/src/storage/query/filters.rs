// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

pub(crate) mod each_n;
pub(crate) mod each_s;
pub(crate) mod exclude;
pub(crate) mod include;
pub(crate) mod record_state;
pub(crate) mod time_range;
pub(crate) mod when;

use crate::storage::proto::record::Label;

/// Trait for record filters in queries.
pub trait RecordFilter {
    /// Filter the record by condition.
    fn filter(&mut self, record: &dyn RecordMeta) -> Result<bool, ReductError>;
}

pub(crate) use each_n::EachNFilter;
pub(crate) use each_s::EachSecondFilter;
pub(crate) use exclude::ExcludeLabelFilter;
pub(crate) use include::IncludeLabelFilter;
pub(crate) use record_state::RecordStateFilter;
use reduct_base::error::ReductError;
use reduct_base::io::RecordMeta;
pub(crate) use time_range::TimeRangeFilter;
pub(crate) use when::WhenFilter;
