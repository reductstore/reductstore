// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

pub(crate) mod each_n;
pub(crate) mod each_s;
pub(crate) mod exclude;
pub(crate) mod include;
pub(crate) mod record_state;
pub(crate) mod time_range;
pub(crate) mod when;

/// Trait for record filters in queries.
pub trait RecordFilter {
    /// Filter the record by condition.
    ///
    /// # Arguments
    ///
    /// * `record` - The record metadata to filter.
    ///
    /// # Returns
    ///
    /// * `Ok(true)` if the record passes the filter, `Ok(false)` otherwise.
    /// * Err(`ReductError::Interrupt`) if the filter is interrupted
    /// * `Err(ReductError)` if an error occurs during filtering.
    fn filter(&mut self, record: &RecordMeta) -> Result<bool, ReductError>;
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

#[cfg(test)]
mod tests {
    use crate::storage::proto::ts_to_us;
    use prost_wkt_types::Timestamp;
    use reduct_base::io::RecordMeta;
    use reduct_base::Labels;

    // Wrapper for Record to implement RecordMeta
    // and use it in filter tests
    pub(super) struct RecordWrapper {
        timestamp: u64,
        labels: Labels,
        state: i32,
    }
}
