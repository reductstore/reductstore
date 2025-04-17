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

#[cfg(test)]
mod tests {
    use crate::storage::proto::{ts_to_us, Record};
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

    impl RecordMeta for RecordWrapper {
        fn timestamp(&self) -> u64 {
            self.timestamp
        }

        fn labels(&self) -> &Labels {
            &self.labels
        }

        fn state(&self) -> i32 {
            self.state
        }
    }

    impl From<Record> for RecordWrapper {
        fn from(record: Record) -> Self {
            RecordWrapper {
                timestamp: ts_to_us(record.timestamp.as_ref().unwrap_or(&Timestamp {
                    seconds: 0,
                    nanos: 0,
                })),
                labels: record
                    .labels
                    .iter()
                    .map(|l| (l.name.clone(), l.value.clone()))
                    .collect(),
                state: record.state,
            }
        }
    }
}
