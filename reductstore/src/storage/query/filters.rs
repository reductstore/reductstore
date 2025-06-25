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
pub trait RecordFilter<R: Into<RecordMeta>> {
    /// Filter the record by condition.
    ///
    /// # Arguments
    ///
    /// * `record` - The record metadata to filter.
    /// * `ctx` - The context for the filter, which can be used to pass additional information.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(Vec<(R, Self::Ctx)>))` - returns a vector of records and their contexts if the record matches the filter. Empty vector if no records match.
    /// * `Ok(None)` - if the filtering is interrupted e.g. $limit operator is reached.
    /// * `Err(ReductError)` - if an error occurs during filtering.
    fn filter(&mut self, record: R) -> Result<Option<Vec<R>>, ReductError>;
}

pub(crate) fn apply_filters_recursively<R: Into<RecordMeta>>(
    filters: &mut [Box<dyn RecordFilter<R> + Send + Sync>],
    notifications: Vec<R>,
    index: usize,
) -> Result<Option<Vec<R>>, ReductError> {
    if index == filters.len() {
        return Ok(Some(notifications));
    }

    for notification in notifications {
        match filters[index].filter(notification)? {
            Some(notifications) => {
                if !notifications.is_empty() {
                    return apply_filters_recursively(filters, notifications, index + 1);
                }
            }

            None => return Ok(None),
        }
    }

    Ok(Some(vec![]))
}

use crate::replication::TransactionNotification;
pub(crate) use each_n::EachNFilter;
pub(crate) use each_s::EachSecondFilter;
pub(crate) use exclude::ExcludeLabelFilter;
pub(crate) use include::IncludeLabelFilter;
use log::warn;
pub(crate) use record_state::RecordStateFilter;
use reduct_base::error::ReductError;
use reduct_base::io::RecordMeta;
pub(crate) use time_range::TimeRangeFilter;
pub(crate) use when::WhenFilter;
