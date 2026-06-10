// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

//! Periodic usage statistics emitted as `$system` events.
//!
//! Traffic is counted at the storage engine choke points — all writes in
//! [`crate::storage::engine::StorageEngine::begin_write`] and all reads at
//! `RecordReader` creation — so external, replication and Zenoh traffic
//! count uniformly. A background task drains the counters every
//! [`usage_task::USAGE_FLUSH_INTERVAL`] and writes one flat-JSON event per
//! instance to `$system/usage/<instance>/total`.

mod usage_event_payload;
mod usage_task;

pub(crate) use usage_task::UsageStatsTask;

use std::sync::atomic::{AtomicU64, Ordering};

/// Interval traffic counters incremented at the storage engine choke points.
///
/// Plain relaxed atomics: the counters are independent tallies, no ordering
/// between them is required. `drain` swaps each counter to zero, so
/// increments that race with a flush roll into the next interval instead of
/// being lost.
#[derive(Debug, Default)]
pub struct UsageCounters {
    write_bytes: AtomicU64,
    read_bytes: AtomicU64,
    records_written: AtomicU64,
    records_read: AtomicU64,
}

/// Counter values drained for one flush interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DrainedUsageCounters {
    pub write_bytes: u64,
    pub read_bytes: u64,
    pub records_written: u64,
    pub records_read: u64,
}

impl UsageCounters {
    pub(crate) fn count_write(&self, bytes: u64) {
        self.write_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.records_written.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn count_read(&self, bytes: u64) {
        self.read_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.records_read.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn drain(&self) -> DrainedUsageCounters {
        DrainedUsageCounters {
            write_bytes: self.write_bytes.swap(0, Ordering::Relaxed),
            read_bytes: self.read_bytes.swap(0, Ordering::Relaxed),
            records_written: self.records_written.swap(0, Ordering::Relaxed),
            records_read: self.records_read.swap(0, Ordering::Relaxed),
        }
    }
}

/// Point-in-time storage totals collected in a single walk over all buckets.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct UsageSnapshot {
    pub storage_bytes: u64,
    pub bucket_count: u64,
    pub entry_count: u64,
    pub block_count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    fn drain_returns_counts_and_resets() {
        let counters = UsageCounters::default();
        counters.count_write(10);
        counters.count_write(5);
        counters.count_read(7);

        assert_eq!(
            counters.drain(),
            DrainedUsageCounters {
                write_bytes: 15,
                read_bytes: 7,
                records_written: 2,
                records_read: 1,
            }
        );
        assert_eq!(
            counters.drain(),
            DrainedUsageCounters {
                write_bytes: 0,
                read_bytes: 0,
                records_written: 0,
                records_read: 0,
            },
            "drain must reset the counters"
        );
    }

    #[rstest]
    fn counts_after_drain_land_in_next_interval() {
        let counters = UsageCounters::default();
        counters.count_write(10);
        counters.drain();

        counters.count_write(3);
        counters.count_read(4);
        let drained = counters.drain();
        assert_eq!(drained.write_bytes, 3);
        assert_eq!(drained.records_written, 1);
        assert_eq!(drained.read_bytes, 4);
        assert_eq!(drained.records_read, 1);
    }
}
