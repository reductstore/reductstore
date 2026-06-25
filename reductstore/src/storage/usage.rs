// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

//! Periodic usage statistics emitted as `$system` events.
//!
//! Traffic is counted at the storage IO choke points — all writes at
//! `RecordWriter` creation and all reads at `RecordReader` creation — so
//! external, replication and Zenoh traffic count uniformly.
//! [`UsageEventAggregator`] owns a background task that drains the counters
//! every [`usage_aggregator::USAGE_FLUSH_INTERVAL`] and writes one flat-JSON
//! event per instance to `$system/usage/<instance>/total`.

mod usage_aggregator;
mod usage_event_payload;

pub(crate) use usage_aggregator::UsageEventAggregator;

use crate::core::sync::AsyncRwLock;
use reduct_base::error::ReductError;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

/// Interval traffic counters incremented at the storage engine choke points.
///
/// The instance-wide tallies are plain relaxed atomics: independent counters,
/// no ordering between them is required. Per-bucket traffic — including each
/// bucket's set of distinct entries written to / read from — is kept in
/// `per_bucket` behind a single async lock so each `count_*` updates it in one
/// critical section. `drain` resets everything, so increments that race with a
/// flush roll into the next interval instead of being lost.
pub(crate) struct UsageCounters {
    write_bytes: AtomicU64,
    read_bytes: AtomicU64,
    records_written: AtomicU64,
    records_read: AtomicU64,
    per_bucket: AsyncRwLock<HashMap<String, BucketTraffic>>,
}

impl Default for UsageCounters {
    fn default() -> Self {
        Self {
            write_bytes: AtomicU64::new(0),
            read_bytes: AtomicU64::new(0),
            records_written: AtomicU64::new(0),
            records_read: AtomicU64::new(0),
            per_bucket: AsyncRwLock::new(HashMap::new()),
        }
    }
}

/// Interval traffic tallied for a single bucket.
#[derive(Debug, Default)]
struct BucketTraffic {
    write_bytes: u64,
    read_bytes: u64,
    records_written: u64,
    records_read: u64,
    written_entries: HashSet<String>,
    read_entries: HashSet<String>,
}

/// Counter values drained for one flush interval, for the instance total or a
/// single bucket.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DrainedUsageCounters {
    pub write_bytes: u64,
    pub read_bytes: u64,
    pub records_written: u64,
    pub records_read: u64,
    pub written_entries: u64,
    pub read_entries: u64,
}

/// Result of draining the counters: the instance total plus per-bucket traffic.
#[derive(Debug)]
pub(crate) struct DrainedUsage {
    pub total: DrainedUsageCounters,
    pub buckets: HashMap<String, DrainedUsageCounters>,
}

impl UsageCounters {
    pub(crate) async fn count_write(
        &self,
        bucket_name: &str,
        entry_name: &str,
        bytes: u64,
    ) -> Result<(), ReductError> {
        self.write_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.records_written.fetch_add(1, Ordering::Relaxed);

        let mut buckets = self.per_bucket.write().await?;
        let bucket = buckets.entry(bucket_name.to_string()).or_default();
        bucket.write_bytes += bytes;
        bucket.records_written += 1;
        bucket.written_entries.insert(entry_name.to_string());
        Ok(())
    }

    pub(crate) async fn count_read(
        &self,
        bucket_name: &str,
        entry_name: &str,
        bytes: u64,
    ) -> Result<(), ReductError> {
        self.read_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.records_read.fetch_add(1, Ordering::Relaxed);

        let mut buckets = self.per_bucket.write().await?;
        let bucket = buckets.entry(bucket_name.to_string()).or_default();
        bucket.read_bytes += bytes;
        bucket.records_read += 1;
        bucket.read_entries.insert(entry_name.to_string());
        Ok(())
    }

    pub(crate) async fn drain(&self) -> Result<DrainedUsage, ReductError> {
        let mut buckets_guard = self.per_bucket.write().await?;

        // Draining the whole map drops every bucket; only buckets with traffic
        // in the next interval are re-inserted, so a deleted bucket cannot grow
        // the map unbounded.
        let buckets: HashMap<String, DrainedUsageCounters> = buckets_guard
            .drain()
            .map(|(name, traffic)| {
                (
                    name,
                    DrainedUsageCounters {
                        write_bytes: traffic.write_bytes,
                        read_bytes: traffic.read_bytes,
                        records_written: traffic.records_written,
                        records_read: traffic.records_read,
                        written_entries: traffic.written_entries.len() as u64,
                        read_entries: traffic.read_entries.len() as u64,
                    },
                )
            })
            .collect();

        // An entry is identified by bucket + name, so the instance-total
        // distinct-entry counts are the sum of the per-bucket counts.
        let total = DrainedUsageCounters {
            write_bytes: self.write_bytes.swap(0, Ordering::Relaxed),
            read_bytes: self.read_bytes.swap(0, Ordering::Relaxed),
            records_written: self.records_written.swap(0, Ordering::Relaxed),
            records_read: self.records_read.swap(0, Ordering::Relaxed),
            written_entries: buckets.values().map(|bucket| bucket.written_entries).sum(),
            read_entries: buckets.values().map(|bucket| bucket.read_entries).sum(),
        };

        Ok(DrainedUsage { total, buckets })
    }
}

/// Point-in-time storage totals collected in a single walk over all buckets.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct UsageSnapshot {
    pub storage_bytes: u64,
    pub bucket_count: u64,
    pub entry_count: u64,
    pub block_count: u64,
    pub record_count: u64,
}

/// Point-in-time storage totals for a single bucket.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct BucketSnapshot {
    pub storage_bytes: u64,
    pub entry_count: u64,
    pub block_count: u64,
    pub record_count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn drain_returns_counts_and_resets() {
        let counters = UsageCounters::default();
        counters
            .count_write("bucket-1", "entry-1", 10)
            .await
            .unwrap();
        counters
            .count_write("bucket-1", "entry-1", 5)
            .await
            .unwrap();
        counters.count_read("bucket-1", "entry-1", 7).await.unwrap();

        assert_eq!(
            counters.drain().await.unwrap().total,
            DrainedUsageCounters {
                write_bytes: 15,
                read_bytes: 7,
                records_written: 2,
                records_read: 1,
                written_entries: 1,
                read_entries: 1,
            }
        );
        assert_eq!(
            counters.drain().await.unwrap().total,
            DrainedUsageCounters::default(),
            "drain must reset the counters"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn counts_after_drain_land_in_next_interval() {
        let counters = UsageCounters::default();
        counters
            .count_write("bucket-1", "entry-1", 10)
            .await
            .unwrap();
        counters.drain().await.unwrap();

        counters
            .count_write("bucket-1", "entry-1", 3)
            .await
            .unwrap();
        counters.count_read("bucket-1", "entry-1", 4).await.unwrap();
        let drained = counters.drain().await.unwrap().total;
        assert_eq!(drained.write_bytes, 3);
        assert_eq!(drained.records_written, 1);
        assert_eq!(drained.read_bytes, 4);
        assert_eq!(drained.records_read, 1);
    }

    #[rstest]
    #[tokio::test]
    async fn per_bucket_counts_isolate_by_bucket() {
        let counters = UsageCounters::default();
        counters
            .count_write("bucket-1", "entry-1", 10)
            .await
            .unwrap();
        counters
            .count_write("bucket-2", "entry-1", 25)
            .await
            .unwrap();
        counters.count_read("bucket-2", "entry-2", 7).await.unwrap();

        let drained = counters.drain().await.unwrap();
        assert_eq!(drained.total.write_bytes, 35);

        let b1 = drained.buckets.get("bucket-1").unwrap();
        assert_eq!(b1.write_bytes, 10);
        assert_eq!(b1.records_written, 1);
        assert_eq!(b1.read_bytes, 0);

        let b2 = drained.buckets.get("bucket-2").unwrap();
        assert_eq!(b2.write_bytes, 25);
        assert_eq!(b2.read_bytes, 7);
        assert_eq!(b2.records_read, 1);
    }

    #[rstest]
    #[tokio::test]
    async fn drained_buckets_are_removed_from_the_map() {
        let counters = UsageCounters::default();
        counters
            .count_write("bucket-1", "entry-1", 10)
            .await
            .unwrap();

        let drained = counters.drain().await.unwrap();
        assert!(drained.buckets.contains_key("bucket-1"));

        // Nothing was written in this interval, so the bucket must be gone.
        let drained = counters.drain().await.unwrap();
        assert!(
            drained.buckets.is_empty(),
            "a bucket that drains to zero must be removed from the map"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn distinct_entries_count_unique_names_not_operations() {
        let counters = UsageCounters::default();
        // Same entry written five times in one interval.
        for _ in 0..5 {
            counters
                .count_write("bucket-1", "entry-1", 10)
                .await
                .unwrap();
        }
        // Two distinct entries read.
        counters.count_read("bucket-1", "entry-1", 4).await.unwrap();
        counters.count_read("bucket-1", "entry-2", 4).await.unwrap();

        let drained = counters.drain().await.unwrap();
        assert_eq!(drained.total.records_written, 5);
        assert_eq!(
            drained.total.written_entries, 1,
            "five writes to one entry is one distinct written entry"
        );
        assert_eq!(drained.total.records_read, 2);
        assert_eq!(drained.total.read_entries, 2);

        let bucket = drained.buckets.get("bucket-1").unwrap();
        assert_eq!(bucket.written_entries, 1);
        assert_eq!(bucket.read_entries, 2);
    }

    #[rstest]
    #[tokio::test]
    async fn distinct_written_entries_count_two_different_entries() {
        let counters = UsageCounters::default();
        counters
            .count_write("bucket-1", "entry-1", 10)
            .await
            .unwrap();
        counters
            .count_write("bucket-1", "entry-2", 10)
            .await
            .unwrap();

        let drained = counters.drain().await.unwrap();
        assert_eq!(drained.total.written_entries, 2);
        assert_eq!(drained.total.records_written, 2);
    }

    #[rstest]
    #[tokio::test]
    async fn distinct_entries_are_keyed_by_bucket_and_name() {
        let counters = UsageCounters::default();
        // The same entry name in two different buckets in one interval.
        counters
            .count_write("bucket-a", "sensor-1", 10)
            .await
            .unwrap();
        counters
            .count_write("bucket-b", "sensor-1", 10)
            .await
            .unwrap();
        counters
            .count_read("bucket-a", "sensor-1", 10)
            .await
            .unwrap();
        counters
            .count_read("bucket-b", "sensor-1", 10)
            .await
            .unwrap();

        let drained = counters.drain().await.unwrap();
        assert_eq!(drained.buckets.get("bucket-a").unwrap().written_entries, 1);
        assert_eq!(drained.buckets.get("bucket-b").unwrap().written_entries, 1);
        assert_eq!(drained.buckets.get("bucket-a").unwrap().read_entries, 1);
        assert_eq!(drained.buckets.get("bucket-b").unwrap().read_entries, 1);

        // Composite keying: an entry is identified by bucket + name, so the same
        // name in two buckets is two distinct entries in the instance total.
        assert_eq!(drained.total.written_entries, 2);
        assert_eq!(drained.total.read_entries, 2);
    }
}
