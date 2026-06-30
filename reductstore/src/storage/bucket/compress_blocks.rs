// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::storage::bucket::Bucket;
use crate::storage::entry::{is_system_meta_entry, CompressionStats};
use reduct_base::error::ReductError;
use std::sync::Arc;

enum CompressionMode {
    Compress,
    Estimate,
}

impl Bucket {
    /// Compress blocks over a timestamp range across matching entries.
    pub async fn compress_blocks(
        self: Arc<Self>,
        entries_filter: Option<Vec<String>>,
        start: Option<u64>,
        stop: Option<u64>,
    ) -> Result<CompressionStats, ReductError> {
        self.process_compressible_data(entries_filter, start, stop, CompressionMode::Compress)
            .await
    }

    /// Estimate blocks and records that would be compressed over a timestamp range.
    pub async fn estimate_compressible_data(
        self: Arc<Self>,
        entries_filter: Option<Vec<String>>,
        start: Option<u64>,
        stop: Option<u64>,
    ) -> Result<CompressionStats, ReductError> {
        self.process_compressible_data(entries_filter, start, stop, CompressionMode::Estimate)
            .await
    }

    async fn process_compressible_data(
        self: Arc<Self>,
        entries_filter: Option<Vec<String>>,
        start: Option<u64>,
        stop: Option<u64>,
        mode: CompressionMode,
    ) -> Result<CompressionStats, ReductError> {
        let entries = self.entries.read().await?.clone();
        let requested_entries = Self::requested_entries(&entries_filter);
        let mut total = CompressionStats::default();

        for (entry_name, entry) in entries {
            if !Self::is_requested_entry(&entry_name, &requested_entries) {
                continue;
            }

            if is_system_meta_entry(&entry_name) {
                continue;
            }

            let stats = match mode {
                CompressionMode::Compress => entry.compress_blocks(start, stop).await?,
                CompressionMode::Estimate => entry.estimate_compressible_data(start, stop).await?,
            };
            total.blocks += stats.blocks;
            total.records += stats.records;
        }

        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::Cfg;
    use crate::storage::bucket::tests::{bucket, write, write_meta};
    use rstest::rstest;
    use serial_test::serial;

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_compress_blocks_filters_entries(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        write(&bucket, "entry-a", 1, b"a1").await.unwrap();
        write(&bucket, "entry-b", 2, b"b1").await.unwrap();
        write(&bucket, "entry-c", 3, b"c1").await.unwrap();
        bucket.sync_fs().await.unwrap();
        let bucket = Arc::new(
            Bucket::builder()
                .path(bucket.path.clone())
                .cfg(Cfg::default())
                .usage_counters(Default::default())
                .restore()
                .await
                .unwrap(),
        );

        let compressed = bucket
            .clone()
            .compress_blocks(Some(vec!["entry-a".into(), "entry-b".into()]), None, None)
            .await
            .unwrap();

        assert_eq!(
            compressed,
            CompressionStats {
                blocks: 2,
                records: 2
            }
        );
        assert_eq!(
            bucket
                .clone()
                .estimate_compressible_data(None, None, None)
                .await
                .unwrap(),
            CompressionStats {
                blocks: 1,
                records: 1
            }
        );
        assert_eq!(
            bucket
                .clone()
                .estimate_compressible_data(Some(vec!["entry-c".into()]), None, None)
                .await
                .unwrap(),
            CompressionStats {
                blocks: 1,
                records: 1
            }
        );
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_compress_blocks_skips_meta_entries(#[future] bucket: Arc<Bucket>) {
        let bucket = bucket.await;
        write_meta(&bucket, "entry-a/$meta", 1, b"meta")
            .await
            .unwrap();
        bucket.sync_fs().await.unwrap();
        let bucket = Arc::new(
            Bucket::builder()
                .path(bucket.path.clone())
                .cfg(Cfg::default())
                .usage_counters(Default::default())
                .restore()
                .await
                .unwrap(),
        );

        let compressed = bucket
            .clone()
            .compress_blocks(Some(vec!["entry-a/$meta".into()]), None, None)
            .await
            .unwrap();

        assert_eq!(compressed, CompressionStats::default());
        assert_eq!(
            bucket
                .clone()
                .estimate_compressible_data(Some(vec!["entry-a/$meta".into()]), None, None)
                .await
                .unwrap(),
            CompressionStats::default()
        );
        assert!(bucket.begin_read("entry-a/$meta", 1).await.is_ok());
    }
}
