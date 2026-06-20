// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::storage::bucket::Bucket;
use crate::storage::entry::{is_system_meta_entry, CompressionStats};
use reduct_base::error::ReductError;
use std::sync::Arc;

impl Bucket {
    /// Compress blocks over a timestamp range across matching entries.
    pub async fn compress_blocks(
        self: Arc<Self>,
        entries_filter: Option<Vec<String>>,
        start: Option<u64>,
        stop: Option<u64>,
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

            let stats = entry.compress_blocks(start, stop).await?;
            total.blocks += stats.blocks;
            total.records += stats.records;
        }

        Ok(total)
    }

    /// Count blocks that would be compressed over a timestamp range.
    pub async fn count_compressible_blocks(
        self: Arc<Self>,
        entries_filter: Option<Vec<String>>,
        start: Option<u64>,
        stop: Option<u64>,
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

            let stats = entry.count_compressible_blocks(start, stop).await?;
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
            Bucket::restore(bucket.path.clone(), Cfg::default(), Default::default())
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
                .count_compressible_blocks(None, None, None)
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
                .count_compressible_blocks(Some(vec!["entry-c".into()]), None, None)
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
            Bucket::restore(bucket.path.clone(), Cfg::default(), Default::default())
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
                .count_compressible_blocks(Some(vec!["entry-a/$meta".into()]), None, None)
                .await
                .unwrap(),
            CompressionStats::default()
        );
        assert!(bucket.begin_read("entry-a/$meta", 1).await.is_ok());
    }
}
