// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use super::Entry;
use crate::storage::block_manager::compress::CompressionAlgorithm;
use reduct_base::error::{ErrorCode, ReductError};

impl Entry {
    /// Compress uncompressed blocks whose block IDs fall within `[start, stop)`.
    ///
    /// Blocks still present in the write cache are skipped to avoid compressing
    /// blocks that may not be fully flushed yet.
    pub async fn compress_blocks(
        &self,
        start: Option<u64>,
        stop: Option<u64>,
    ) -> Result<u64, ReductError> {
        let block_ids = {
            let bm = self.block_manager.read().await?;
            let index = bm.index();

            let range: Box<dyn Iterator<Item = &u64> + '_> = match (start, stop) {
                (Some(start), Some(stop)) => Box::new(index.tree().range(start..stop)),
                (Some(start), None) => Box::new(index.tree().range(start..)),
                (None, Some(stop)) => Box::new(index.tree().range(..stop)),
                (None, None) => Box::new(index.tree().iter()),
            };

            range
                .filter(|&&block_id| {
                    index
                        .get_block(block_id)
                        .and_then(|block| block.compression)
                        .unwrap_or(i32::from(CompressionAlgorithm::None))
                        == i32::from(CompressionAlgorithm::None)
                })
                .copied()
                .collect::<Vec<_>>()
        };

        let mut compressed_count = 0;
        for block_id in block_ids {
            let mut bm = self.block_manager.write().await?;
            if bm.is_block_in_write_cache(block_id) {
                continue;
            }

            match bm
                .compress_block(block_id, CompressionAlgorithm::Zstd)
                .await
            {
                Ok(()) => compressed_count += 1,
                Err(err) if matches!(err.status(), ErrorCode::Conflict | ErrorCode::NotFound) => {}
                Err(err) => return Err(err),
            }
        }

        Ok(compressed_count)
    }

    /// Count uncompressed blocks whose block IDs fall within `[start, stop)`.
    pub async fn count_compressible_blocks(
        &self,
        start: Option<u64>,
        stop: Option<u64>,
    ) -> Result<u64, ReductError> {
        let bm = self.block_manager.read().await?;
        let index = bm.index();

        let range: Box<dyn Iterator<Item = &u64> + '_> = match (start, stop) {
            (Some(start), Some(stop)) => Box::new(index.tree().range(start..stop)),
            (Some(start), None) => Box::new(index.tree().range(start..)),
            (None, Some(stop)) => Box::new(index.tree().range(..stop)),
            (None, None) => Box::new(index.tree().iter()),
        };

        let count = range
            .filter(|&&block_id| {
                !bm.is_block_in_write_cache(block_id)
                    && index
                        .get_block(block_id)
                        .and_then(|block| block.compression)
                        .unwrap_or(i32::from(CompressionAlgorithm::None))
                        == i32::from(CompressionAlgorithm::None)
            })
            .count() as u64;

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::Cfg;
    use crate::storage::entry::EntrySettings;
    use bytes::Bytes;
    use reduct_base::Labels;
    use rstest::{fixture, rstest};
    use serial_test::serial;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_compress_blocks_full_range(path: PathBuf) {
        let entry = entry(multi_block_settings(), path.clone()).await;
        write_blocks(&entry, &[1_000_000, 2_000_000, 3_000_000]).await;
        let entry = restore_flushed_entry(&entry, multi_block_settings(), path).await;

        assert_eq!(entry.compress_blocks(None, None).await.unwrap(), 3);

        assert_compressed(&entry, 1_000_000, true).await;
        assert_compressed(&entry, 2_000_000, true).await;
        assert_compressed(&entry, 3_000_000, true).await;
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_compress_blocks_partial_range(path: PathBuf) {
        let entry = entry(multi_block_settings(), path.clone()).await;
        write_blocks(&entry, &[1_000_000, 2_000_000, 3_000_000]).await;
        let entry = restore_flushed_entry(&entry, multi_block_settings(), path).await;

        assert_eq!(
            entry
                .compress_blocks(Some(2_000_000), Some(3_000_000))
                .await
                .unwrap(),
            1
        );

        assert_compressed(&entry, 1_000_000, false).await;
        assert_compressed(&entry, 2_000_000, true).await;
        assert_compressed(&entry, 3_000_000, false).await;
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_compress_blocks_start_only(path: PathBuf) {
        let entry = entry(multi_block_settings(), path.clone()).await;
        write_blocks(&entry, &[1_000_000, 2_000_000, 3_000_000]).await;
        let entry = restore_flushed_entry(&entry, multi_block_settings(), path).await;

        assert_eq!(
            entry.compress_blocks(Some(2_000_000), None).await.unwrap(),
            2
        );

        assert_compressed(&entry, 1_000_000, false).await;
        assert_compressed(&entry, 2_000_000, true).await;
        assert_compressed(&entry, 3_000_000, true).await;
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_compress_blocks_stop_only(path: PathBuf) {
        let entry = entry(multi_block_settings(), path.clone()).await;
        write_blocks(&entry, &[1_000_000, 2_000_000, 3_000_000]).await;
        let entry = restore_flushed_entry(&entry, multi_block_settings(), path).await;

        assert_eq!(
            entry.compress_blocks(None, Some(3_000_000)).await.unwrap(),
            2
        );

        assert_compressed(&entry, 1_000_000, true).await;
        assert_compressed(&entry, 2_000_000, true).await;
        assert_compressed(&entry, 3_000_000, false).await;
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_compress_blocks_skips_already_compressed(path: PathBuf) {
        let entry = entry(multi_block_settings(), path.clone()).await;
        write_blocks(&entry, &[1_000_000, 2_000_000, 3_000_000]).await;
        let entry = restore_flushed_entry(&entry, multi_block_settings(), path).await;

        assert_eq!(entry.compress_blocks(None, None).await.unwrap(), 3);
        assert_eq!(entry.compress_blocks(None, None).await.unwrap(), 0);
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_compress_blocks_empty_entry(path: PathBuf) {
        let entry = entry(multi_block_settings(), path).await;
        assert_eq!(entry.compress_blocks(None, None).await.unwrap(), 0);
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_count_compressible_blocks_full_range(path: PathBuf) {
        let entry = entry(multi_block_settings(), path.clone()).await;
        write_blocks(&entry, &[1_000_000, 2_000_000, 3_000_000]).await;
        let entry = restore_flushed_entry(&entry, multi_block_settings(), path).await;

        assert_eq!(
            entry.count_compressible_blocks(None, None).await.unwrap(),
            3
        );
        assert_eq!(
            entry
                .count_compressible_blocks(Some(2_000_000), Some(3_000_000))
                .await
                .unwrap(),
            1
        );
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_count_compressible_blocks_after_compression(path: PathBuf) {
        let entry = entry(multi_block_settings(), path.clone()).await;
        write_blocks(&entry, &[1_000_000, 2_000_000, 3_000_000]).await;
        let entry = restore_flushed_entry(&entry, multi_block_settings(), path).await;

        assert_eq!(entry.compress_blocks(None, None).await.unwrap(), 3);
        assert_eq!(
            entry.count_compressible_blocks(None, None).await.unwrap(),
            0
        );
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_compress_blocks_skips_write_cache(path: PathBuf) {
        let entry = entry(multi_block_settings(), path).await;
        write_blocks(&entry, &[1_000_000, 2_000_000, 3_000_000]).await;
        entry
            .block_manager
            .write()
            .await
            .unwrap()
            .save_cache_on_disk()
            .await
            .unwrap();

        assert_eq!(entry.compress_blocks(None, None).await.unwrap(), 1);

        assert_compressed(&entry, 1_000_000, true).await;
        assert_compressed(&entry, 2_000_000, false).await;
        assert_compressed(&entry, 3_000_000, false).await;
    }

    #[fixture]
    fn path() -> PathBuf {
        tempfile::tempdir().unwrap().keep().join("bucket")
    }

    fn multi_block_settings() -> EntrySettings {
        EntrySettings {
            max_block_size: 10000,
            max_block_records: 1,
        }
    }

    async fn entry(settings: EntrySettings, path: PathBuf) -> Arc<Entry> {
        Arc::new(
            Entry::try_build("entry", path.clone(), settings, Cfg::default().into())
                .await
                .unwrap(),
        )
    }

    async fn write_blocks(entry: &Arc<Entry>, block_ids: &[u64]) {
        for block_id in block_ids {
            write_stub_record(entry, *block_id).await;
        }
    }

    async fn write_stub_record(entry: &Arc<Entry>, time: u64) {
        write_record(entry, time, b"0123456789".to_vec()).await;
    }

    async fn write_record(entry: &Arc<Entry>, time: u64, data: Vec<u8>) {
        let mut sender = entry
            .clone()
            .begin_write(
                time,
                data.len() as u64,
                "text/plain".to_string(),
                Labels::new(),
            )
            .await
            .unwrap();
        sender.send(Ok(Some(Bytes::from(data)))).await.unwrap();
        sender.send(Ok(None)).await.expect("Failed to send None");
        drop(sender);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    async fn restore_flushed_entry(
        entry: &Arc<Entry>,
        settings: EntrySettings,
        bucket_path: PathBuf,
    ) -> Entry {
        entry
            .block_manager
            .write()
            .await
            .unwrap()
            .save_cache_on_disk()
            .await
            .unwrap();

        Entry::restore(
            bucket_path.join(entry.name()),
            entry.name().to_string(),
            entry.bucket_name().to_string(),
            settings,
            Cfg::default().into(),
        )
        .await
        .unwrap()
        .unwrap()
    }

    async fn assert_compressed(entry: &Entry, block_id: u64, expected: bool) {
        let bm = entry.block_manager.read().await.unwrap();
        let block = bm.index().get_block(block_id).unwrap();
        assert_eq!(
            block
                .compression
                .unwrap_or(i32::from(CompressionAlgorithm::None))
                == i32::from(CompressionAlgorithm::Zstd),
            expected,
            "unexpected compression state for block {}",
            block_id
        );
    }
}
