// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use super::{CompressionStats, Entry};
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
    ) -> Result<CompressionStats, ReductError> {
        let blocks = {
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
                .filter_map(|&block_id| {
                    index
                        .get_block(block_id)
                        .map(|block| (block_id, block.record_count))
                })
                .collect::<Vec<_>>()
        };

        let mut stats = CompressionStats::default();
        for (block_id, record_count) in blocks {
            {
                let mut bm = self.block_manager.write().await?;
                if bm.is_block_in_write_cache(block_id) {
                    continue;
                }

                match bm
                    .compress_block(block_id, CompressionAlgorithm::Zstd)
                    .await
                {
                    Ok(()) => {
                        stats.blocks += 1;
                        stats.records += record_count;
                    }
                    Err(err)
                        if matches!(err.status(), ErrorCode::Conflict | ErrorCode::NotFound) => {}
                    Err(err) => return Err(err),
                }
            }
            tokio::task::yield_now().await;
        }

        Ok(stats)
    }

    /// Count uncompressed blocks and their records whose block IDs fall within `[start, stop)`.
    pub async fn count_compressible_blocks(
        &self,
        start: Option<u64>,
        stop: Option<u64>,
    ) -> Result<CompressionStats, ReductError> {
        let bm = self.block_manager.read().await?;
        let index = bm.index();

        let range: Box<dyn Iterator<Item = &u64> + '_> = match (start, stop) {
            (Some(start), Some(stop)) => Box::new(index.tree().range(start..stop)),
            (Some(start), None) => Box::new(index.tree().range(start..)),
            (None, Some(stop)) => Box::new(index.tree().range(..stop)),
            (None, None) => Box::new(index.tree().iter()),
        };

        let stats = range
            .filter(|&&block_id| {
                !bm.is_block_in_write_cache(block_id)
                    && index
                        .get_block(block_id)
                        .and_then(|block| block.compression)
                        .unwrap_or(i32::from(CompressionAlgorithm::None))
                        == i32::from(CompressionAlgorithm::None)
            })
            .filter_map(|block_id| index.get_block(*block_id))
            .fold(CompressionStats::default(), |mut stats, block| {
                stats.blocks += 1;
                stats.records += block.record_count;
                stats
            });

        Ok(stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::Cfg;
    use crate::core::file_cache::FILE_CACHE;
    use crate::storage::block_manager::DATA_FILE_EXT;
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

        assert_eq!(
            entry.compress_blocks(None, None).await.unwrap(),
            CompressionStats {
                blocks: 3,
                records: 3
            }
        );

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
            CompressionStats {
                blocks: 1,
                records: 1
            }
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
            CompressionStats {
                blocks: 2,
                records: 2
            }
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
            CompressionStats {
                blocks: 2,
                records: 2
            }
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

        assert_eq!(
            entry.compress_blocks(None, None).await.unwrap(),
            CompressionStats {
                blocks: 3,
                records: 3
            }
        );
        assert_eq!(
            entry.compress_blocks(None, None).await.unwrap(),
            CompressionStats::default()
        );
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_compress_blocks_does_not_block_concurrent_reads(path: PathBuf) {
        let block_ids = [1_000_000, 2_000_000, 3_000_000, 4_000_000, 5_000_000];
        let entry = entry(multi_block_settings(), path.clone()).await;
        write_blocks(&entry, &block_ids).await;
        let entry = Arc::new(restore_flushed_entry(&entry, multi_block_settings(), path).await);

        let observer_entry = Arc::clone(&entry);
        let observer = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(2), async move {
                loop {
                    let compressed = count_compressed(&observer_entry, &block_ids).await;
                    if compressed > 0 && compressed < block_ids.len() {
                        return;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("compression should release the block manager lock between blocks");
        });

        let compress_entry = Arc::clone(&entry);
        let compressor =
            tokio::spawn(async move { compress_entry.compress_blocks(None, None).await });

        observer.await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), entry.begin_read(1_000_000))
            .await
            .expect("concurrent read should not wait for all blocks to compress")
            .unwrap();

        assert_eq!(
            compressor.await.unwrap().unwrap(),
            CompressionStats {
                blocks: block_ids.len() as u64,
                records: block_ids.len() as u64,
            }
        );
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_compress_blocks_empty_entry(path: PathBuf) {
        let entry = entry(multi_block_settings(), path).await;
        assert_eq!(
            entry.compress_blocks(None, None).await.unwrap(),
            CompressionStats::default()
        );
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
            CompressionStats {
                blocks: 3,
                records: 3
            }
        );
        assert_eq!(
            entry
                .count_compressible_blocks(Some(2_000_000), Some(3_000_000))
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
    async fn test_count_compressible_blocks_start_only(path: PathBuf) {
        let entry = entry(multi_block_settings(), path.clone()).await;
        write_blocks(&entry, &[1_000_000, 2_000_000, 3_000_000]).await;
        let entry = restore_flushed_entry(&entry, multi_block_settings(), path).await;

        assert_eq!(
            entry
                .count_compressible_blocks(Some(2_000_000), None)
                .await
                .unwrap(),
            CompressionStats {
                blocks: 2,
                records: 2
            }
        );
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_count_compressible_blocks_stop_only(path: PathBuf) {
        let entry = entry(multi_block_settings(), path.clone()).await;
        write_blocks(&entry, &[1_000_000, 2_000_000, 3_000_000]).await;
        let entry = restore_flushed_entry(&entry, multi_block_settings(), path).await;

        assert_eq!(
            entry
                .count_compressible_blocks(None, Some(3_000_000))
                .await
                .unwrap(),
            CompressionStats {
                blocks: 2,
                records: 2
            }
        );
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_count_compressible_blocks_after_compression(path: PathBuf) {
        let entry = entry(multi_block_settings(), path.clone()).await;
        write_blocks(&entry, &[1_000_000, 2_000_000, 3_000_000]).await;
        let entry = restore_flushed_entry(&entry, multi_block_settings(), path).await;

        assert_eq!(
            entry.compress_blocks(None, None).await.unwrap(),
            CompressionStats {
                blocks: 3,
                records: 3
            }
        );
        assert_eq!(
            entry.count_compressible_blocks(None, None).await.unwrap(),
            CompressionStats::default()
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

        assert_eq!(
            entry.compress_blocks(None, None).await.unwrap(),
            CompressionStats {
                blocks: 1,
                records: 1
            }
        );

        assert_compressed(&entry, 1_000_000, true).await;
        assert_compressed(&entry, 2_000_000, false).await;
        assert_compressed(&entry, 3_000_000, false).await;
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_compress_blocks_returns_internal_error(path: PathBuf) {
        let entry = entry(multi_block_settings(), path.clone()).await;
        write_blocks(&entry, &[1_000_000]).await;
        let entry = restore_flushed_entry(&entry, multi_block_settings(), path).await;
        {
            let bm = entry.block_manager.read().await.unwrap();
            FILE_CACHE
                .remove(&bm.path().join(format!("1000000{}", DATA_FILE_EXT)))
                .await
                .unwrap();
        }

        let err = entry.compress_blocks(None, None).await.err().unwrap();

        assert_eq!(err.status(), ErrorCode::InternalServerError);
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
            Entry::try_build(
                "entry",
                path.clone(),
                settings,
                Cfg::default().into(),
                Default::default(),
            )
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
            Default::default(),
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

    async fn count_compressed(entry: &Entry, block_ids: &[u64]) -> usize {
        let bm = entry.block_manager.read().await.unwrap();
        block_ids
            .iter()
            .filter(|&&block_id| {
                bm.index()
                    .get_block(block_id)
                    .and_then(|block| block.compression)
                    .unwrap_or(i32::from(CompressionAlgorithm::None))
                    == i32::from(CompressionAlgorithm::Zstd)
            })
            .count()
    }
}
