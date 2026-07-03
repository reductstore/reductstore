// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

pub(in crate::storage) mod block;
mod block_cache;
pub(in crate::storage) mod block_index;
pub(in crate::storage) mod compress;
pub(in crate::storage) mod decompress_cache;
mod lifecycle;
mod lookup;
mod paths;
mod persistence;
mod read_only;
mod records;
pub(in crate::storage) mod wal;

use crate::backend::BackendType;
use crate::cfg::{Cfg, InstanceRole};
use crate::core::file_cache::FILE_CACHE;
use crate::core::sync::AsyncRwLock;
use crate::storage::block_manager::block::Block;
use crate::storage::block_manager::block_cache::BlockCache;
use crate::storage::block_manager::compress::CompressionAlgorithm;
use crate::storage::block_manager::decompress_cache::{DecompressCache, DecompressedFileType};
use crate::storage::block_manager::wal::{create_wal, Wal, WalEntry};
use crate::storage::entry::io::record_reader::read_in_chunks;
use crate::storage::proto::{record, ts_to_us, us_to_ts, Block as BlockProto, Record};
use crate::storage::usage::UsageCounters;
use block_index::BlockIndex;
use crc64fast::Digest;
use log::{debug, error, trace, warn};
use prost::bytes::{Bytes, BytesMut};
use prost::Message;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use reduct_base::too_early;
use std::fs::OpenOptions;
use std::io::{Read, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{Receiver, Sender};

pub(crate) type BlockRef = Arc<AsyncRwLock<Block>>;

/// Helper class for IO operations with blocks and records.
///
/// ## Notes
///
/// It is not thread safe and may cause data corruption if used from multiple threads,
/// because it does not lock the block descriptor file. Use it with RwLock<BlockManager>
pub(in crate::storage) struct BlockManager {
    path: PathBuf,
    bucket: String,
    entry: String,
    block_index: BlockIndex,
    block_cache: BlockCache,
    decompress_cache: DecompressCache,
    wal: Box<dyn Wal + Sync + Send>,
    cfg: Arc<Cfg>,
    usage_counters: Arc<UsageCounters>,
    last_replica_sync: Instant,
}

pub const DESCRIPTOR_FILE_EXT: &str = ".meta";
pub const DATA_FILE_EXT: &str = ".blk";
pub const COMPRESSED_DESCRIPTOR_FILE_EXT: &str = ".meta.zst";
pub const COMPRESSED_DATA_FILE_EXT: &str = ".blk.zst";
pub const BLOCK_INDEX_FILE: &str = "blocks.idx";

// we need 2 to avoid double sync when start a new one but not yet saved the old one when the record is written
const WRITE_BLOCK_CACHE_SIZE: usize = 2;

// global read cache size, shared by all entries, should be large enough to hold hot blocks of all entries but not too large to cause memory pressure
const READ_BLOCK_CACHE_SIZE: usize = 128;
impl BlockManager {
    /// Create a new block manager.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the block manager directory.
    /// * `index` - Block index to use.
    /// * `bucket` - Bucket name.
    /// * `entry` - Full entry name.
    /// * `cfg` - Configuration.
    /// * `usage_counters` - Shared usage traffic counters.
    pub(crate) async fn build(
        path: PathBuf,
        index: BlockIndex,
        bucket: String,
        entry: String,
        cfg: Arc<Cfg>,
        usage_counters: Arc<UsageCounters>,
    ) -> Result<Self, ReductError> {
        Ok(Self {
            path: path.clone(),
            bucket,
            entry,
            block_index: index,
            block_cache: BlockCache::new(
                path.to_string_lossy().into_owned(),
                WRITE_BLOCK_CACHE_SIZE,
                READ_BLOCK_CACHE_SIZE,
                Duration::from_secs(30),
            ),
            decompress_cache: DecompressCache::default(),
            wal: create_wal(path.clone()).await?,
            cfg,
            usage_counters,
            last_replica_sync: Instant::now(),
        })
    }
}

pub type RecordRx = Receiver<Result<Bytes, ReductError>>;
pub type RecordTx = Sender<Result<Option<Bytes>, ReductError>>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::sync::AsyncRwLock;
    use crate::storage::proto::record::Label;
    use crate::storage::proto::Record;
    use crate::storage::proto::{us_to_ts, BlockIndex as BlockIndexProto};
    use prost_wkt_types::Timestamp;
    use reduct_base::error::ErrorCode;
    use rstest::{fixture, rstest};

    use crate::storage::engine::MAX_IO_BUFFER_SIZE;
    use crate::storage::entry::RecordWriter;
    use rand::distr::Alphanumeric;
    use rand::{rng, RngExt};
    use reduct_base::io::WriteRecord;
    use std::time::Duration;
    use tempfile::tempdir;

    mod block_operations {
        use super::*;
        use reduct_base::io::WriteRecord;

        #[rstest]
        #[tokio::test]
        async fn test_sync_data_block_ok_for_replica() {
            let path = tempdir().unwrap().keep().join("bucket").join("entry");
            let mut cfg = Cfg::default();
            cfg.role = InstanceRole::Replica;
            let block_manager = BlockManager::build(
                path.clone(),
                BlockIndex::new(path.clone()),
                "bucket".to_string(),
                "entry".to_string(),
                Arc::new(cfg),
                Default::default(),
            )
            .await
            .unwrap();
            block_manager.sync_data_block(1).await.unwrap();
        }

        #[rstest]
        #[tokio::test]
        async fn test_sync_data_block_ok_for_missing_path() {
            let path = tempdir().unwrap().keep().join("bucket").join("entry");
            let cfg = Cfg::default();
            let block_manager = BlockManager::build(
                path.clone(),
                BlockIndex::new(path.clone()),
                "bucket".to_string(),
                "entry".to_string(),
                Arc::new(cfg),
                Default::default(),
            )
            .await
            .unwrap();
            block_manager.sync_data_block(999).await.unwrap();
        }

        #[rstest]
        #[tokio::test]
        async fn test_starting_block(#[future] block_manager: BlockManager) {
            let mut block_manager = block_manager.await;
            let block_id = 1_000_005;

            let block_ref = block_manager.start_new_block(block_id, 1024).await.unwrap();
            assert_eq!(block_ref.read().await.unwrap().block_id(), block_id,);

            // Create an empty block
            block_manager.save_cache_on_disk().await.unwrap();
            let file = std::fs::File::open(
                block_manager
                    .path
                    .join(format!("{}{}", block_id, DATA_FILE_EXT)),
            )
            .unwrap();
            assert_eq!(file.metadata().unwrap().len(), 1024);

            // Create a block descriptor
            let buf = std::fs::read(
                block_manager
                    .path
                    .join(format!("{}{}", block_id, DESCRIPTOR_FILE_EXT)),
            )
            .unwrap();

            let block_from_file: Block = BlockProto::decode(Bytes::from(buf)).unwrap().into();
            assert_eq!(block_from_file, block_ref.read().await.unwrap().to_owned());
        }

        #[rstest]
        #[tokio::test]
        async fn test_save_meta_stores_version_in_descriptor() {
            let path = tempdir().unwrap().keep().join("bucket").join("entry");
            FILE_CACHE.create_dir_all(&path).await.unwrap();
            let mut block_manager = BlockManager::build(
                path.clone(),
                BlockIndex::new(path.join(BLOCK_INDEX_FILE)),
                "bucket".to_string(),
                "entry".to_string(),
                Cfg::default().into(),
                Default::default(),
            )
            .await
            .unwrap();
            let block_id = 1;
            let block_ref = block_manager.start_new_block(block_id, 1024).await.unwrap();
            block_manager
                .save_meta_on_disk(block_ref.clone())
                .await
                .unwrap();

            let block_proto = BlockProto::decode(
                std::fs::read(block_manager.path_to_desc(block_id))
                    .unwrap()
                    .as_slice(),
            )
            .unwrap();
            assert_eq!(block_proto.version, Some(1));

            block_manager.save_meta_on_disk(block_ref).await.unwrap();

            let block_proto = BlockProto::decode(
                std::fs::read(block_manager.path_to_desc(block_id))
                    .unwrap()
                    .as_slice(),
            )
            .unwrap();
            assert_eq!(block_proto.version, Some(2));
        }

        #[rstest]
        #[tokio::test]
        async fn test_save_cache_metadata_skips_blocks_without_wal() {
            let path = tempdir().unwrap().keep().join("bucket").join("entry");
            let mut block_manager = BlockManager::build(
                path.clone(),
                BlockIndex::new(path.join(BLOCK_INDEX_FILE)),
                "bucket".to_string(),
                "entry".to_string(),
                Cfg::default().into(),
                Default::default(),
            )
            .await
            .unwrap();

            let block_id = 1_000_005;
            block_manager.start_new_block(block_id, 1024).await.unwrap();

            block_manager.save_cache_metadata_on_disk().await.unwrap();

            assert!(!path
                .join(format!("{}{}", block_id, DESCRIPTOR_FILE_EXT))
                .exists());
            assert!(!path.join(BLOCK_INDEX_FILE).exists());
        }

        #[rstest]
        #[tokio::test]
        async fn test_starting_block_no_preallocation_for_remote_backend() {
            let path = tempdir().unwrap().keep().join("bucket").join("entry");
            let mut cfg = Cfg::default();
            cfg.backend_config.backend_type = BackendType::Remote;

            let mut block_manager = BlockManager::build(
                path.clone(),
                BlockIndex::new(path.join(BLOCK_INDEX_FILE)),
                "bucket".to_string(),
                "entry".to_string(),
                Arc::new(cfg),
                Default::default(),
            )
            .await
            .unwrap();

            let block_id = 2_000_005;
            block_manager.start_new_block(block_id, 1024).await.unwrap();
            block_manager.save_cache_on_disk().await.unwrap();

            let file = std::fs::File::open(
                block_manager
                    .path
                    .join(format!("{}{}", block_id, DATA_FILE_EXT)),
            )
            .unwrap();
            assert_eq!(file.metadata().unwrap().len(), 0);
        }

        #[rstest]
        #[tokio::test]
        async fn test_loading_block(#[future] block_manager: BlockManager, block_id: u64) {
            let mut block_manager = block_manager.await;
            block_manager.start_new_block(block_id, 1024).await.unwrap();
            let block_ref = block_manager.start_new_block(20000005, 1024).await.unwrap();
            let block = block_ref.read().await.unwrap();
            let loaded_block = block_manager.load_block(block.block_id()).await.unwrap();
            assert_eq!(
                loaded_block.read().await.unwrap().block_id(),
                block.block_id()
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_loading_corrupted_block(
            #[future] block_manager: BlockManager,
            block_id: u64,
        ) {
            let mut block_manager = block_manager.await;
            block_manager.start_new_block(block_id, 1024).await.unwrap();
            block_manager.save_cache_on_disk().await.unwrap();
            block_manager.block_cache.remove(&block_id);

            let path = block_manager.path_to_desc(block_id);
            std::fs::write(&path, b"corrupted").unwrap();

            let err = block_manager.load_block(block_id).await.err().unwrap();
            assert_eq!(err.status(), ErrorCode::InternalServerError);
            assert!(err.to_string().contains("corrupted"));
            assert!(block_manager.index().get_block(block_id).is_some());
            assert!(block_manager.is_block_corrupted(block_id));
        }

        #[rstest]
        #[tokio::test]
        async fn test_load_block_stale_index_self_heals(#[future] block_manager: BlockManager) {
            let mut block_manager = block_manager.await;
            let block_id = 1;

            let block_ref = block_manager.load_block(block_id).await.unwrap();
            block_manager.save_meta_on_disk(block_ref).await.unwrap();
            block_manager.block_cache.remove(&block_id);
            let desc_version = BlockProto::decode(
                std::fs::read(block_manager.path_to_desc(block_id))
                    .unwrap()
                    .as_slice(),
            )
            .unwrap()
            .version
            .unwrap();

            let index_block = block_manager.index_mut().get_block_mut(block_id).unwrap();
            index_block.version = Some(desc_version - 1);
            index_block.crc64 = Some(0);
            block_manager.index_mut().save().await.unwrap();

            let loaded = block_manager.load_block(block_id).await;

            assert!(loaded.is_ok());
            let index_block = block_manager.index().get_block(block_id).unwrap();
            assert_eq!(index_block.version, Some(desc_version));
            assert_ne!(index_block.crc64, Some(0));
        }

        #[rstest]
        #[tokio::test]
        async fn test_load_block_corrupted_descriptor_detected(
            #[future] block_manager: BlockManager,
        ) {
            let mut block_manager = block_manager.await;
            let block_id = 1;
            block_manager.block_cache.remove(&block_id);

            let path = block_manager.path_to_desc(block_id);
            let mut block_proto =
                BlockProto::decode(std::fs::read(&path).unwrap().as_slice()).unwrap();
            block_proto.record_count += 1;
            std::fs::write(&path, block_proto.encode_to_vec()).unwrap();

            let err = block_manager.load_block(block_id).await.err().unwrap();

            assert_eq!(err.status(), ErrorCode::InternalServerError);
            assert!(block_manager.is_block_corrupted(block_id));
        }

        #[rstest]
        #[tokio::test]
        async fn test_mark_block_corrupted_removes_cache(
            #[future] block_manager: BlockManager,
            block_id: u64,
        ) {
            let mut block_manager = block_manager.await;
            block_manager.load_block(block_id).await.unwrap();
            assert!(block_manager.block_cache.get_read(&block_id).is_some());

            block_manager.mark_block_corrupted(block_id).await.unwrap();

            assert!(block_manager.is_block_corrupted(block_id));
            assert!(block_manager.block_cache.get_read(&block_id).is_none());
        }

        #[rstest]
        #[tokio::test]
        async fn test_mark_block_corrupted_persists_in_descriptor(
            #[future] block_manager: BlockManager,
            block_id: u64,
        ) {
            let mut block_manager = block_manager.await;

            block_manager.mark_block_corrupted(block_id).await.unwrap();

            let block_proto = BlockProto::decode(
                std::fs::read(block_manager.path_to_desc(block_id))
                    .unwrap()
                    .as_slice(),
            )
            .unwrap();
            assert_eq!(block_proto.corrupted, Some(true));
        }

        #[rstest]
        #[tokio::test]
        async fn test_recover_being_time_from_id(
            #[future] block_manager: BlockManager,
            block_id: u64,
        ) {
            let mut block_manager = block_manager.await;
            block_manager.start_new_block(block_id, 1024).await.unwrap();
            block_manager.block_cache.remove(&block_id);

            let path = block_manager.path_to_desc(block_id);
            std::fs::write(&path, b"").unwrap();

            let result = block_manager.load_block(block_id).await;
            assert!(
                result.is_ok(),
                "It's ok to recover begin time from block id for blocks which aren't synced yet"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_start_reading(#[future] block_manager: BlockManager, block_id: u64) {
            let mut block_manager = block_manager.await;
            let block = block_manager.start_new_block(block_id, 1024).await.unwrap();
            let block_id = block.read().await.unwrap().block_id();
            let loaded_block = block_manager.load_block(block_id).await.unwrap();
            assert_eq!(loaded_block.read().await.unwrap().block_id(), block_id);
        }

        #[rstest]
        #[tokio::test]
        async fn test_finish_block(#[future] block_manager: BlockManager, block_id: u64) {
            let mut block_manager = block_manager.await;
            let block = block_manager
                .start_new_block(block_id + 1, 1024)
                .await
                .unwrap();
            let block_id = block.read().await.unwrap().block_id();
            let loaded_block = block_manager.load_block(block_id).await.unwrap();
            assert_eq!(loaded_block.read().await.unwrap().block_id(), block_id);

            block_manager.finish_block(loaded_block).await.unwrap();

            let path = block_manager
                .path
                .join(format!("{}{}", block_id, DATA_FILE_EXT));
            for _ in 0..100 {
                if std::fs::metadata(&path).unwrap().len() == 0 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            assert_eq!(std::fs::metadata(path).unwrap().len(), 0);
        }

        #[rstest]
        #[tokio::test]
        async fn test_unfinished_writing(
            #[future] block_manager: BlockManager,
            #[future] block: BlockRef,
            block_id: u64,
        ) {
            let block_manager = block_manager.await;
            let block = block.await;
            let block_manager = Arc::new(AsyncRwLock::new(block_manager));
            let mut writer = RecordWriter::try_new(Arc::clone(&block_manager), block, 0)
                .await
                .unwrap();

            writer.send(Ok(None)).await.unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await; // wait for thread to finish

            let block_ref = block_manager
                .write()
                .await
                .unwrap()
                .load_block(block_id)
                .await
                .unwrap();
            assert_eq!(
                block_ref.read().await.unwrap().get_record(0).unwrap().state,
                2
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_remove_non_existing_block(#[future] block_manager: BlockManager) {
            let mut block_manager = block_manager.await;
            block_manager.remove_block(999999).await.expect("No error");
        }
    }

    mod index_operations {
        use super::*;
        use reduct_base::io::WriteRecord;

        #[rstest]
        #[tokio::test]
        async fn test_update_index_when_start_new_one(
            #[future] block_manager: BlockManager,
            #[future] block: BlockRef,
            block_id: u64,
        ) {
            let block_manager = block_manager.await;
            let block = block.await;
            let block_manager = Arc::new(AsyncRwLock::new(block_manager));
            let record = Record {
                timestamp: Some(Timestamp {
                    seconds: 1,
                    nanos: 0,
                }),
                begin: 0,
                end: 5,
                state: 1,
                labels: vec![],
                content_type: "".to_string(),
            };
            block
                .write()
                .await
                .unwrap()
                .insert_or_update_record(record.clone());

            let mut writer = RecordWriter::try_new(Arc::clone(&block_manager), block, 1000_000)
                .await
                .unwrap();
            writer.send(Ok(Some(Bytes::from("hallo")))).await.unwrap();
            writer.send(Ok(None)).await.unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await; // wait for thread to finish

            // must save record in WAL
            let mut bm = block_manager.write().await.unwrap();
            {
                let entries = bm.wal.read(block_id).await.unwrap();
                assert_eq!(entries.len(), 1);
                let record_from_wall = match &entries[0] {
                    WalEntry::WriteRecord(record) => record,
                    _ => panic!("Expected WriteRecord"),
                };

                assert_eq!(record, *record_from_wall);
            }

            let index = BlockIndex::from_proto(
                PathBuf::new(),
                BlockIndexProto::decode(
                    std::fs::read(bm.path.join(BLOCK_INDEX_FILE))
                        .unwrap()
                        .as_slice(),
                )
                .unwrap(),
            )
            .unwrap();
            assert_eq!(
                index.get_block(block_id).unwrap().record_count,
                1,
                "index not updated"
            );

            // drop cache in disk when block is changed (we need two blocks because of cache)
            let _ = bm.start_new_block(block_id + 1, 1024).await.unwrap();
            let _ = bm.start_new_block(block_id + 2, 1024).await.unwrap();

            let err = bm.wal.read(block_id).await.err().unwrap();
            assert_eq!(
                err.status(),
                ErrorCode::InternalServerError,
                "WAL removed after index updated"
            );

            let index = BlockIndex::from_proto(
                PathBuf::new(),
                BlockIndexProto::decode(
                    std::fs::read(bm.path.join(BLOCK_INDEX_FILE))
                        .unwrap()
                        .as_slice(),
                )
                .unwrap(),
            )
            .unwrap();
            assert_eq!(
                index.get_block(block_id).unwrap().record_count,
                2,
                "index update"
            );

            let er = bm.wal.read(block_id + 1).await.err().unwrap();
            assert_eq!(
                er.status(),
                ErrorCode::InternalServerError,
                "WAL not removed after index updated"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_update_index_when_update_labels(
            #[future] block_manager: BlockManager,
            #[future] block: BlockRef,
            block_id: u64,
        ) {
            let mut bm = block_manager.await;
            let block = block.await;
            let index = BlockIndex::try_load(bm.path.join(BLOCK_INDEX_FILE))
                .await
                .unwrap();
            assert_eq!(
                index.get_block(block_id).unwrap().metadata_size,
                27,
                "index not updated"
            );

            let block_ref = block;
            let record = {
                let lock = block_ref.write().await.unwrap();
                let mut record = lock.get_record(0).unwrap().clone();
                record.labels = vec![Label {
                    name: "key".to_string(),
                    value: "value".to_string(),
                }];

                record
            };

            bm.update_records(block_id, vec![record]).await.unwrap();
            bm.save_cache_on_disk().await.unwrap();
            let block_index_proto = BlockIndexProto::decode(
                std::fs::read(bm.path.join(BLOCK_INDEX_FILE))
                    .unwrap()
                    .as_slice(),
            )
            .unwrap();
            assert_eq!(
                block_index_proto.blocks[0].metadata_size, 41,
                "index updated"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_update_records_decompresses_block(
            #[future] block_manager: BlockManager,
            #[future] block: BlockRef,
            block_id: u64,
        ) {
            let mut bm = block_manager.await;
            let block_ref = block.await;
            bm.compress_block(block_id, CompressionAlgorithm::Zstd)
                .await
                .unwrap();

            let record = {
                let block = block_ref.read().await.unwrap();
                let mut record = block.get_record(0).unwrap().clone();
                record.labels = vec![Label {
                    name: "key".to_string(),
                    value: "value".to_string(),
                }];
                record
            };

            bm.update_records(block_id, vec![record]).await.unwrap();

            assert!(!bm.block_is_compressed(block_id));
            assert!(bm.path_to_data(block_id).exists());
            assert!(bm.path_to_desc(block_id).exists());
            assert!(!bm.path_to_compressed_data(block_id).exists());
            assert!(!bm.path_to_compressed_desc(block_id).exists());
            let block_ref = bm.load_block(block_id).await.unwrap();
            let block = block_ref.read().await.unwrap();
            assert_eq!(block.get_record(0).unwrap().labels[0].value, "value");
        }

        #[rstest]
        #[tokio::test]
        async fn test_update_index_when_remove_block(
            #[future] block_manager: BlockManager,
            block_id: u64,
        ) {
            let mut bm = block_manager.await;
            bm.remove_block(block_id).await.unwrap();

            let index = BlockIndex::try_load(bm.path.join(BLOCK_INDEX_FILE))
                .await
                .unwrap();
            assert!(index.get_block(block_id).is_none(), "index updated");
        }

        #[rstest]
        #[tokio::test]
        async fn test_update_index_when_remove_record(
            #[future] block_manager: BlockManager,
            #[future] block: BlockRef,
            block_id: u64,
        ) {
            let block_manager = block_manager.await;
            let block = block.await;
            let block_manager = Arc::new(AsyncRwLock::new(block_manager));
            write_record(1, 100, &block_manager, block.clone()).await;

            let mut bm = block_manager.write().await.unwrap();
            let index = BlockIndex::try_load(bm.path.join(BLOCK_INDEX_FILE))
                .await
                .unwrap();
            assert_eq!(index.get_block(1).unwrap().record_count, 2);

            bm.remove_records(block_id, vec![1]).await.unwrap();

            let index = BlockIndex::try_load(bm.path.join(BLOCK_INDEX_FILE))
                .await
                .unwrap();
            assert_eq!(index.get_block(1).unwrap().record_count, 1, "index updated");
        }

        #[rstest]
        #[tokio::test]
        async fn test_recovering_index_if_no_meta_file(
            #[future] block_manager: BlockManager,
            block_id: u64,
        ) {
            let mut block_manager = block_manager.await;
            assert!(block_manager.index().get_block(block_id).is_some());

            FILE_CACHE
                .remove(&block_manager.path_to_desc(block_id))
                .await
                .unwrap();
            block_manager.block_cache.remove(&block_id); // remove block from cache to load it from disk
            assert_eq!(
                block_manager
                    .load_block(block_id)
                    .await
                    .err()
                    .unwrap()
                    .status(),
                ErrorCode::InternalServerError
            );
            assert!(
                block_manager.index().get_block(block_id).is_some(),
                "corrupted blocks remain in the index for quota cleanup"
            );
            assert!(block_manager.is_block_corrupted(block_id));
        }
    }

    mod record_removing {
        use super::*;
        use crate::storage::entry::RecordReader;
        use reduct_base::io::ReadRecord;

        #[rstest]
        #[case(0)]
        #[case(500)]
        #[case(MAX_IO_BUFFER_SIZE+1)]
        #[tokio::test(flavor = "multi_thread")]
        async fn test_remove_records(
            #[case] record_size: usize,
            #[future] block_manager: BlockManager,
            #[future] block: BlockRef,
            block_id: u64,
        ) {
            let block_manager = block_manager.await;
            let block = block.await;
            let block_manager = Arc::new(AsyncRwLock::new(block_manager));
            let block_ref = block;
            let (record, record_body) =
                write_record(1, record_size, &block_manager, block_ref.clone()).await;

            // remove first record
            block_manager
                .write()
                .await
                .unwrap()
                .remove_records(block_id, vec![0])
                .await
                .unwrap();

            let block = block_ref.read().await.unwrap();
            assert_eq!(block.record_count(), 1);

            let record_time = ts_to_us(&record.timestamp.unwrap());
            let record = block.get_record(record_time).unwrap();
            assert_eq!(ts_to_us(&record.timestamp.unwrap()), record_time);
            assert_eq!(record.begin, 0);
            assert_eq!(record.end, record_size as u64);
            assert_eq!(record.state, 1);

            // read content
            let mut reader = RecordReader::try_new(
                Arc::clone(&block_manager),
                block_ref.clone(),
                record_time,
                None,
                None,
            )
            .await
            .unwrap();

            let mut received = BytesMut::new();
            while let Some(Ok(chunk)) = reader.read_chunk() {
                received.extend_from_slice(&chunk);
            }
            assert_eq!(received.len(), record_size);
            assert_eq!(received, record_body.as_bytes());

            assert!(
                block_manager
                    .write()
                    .await
                    .unwrap()
                    .wal
                    .read(block.block_id())
                    .await
                    .is_err(),
                "wal must be removed after successful update"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_remove_only_one_record(#[future] block_manager: BlockManager, block_id: u64) {
            let mut block_manager = block_manager.await;
            block_manager
                .remove_records(block_id, vec![0])
                .await
                .unwrap();

            // block must be removed
            let err = block_manager.load_block(block_id).await.err().unwrap();
            assert_eq!(err.status(), ErrorCode::InternalServerError);
            assert!(block_manager.block_index.get_block(block_id).is_none());
            assert!(!block_manager.exist(block_id).await.unwrap());
        }

        #[rstest]
        #[tokio::test]
        async fn test_remove_records_wal(
            #[future] block_manager: BlockManager,
            #[future] block: BlockRef,
        ) {
            let block_manager = block_manager.await;
            let block = block.await;
            let block_manager = Arc::new(AsyncRwLock::new(block_manager));
            write_record(1, 5, &block_manager, block.clone()).await;

            let mut bm = block_manager.write().await.unwrap();

            let block_id = block.read().await.unwrap().block_id();
            FILE_CACHE.remove(&bm.path_to_data(block_id)).await.unwrap();

            let res = bm.remove_records(block_id, vec![1]).await;
            assert!(
                res.is_err(),
                "we broke the method removing the source block"
            );

            let entries = bm.wal.read(block_id).await.unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(
                entries[0],
                WalEntry::RemoveRecord(1),
                "wal must have the record"
            );
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn test_remove_records_decompresses_block(
            #[future] block_manager: BlockManager,
            #[future] block: BlockRef,
            block_id: u64,
        ) {
            let block_manager = block_manager.await;
            let block = block.await;
            let block_manager = Arc::new(AsyncRwLock::new(block_manager));
            write_record(1, 10, &block_manager, block.clone()).await;

            {
                let mut bm = block_manager.write().await.unwrap();
                bm.compress_block(block_id, CompressionAlgorithm::Zstd)
                    .await
                    .unwrap();
                bm.remove_records(block_id, vec![0]).await.unwrap();

                assert!(!bm.block_is_compressed(block_id));
                assert!(bm.path_to_data(block_id).exists());
                assert!(bm.path_to_desc(block_id).exists());
                assert!(!bm.path_to_compressed_data(block_id).exists());
                assert!(!bm.path_to_compressed_desc(block_id).exists());
            }

            let block = block.read().await.unwrap();
            assert_eq!(block.record_count(), 1);
            assert!(block.get_record(1).is_some());
        }
    }

    async fn write_record(
        record_time: u64,
        record_size: usize,
        block_manager: &Arc<AsyncRwLock<BlockManager>>,
        block_ref: BlockRef,
    ) -> (Record, String) {
        let block_size = block_ref.read().await.unwrap().size();
        let record = Record {
            timestamp: Some(us_to_ts(&record_time)),
            begin: block_size,
            end: (record_size as u64 + block_size),
            state: 1,
            labels: vec![],
            content_type: "".to_string(),
        };
        block_ref
            .write()
            .await
            .unwrap()
            .insert_or_update_record(record.clone());

        let record_body: String = rng()
            .sample_iter(&Alphanumeric)
            .take(record_size)
            .map(char::from)
            .collect();

        let block_copy = block_ref.clone();
        let body_copy = record_body.clone();
        let bm_copy = Arc::clone(block_manager);
        let mut writer = RecordWriter::try_new(bm_copy, block_copy, record_time)
            .await
            .unwrap();
        writer.send(Ok(Some(Bytes::from(body_copy)))).await.unwrap();
        writer.send(Ok(None)).await.unwrap();

        tokio::time::sleep(Duration::from_millis(10)).await; // wait for thread to finish
        block_manager
            .write()
            .await
            .unwrap()
            .save_meta_on_disk(block_ref.clone())
            .await
            .unwrap();
        (record, record_body)
    }
    #[fixture]
    fn block_id() -> u64 {
        1
    }

    #[fixture]
    async fn block(#[future] block_manager: BlockManager, block_id: u64) -> BlockRef {
        let mut block_manager = block_manager.await;
        block_manager.load_block(block_id).await.unwrap()
    }

    #[fixture]
    async fn block_manager(block_id: u64) -> BlockManager {
        let path = tempdir().unwrap().keep().join("bucket").join("entry");

        let mut bm = BlockManager::build(
            path.clone(),
            BlockIndex::new(path.join(BLOCK_INDEX_FILE)),
            "bucket".to_string(),
            "entry".to_string(),
            Cfg::default().into(),
            Default::default(),
        )
        .await
        .unwrap();
        let block_ref = bm.start_new_block(block_id, 1024).await.unwrap().clone();

        let mut block = block_ref.write().await.unwrap();
        block.insert_or_update_record(Record {
            timestamp: Some(Timestamp {
                seconds: 0,
                nanos: 0,
            }),
            begin: 0,
            end: (MAX_IO_BUFFER_SIZE + 1) as u64,
            state: 0,
            labels: vec![],
            content_type: "".to_string(),
        });

        let (file, offset) = bm.begin_write_record(&block, 0).unwrap();
        drop(block);

        FILE_CACHE
            .write_or_create(&file, SeekFrom::Start(offset))
            .await
            .unwrap()
            .write(&vec![0; MAX_IO_BUFFER_SIZE + 1])
            .unwrap();

        bm.finish_write_record(block_id, record::State::Finished, 0)
            .await
            .unwrap();
        bm.save_meta_on_disk(block_ref).await.unwrap();
        bm
    }
}
