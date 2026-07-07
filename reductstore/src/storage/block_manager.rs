// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

mod accessors;
pub(in crate::storage) mod block;
mod block_cache;
pub(in crate::storage) mod block_index;
mod cache;
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
mod test_utils;
