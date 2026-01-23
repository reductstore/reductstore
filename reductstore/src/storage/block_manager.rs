// Copyright 2023-2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

pub(in crate::storage) mod block;
mod block_cache;
pub(in crate::storage) mod block_index;
mod read_only;
pub(in crate::storage) mod wal;

use crate::cfg::{Cfg, InstanceRole};
use crate::core::file_cache::FILE_CACHE;
use crate::core::sync::AsyncRwLock;
use crate::storage::block_manager::block::Block;
use crate::storage::block_manager::block_cache::BlockCache;
use crate::storage::block_manager::wal::{create_wal, Wal, WalEntry};
use crate::storage::entry::io::record_reader::read_in_chunks;
use crate::storage::proto::{record, ts_to_us, us_to_ts, Block as BlockProto, Record};
use block_index::BlockIndex;
use crc64fast::Digest;
use log::{debug, error, info, trace, warn};
use prost::bytes::{Bytes, BytesMut};
use prost::Message;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
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
    wal: Box<dyn Wal + Sync + Send>,
    cfg: Arc<Cfg>,
    last_replica_sync: Instant,
}

pub const DESCRIPTOR_FILE_EXT: &str = ".meta";
pub const DATA_FILE_EXT: &str = ".blk";
pub const BLOCK_INDEX_FILE: &str = "blocks.idx";

const WRITE_BLOCK_CACHE_SIZE: usize = 2; // we need 2 to avoid double sync when start a new one but not yet saved the old one when the record is written
const READ_BLOCK_CACHE_SIZE: usize = 16;

impl BlockManager {
    /// Create a new block manager.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the block manager directory.
    /// * `index` - Block index to use.
    /// * `cfg` - Configuration.
    pub(crate) async fn build(path: PathBuf, index: BlockIndex, cfg: Arc<Cfg>) -> Self {
        let (bucket, entry) = {
            let mut parts = path.iter().rev();
            let entry = parts.next().unwrap().to_str().unwrap().to_string();
            let bucket = parts.next().unwrap().to_str().unwrap().to_string();
            (bucket, entry)
        };

        Self {
            path: path.clone(),
            bucket,
            entry,
            block_index: index,
            block_cache: BlockCache::new(
                WRITE_BLOCK_CACHE_SIZE,
                READ_BLOCK_CACHE_SIZE,
                Duration::from_secs(30),
            ),
            wal: create_wal(path.clone()).await,
            cfg,
            last_replica_sync: Instant::now(),
        }
    }

    pub async fn save_cache_on_disk(&mut self) -> Result<(), ReductError> {
        for block in self.block_cache.write_values() {
            self.save_block_on_disk(block).await?;
        }

        Ok(())
    }

    pub async fn find_block(&mut self, start: u64) -> Result<BlockRef, ReductError> {
        self.update_and_get_index().await?;

        let start_block_id = self.block_index.tree().range(start..).next();
        let id = if start_block_id.is_some() && start >= *start_block_id.unwrap() {
            start_block_id.unwrap().clone()
        } else {
            if let Some(block_id) = self.block_index.tree().range(..start).rev().next() {
                block_id.clone()
            } else {
                return Err(ReductError::not_found(&format!(
                    "Record {} not found in entry {}/{}",
                    start, self.bucket, self.entry
                )));
            }
        };

        self.load_block(id).await
    }

    pub async fn load_block(&mut self, block_id: u64) -> Result<BlockRef, ReductError> {
        // first check if we have the block in write cache
        let mut cached_block = self.block_cache.get_read(&block_id);
        if cached_block.is_none() {
            let path = self.path_to_desc(block_id);
            let buf = match FILE_CACHE.read(&path, SeekFrom::Start(0)).await {
                Ok(mut file) => {
                    let mut buf = vec![];
                    file.read_to_end(&mut buf)?;
                    buf
                }
                Err(err) => {
                    // here we can't read the block descriptor, it might be corrupted or not exist
                    // we should remove it from the index
                    let err_msg = format!("Block descriptor {:?} can't be read: {}", path, err);
                    error!("{}", &err_msg);
                    info!(
                        "Remove block {} from the index. The block {} must be removed manually",
                        block_id,
                        self.path_to_data(block_id).display()
                    );
                    self.block_index.remove_block(block_id);
                    self.block_index.save().await?;
                    return Err(internal_server_error!(&err_msg));
                }
            };

            // calculate crc of the block descriptor
            let mut crc = Digest::new();
            crc.write(&buf);

            if let Some(block) = self.block_index.get_block(block_id) {
                if let Some(block_crc) = block.crc64 {
                    // we check crc if the crc is stored in the index for backward compatibility
                    if block_crc != crc.sum64() {
                        error!("Block descriptor {:?} is corrupted: index CRC {} mismatch with calculated CRC {}.\
                     Remove it and its data block, then restart the database", path, block_crc, crc.sum64());

                        self.block_index.remove_block(block_id);
                        return Err(internal_server_error!(
                            "Block descriptor {:?} is corrupted",
                            path
                        ));
                    }
                }
            } else {
                return Err(internal_server_error!(
                    "Block descriptor {:?} is not in the index",
                    path
                ));
            }

            // parse the block descriptor
            let mut block_from_disk = BlockProto::decode(Bytes::from(buf)).map_err(|e| {
                internal_server_error!("Failed to decode block descriptor {:?}: {}", path, e)
            })?;

            if block_from_disk.begin_time.is_none() {
                warn!(
                    "Block descriptor {:?} has no begin time. It might be recovered from the WAL",
                    path
                );
                block_from_disk.begin_time = Some(us_to_ts(&block_id));
            }

            cached_block = Some(Arc::new(AsyncRwLock::new(block_from_disk.into())));
        }

        let cached_block = cached_block.unwrap();
        self.block_cache.insert_read(block_id, cached_block.clone());
        Ok(cached_block)
    }

    pub async fn save_block(&mut self, block: BlockRef) -> Result<(), ReductError> {
        let id = block.read().await?.block_id();
        for (_, block) in self.block_cache.insert_write(id, block.clone()) {
            self.save_block_on_disk(block).await?;
        }

        Ok(())
    }

    pub async fn start_new_block(
        &mut self,
        block_id: u64,
        max_block_size: u64,
    ) -> Result<BlockRef, ReductError> {
        let block = Block::new(block_id);

        // create a block with data
        {
            let mut file = FILE_CACHE
                .write_or_create(&self.path_to_data(block_id), SeekFrom::Start(0))
                .await?;
            file.set_len(max_block_size)?;
        }

        self.block_index.insert_or_update(block.clone());

        let block_ref = Arc::new(AsyncRwLock::new(block));
        self.save_block(block_ref.clone()).await?;
        Ok(block_ref)
    }

    /// Finish writing a record to a block.
    ///
    /// This method will shrink the block data file to the record size and sync the descriptor and data files.
    ///
    /// # Arguments
    ///
    /// * `block` - Block to finish writing to.
    ///
    /// # Errors
    ///
    /// * `ReductError` - If file system operation failed.
    pub async fn finish_block(&mut self, block: BlockRef) -> Result<(), ReductError> {
        let (block_id, block_size) = {
            let block = block.read().await?;
            (block.block_id(), block.size())
        };
        /* resize data block then sync descriptor and data */
        let path = self.path_to_data(block_id);
        {
            let mut data_block = FILE_CACHE
                .write_or_create(&path, SeekFrom::Current(0))
                .await?;
            data_block.set_len(block_size)?;
            data_block.sync_all().await?;
        }

        {
            let mut descr_block = FILE_CACHE
                .write_or_create(&self.path_to_desc(block_id), SeekFrom::Current(0))
                .await?;
            descr_block.sync_all().await?;
        }

        Ok(())
    }

    /// Remove a block from file system.
    ///
    /// This method will sync the block descriptor and data files, remove the block from the cache and index.
    ///
    /// # Arguments
    ///
    /// * `block_id` - ID of the block to remove.
    ///
    /// # Errors
    ///
    /// * `ReductError` - If the block is still in use or file system operation failed.
    pub async fn remove_block(&mut self, block_id: u64) -> Result<(), ReductError> {
        self.wal.append(block_id, WalEntry::RemoveBlock).await?;

        let data_block_path = self.path_to_data(block_id);
        if FILE_CACHE.try_exists(&data_block_path).await? {
            // it can be still in WAL only
            FILE_CACHE.remove(&data_block_path).await?;
        }

        let desc_block_path = self.path_to_desc(block_id);
        if FILE_CACHE.try_exists(&desc_block_path).await? {
            // it can be still in WAL only
            FILE_CACHE.remove(&desc_block_path).await?;
        }

        self.block_index.remove_block(block_id);
        self.block_index.save().await?;

        self.block_cache.remove(&block_id);

        self.wal.remove(block_id).await?;
        Ok(())
    }

    /// Check if a block exists on disk.
    pub async fn exist(&self, block_id: u64) -> Result<bool, ReductError> {
        let path = self.path_to_desc(block_id);
        Ok(FILE_CACHE.try_exists(&path).await?)
    }

    /// Update records in a block and save it on disk.
    ///
    /// # Arguments
    ///
    /// * `block_id` - Block to update records in.
    /// * `records` - Records to update.
    ///
    ///
    /// # Panics
    ///
    /// * If the record is not in the block.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - If the records were updated successfully.
    ///
    /// # Errors
    ///
    /// * `ReductError` - If failed to append to WAL or save the block on disk.
    pub async fn update_records(
        &mut self,
        block_id: u64,
        records: Vec<Record>,
    ) -> Result<(), ReductError> {
        let block_ref = self.load_block(block_id).await?;

        // First, append all WAL entries
        for record in records.iter() {
            self.wal
                .append(block_id, WalEntry::UpdateRecord(record.clone()))
                .await?;
        }

        // Then, update the block in-memory (no await needed here)
        {
            let mut block = block_ref.write().await?;
            for record in records.into_iter() {
                block.insert_or_update_record(record);
            }
        }

        self.save_block(block_ref).await
    }

    /// Remove records from a block and save it on disk.
    ///
    /// The method will create a temporary block file and copy the retained records to it.
    /// In the end, the original block file will be replaced with the temporary one and the block descriptor will be updated.
    ///
    /// # Arguments
    ///
    /// * `block_id` - Block to remove records from.
    /// * `records` - Record timestamps to remove.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - If the records were removed successfully.
    pub async fn remove_records(
        &mut self,
        block_id: u64,
        records: Vec<u64>,
    ) -> Result<(), ReductError> {
        let block_ref = self.load_block(block_id).await?;

        {
            let mut block = block_ref.write().await?;
            for record_time in records {
                block.remove_record(record_time);
                self.wal
                    .append(block_id, WalEntry::RemoveRecord(record_time))
                    .await?;
            }

            // if the block is empty, remove it
            if block.record_count() == 0 {
                let block_id = block.block_id();
                drop(block);
                return self.remove_block(block_id).await;
            }
        }

        // Collect record info needed for copying
        let (temp_block_path, record_info, src_block_path) = {
            let block = block_ref.read().await?;
            let temp_block_path = self.path.join(format!("{}.blk.tmp", block.block_id()));
            let src_block_path = self.path_to_data(block.block_id());

            // Collect record positions
            let record_info: Vec<(u64, u64, u64)> = block
                .record_index()
                .values()
                .map(|record| {
                    let record_time = ts_to_us(&record.timestamp.unwrap());
                    (record_time, record.begin, record.end - record.begin)
                })
                .collect();

            (temp_block_path, record_info, src_block_path)
        };

        // create a temporary block file outside of the cache to avoid unnecessary evictions
        let mut temp_block = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&temp_block_path)
            .map_err(|e| {
                internal_server_error!(
                    "Failed to create temporary block file {:?}: {}",
                    temp_block_path,
                    e
                )
            })?;

        // Copy records to temp file and track new positions
        let mut new_positions: Vec<(u64, u64, u64)> = Vec::new();
        let mut total_offset = 0u64;

        for (record_time, begin, record_size) in record_info {
            let mut read_bytes = 0;
            while read_bytes < record_size {
                let (buf, read) =
                    read_in_chunks(&src_block_path, begin, record_size, read_bytes).await?;

                read_bytes += read as u64;
                temp_block.write_all(&buf)?;
            }

            let new_begin = total_offset;
            total_offset += read_bytes;
            let new_end = total_offset;

            new_positions.push((record_time, new_begin, new_end));

            trace!(
                "Record {}/{} retained with new position begin={}, end={}",
                block_id,
                record_time,
                new_begin,
                new_end
            );
        }

        // Update record positions in the block
        {
            let mut block = block_ref.write().await?;
            for (record_time, new_begin, new_end) in new_positions {
                if let Some(record) = block.record_index_mut().get_mut(&record_time) {
                    record.begin = new_begin;
                    record.end = new_end;
                }
            }

            debug!(
                "New block {:?} is created with {} records",
                temp_block_path,
                block.record_count()
            );
        }

        // Replace the old block file with the new one
        let block_path = {
            let block = block_ref.read().await?;
            self.path_to_data(block.block_id())
        };

        tokio::fs::remove_file(&block_path).await?;
        tokio::fs::rename(&temp_block_path, &block_path).await?;

        FILE_CACHE.discard_recursive(&block_path).await?;
        let mut block_file = FILE_CACHE
            .write_or_create(&block_path, SeekFrom::Start(0))
            .await?;
        block_file.sync_all().await?;

        self.save_block_on_disk(block_ref).await
    }

    /// Begin writing a record to a block.
    ///
    /// # Arguments
    ///
    /// * `block` - Block to write to.
    /// * `record_timestamp` - Timestamp of the record to write.
    ///
    /// # Returns
    ///
    /// * `Ok(file)` - File to write to.
    pub fn begin_write_record(
        &self,
        block: &Block,
        record_timestamp: u64,
    ) -> Result<(PathBuf, u64), ReductError> {
        let path = self.path_to_data(block.block_id());
        let offset = block.get_record(record_timestamp).unwrap().begin;
        Ok((path, offset))
    }

    /// Finish writing a record to a block.
    ///
    /// This method will update the record state, add WAL entry and save the block on disk if needed.
    ///
    /// # Arguments
    ///
    /// * `block_id` - ID of the block to write to.
    /// * `state` - State of the record.
    ///
    /// # Errors
    ///
    /// * `ReductError` - If file system operation failed.
    pub async fn finish_write_record(
        &mut self,
        block_id: u64,
        state: record::State,
        record_timestamp: u64,
    ) -> Result<(), ReductError> {
        // check if the block is still in cache
        let block_ref = if let Some(block_ref) = self.block_cache.get_write(&block_id) {
            let block = block_ref.read().await?;
            if block.block_id() == block_id {
                block_ref.clone()
            } else {
                self.load_block(block_id).await?
            }
        } else {
            self.load_block(block_id).await?
        };

        // Get the record for WAL entry before modifying
        let wal_record = {
            let mut block = block_ref.write().await?;
            block.change_record_state(record_timestamp, i32::from(state))?;
            block.get_record(record_timestamp).unwrap().clone()
        };

        // write to WAL (no guard held)
        self.wal
            .append(block_id, WalEntry::WriteRecord(wal_record))
            .await?;

        self.save_block(block_ref).await?;

        debug!(
            "Finished writing record {} to block {}/{}/{}.meta with state {:?}",
            record_timestamp, self.bucket, self.entry, block_id, state
        );

        Ok(())
    }

    /// Begin reading a record from a block.
    ///
    /// # Arguments
    ///
    /// * `block` - Block to read from.
    /// * `record_timestamp` - Timestamp of the record to read.
    ///
    /// # Returns
    ///
    /// * `Ok(file, offset)` - File to read from and offset to start reading.
    ///
    /// # Errors
    ///
    /// * `ReductError` - If file system operation failed.
    pub(crate) fn begin_read_record(
        &self,
        block: &Block,
        record_timestamp: u64,
    ) -> Result<(PathBuf, u64), ReductError> {
        let path = self.path_to_data(block.block_id());
        let offset = block.get_record(record_timestamp).unwrap().begin;
        Ok((path, offset))
    }

    pub fn index_mut(&mut self) -> &mut BlockIndex {
        &mut self.block_index
    }

    pub fn index(&self) -> &BlockIndex {
        &self.block_index
    }

    pub async fn update_and_get_index(&mut self) -> Result<&BlockIndex, ReductError> {
        self.reload_if_readonly().await?;
        Ok(&self.block_index)
    }

    pub fn bucket_name(&self) -> &String {
        &self.bucket
    }

    pub fn entry_name(&self) -> &String {
        &self.entry
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    fn path_to_desc(&self, block_id: u64) -> PathBuf {
        self.path
            .join(format!("{}{}", block_id, DESCRIPTOR_FILE_EXT))
    }

    fn path_to_data(&self, block_id: u64) -> PathBuf {
        self.path.join(format!("{}{}", block_id, DATA_FILE_EXT))
    }

    async fn save_block_on_disk(&mut self, block_ref: BlockRef) -> Result<(), ReductError> {
        // Take a snapshot under a short-lived write lock to avoid blocking readers
        let (block_id, block_snapshot) = {
            let block = block_ref.read().await?;
            (block.block_id(), block.to_owned())
        };

        debug!(
            "Saving block {}/{}/{} on disk and updating index",
            self.bucket, self.entry, block_id
        );

        let path = self.path_to_desc(block_id);
        let mut buf = BytesMut::new();

        let mut proto = BlockProto::from(block_snapshot);
        proto.encode(&mut buf).map_err(|e| {
            internal_server_error!("Failed to encode block descriptor {:?}: {}", path, e)
        })?;
        let len = buf.len() as u64;

        trace!("Writing block descriptor {:?}", path);

        if self.cfg.role != InstanceRole::Replica {
            let mut lock = FILE_CACHE
                .write_or_create(&path, SeekFrom::Start(0))
                .await?;
            lock.set_len(len)?;
            lock.write_all(&buf)?;
            lock.sync_all().await?; // fix https://github.com/reductstore/reductstore/issues/642
        }

        trace!("Updating block index");
        // update index with block crc
        let mut crc = Digest::new();
        crc.write(&buf);
        proto.metadata_size = len; // update metadata size because it changed
        self.block_index
            .insert_or_update_with_crc(proto, crc.sum64());

        if self.cfg.role != InstanceRole::Replica {
            self.block_index.save().await?;

            trace!("Block {}/{}/{} saved", self.bucket, self.entry, block_id);
            // clean WAL
            self.wal.remove(block_id).await?;
        }
        Ok(())
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
    use rand::{rng, Rng};
    use reduct_base::io::WriteRecord;
    use std::time::Duration;
    use tempfile::tempdir;

    mod block_operations {
        use super::*;
        use reduct_base::io::WriteRecord;

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
                25,
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
                block_index_proto.blocks[0].metadata_size, 39,
                "index updated"
            );
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
                block_manager.index().get_block(block_id).is_none(),
                "we removed the block descriptor file"
            );
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
            .save_block_on_disk(block_ref.clone())
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
            Cfg::default().into(),
        )
        .await;
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
        bm.save_block_on_disk(block_ref).await.unwrap();
        bm
    }
}
