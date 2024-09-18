// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

pub(in crate::storage) mod block;
pub(in crate::storage) mod block_index;
pub(in crate::storage) mod wal;

mod block_cache;
mod use_counter;
use log::{debug, trace};
use prost::bytes::{Bytes, BytesMut};
use prost::Message;

use crate::storage::block_manager::block::Block;
use crate::storage::block_manager::block_cache::BlockCache;
use crate::storage::block_manager::use_counter::UseCounter;
use crate::storage::block_manager::wal::{Wal, WalEntry};
use crate::storage::entry::io::record_reader::read_in_chunks;
use crate::storage::file_cache::{FileWeak, FILE_CACHE};
use crate::storage::proto::{record, ts_to_us, Block as BlockProto, Record};
use crate::storage::storage::IO_OPERATION_TIMEOUT;
use block_index::BlockIndex;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::io::{Read, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc::{Receiver, Sender};

pub(crate) type BlockRef = Arc<RwLock<Block>>;

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
}

pub const DESCRIPTOR_FILE_EXT: &str = ".meta";
pub const DATA_FILE_EXT: &str = ".blk";
pub const BLOCK_INDEX_FILE: &str = "blocks.idx";

impl BlockManager {
    /// Create a new block manager.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the block manager directory.
    /// * `index` - Block index to use.
    pub(crate) fn new(path: PathBuf, index: BlockIndex) -> Self {
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
            block_cache: BlockCache::new(1, 64, IO_OPERATION_TIMEOUT),
            wal: wal::create_wal(path.clone()),
        }
    }

    pub fn save_cache_on_disk(&mut self) -> Result<(), ReductError> {
        if self.block_cache.write_len() == 0 {
            return Ok(());
        }

        for block in self.block_cache.write_values() {
            self.save_block_on_disk(block)?;
        }

        Ok(())
    }

    pub fn find_block(&self, start: u64) -> Result<BlockRef, ReductError> {
        let start_block_id = self.block_index.tree().range(start..).next();
        let id = if start_block_id.is_some() && start >= *start_block_id.unwrap() {
            start_block_id.unwrap().clone()
        } else {
            if let Some(block_id) = self.block_index.tree().range(..start).rev().next() {
                block_id.clone()
            } else {
                return Err(ReductError::not_found(&format!(
                    "No record with timestamp {}",
                    start
                )));
            }
        };

        self.load_block(id)
    }

    pub fn load_block(&self, block_id: u64) -> Result<BlockRef, ReductError> {
        // first check if we have the block in write cache
        let mut cached_block = self.block_cache.get_read(&block_id);
        if cached_block.is_none() {
            let path = self.path_to_desc(block_id);
            let file = FILE_CACHE.read(&path, SeekFrom::Start(0))?.upgrade()?;
            let mut buf = vec![];

            // parse the block descriptor
            let mut lock = file.write()?;
            lock.read_to_end(&mut buf)?;

            let block_from_disk = BlockProto::decode(Bytes::from(buf)).map_err(|e| {
                internal_server_error!("Failed to decode block descriptor {:?}: {}", path, e)
            })?;
            cached_block = Some(Arc::new(RwLock::new(block_from_disk.into())));
        }

        let cached_block = cached_block.unwrap();
        self.block_cache.insert_read(block_id, cached_block.clone());
        Ok(cached_block)
    }

    pub fn save_block(&mut self, block: BlockRef) -> Result<(), ReductError> {
        // save the current block in cache and write on the disk the evicted one
        for (_, block) in self
            .block_cache
            .insert_write(block.read()?.block_id(), block.clone())
        {
            self.save_block_on_disk(block)?;
        }

        Ok(())
    }

    pub fn start_new_block(
        &mut self,
        block_id: u64,
        max_block_size: u64,
    ) -> Result<BlockRef, ReductError> {
        let block = Block::new(block_id);

        // create a block with data
        {
            let file = FILE_CACHE
                .write_or_create(&self.path_to_data(block_id), SeekFrom::Start(0))?
                .upgrade()?;
            let file = file.write()?;
            file.set_len(max_block_size)?;
        }

        self.block_index.insert_or_update(block.clone());

        let block_ref = Arc::new(RwLock::new(block));
        self.save_block(block_ref.clone())?;
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
    pub fn finish_block(&mut self, block: BlockRef) -> Result<(), ReductError> {
        let block = block.read()?;
        /* resize data block then sync descriptor and data */
        let path = self.path_to_data(block.block_id());
        let file = FILE_CACHE
            .write_or_create(&path, SeekFrom::Current(0))?
            .upgrade()?;
        let data_block = file.write()?;
        data_block.set_len(block.size())?;
        data_block.sync_all()?;

        let file = FILE_CACHE
            .write_or_create(&self.path_to_desc(block.block_id()), SeekFrom::Current(0))?
            .upgrade()?;
        let descr_block = file.write()?;
        descr_block.sync_all()?;

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
    pub fn remove_block(&mut self, block_id: u64) -> Result<(), ReductError> {
        self.wal.append(block_id, WalEntry::RemoveBlock)?;

        self.save_block_on_disk(self.load_block(block_id)?)?;
        self.block_cache.remove(&block_id);

        let path = self.path_to_data(block_id);
        FILE_CACHE.remove(&path)?;

        let path = self.path_to_desc(block_id);
        FILE_CACHE.remove(&path)?;

        self.block_index.remove_block(block_id);
        self.block_index.save()?;

        self.wal.remove(block_id)?;
        Ok(())
    }

    /// Check if a block exists on disk.
    pub fn exist(&self, block_id: u64) -> Result<bool, ReductError> {
        let path = self.path_to_desc(block_id);
        Ok(path.try_exists()?)
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
    pub fn update_records(
        &mut self,
        block_id: u64,
        records: Vec<Record>,
    ) -> Result<(), ReductError> {
        let block_ref = self.load_block(block_id)?;

        {
            let mut block = block_ref.write()?;

            for record in records.into_iter() {
                self.wal
                    .append(block.block_id(), WalEntry::UpdateRecord(record.clone()))?;
                block.insert_or_update_record(record);
            }
        }

        self.save_block_on_disk(block_ref)
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
    pub fn remove_records(&mut self, block_id: u64, records: Vec<u64>) -> Result<(), ReductError> {
        let block_ref = self.load_block(block_id)?;
        {
            let mut block = block_ref.write()?;
            for record_time in records {
                block.remove_record(record_time);

                self.wal
                    .append(block.block_id(), WalEntry::RemoveRecord(record_time))?;
            }

            // if the block is empty, remove it
            if block.record_count() == 0 {
                let block_id = block.block_id();
                drop(block); // drop the lock before calling remove_block to avoid deadlock
                return self.remove_block(block_id);
            }
        }

        let temp_block_path = {
            let mut block = block_ref.write()?;
            let temp_block_path = self.path.join(format!("{}.blk.tmp", block.block_id()));
            let temp_block_ref = FILE_CACHE
                .write_or_create(&temp_block_path, SeekFrom::Start(0))?
                .upgrade()?;
            let mut temp_block = temp_block_ref.write()?;
            temp_block.set_len(block.size())?;

            let mut total_offset = 0;
            let block_id = block.block_id();
            for record in block.record_index_mut().values_mut() {
                let record_time = ts_to_us(&record.timestamp.unwrap());

                let (file, offset) = {
                    let path = self.path_to_data(block_id);
                    let offset = record.begin;
                    let file = FILE_CACHE.read(&path, SeekFrom::Start(offset))?;
                    (file, offset)
                };

                let mut read_bytes = 0;
                let record_size = record.end - record.begin;
                while read_bytes < record_size {
                    let (buf, read) = read_in_chunks(&file, offset, record_size, read_bytes)?;
                    if read == 0 {
                        return Err(internal_server_error!("Failed to read record chunk: EOF"));
                    }

                    read_bytes += read as u64;
                    temp_block.write_all(&buf)?;
                }

                // Set position of content in the new block
                total_offset += read_bytes;
                record.begin = total_offset - read_bytes;
                record.end = total_offset;

                trace!(
                    "Record {}/{} retained with new position begin={}, end={}",
                    block_id,
                    record_time,
                    record.begin,
                    record.end
                );
            }

            debug!(
                "New block {:?} is created with {} records",
                temp_block_path,
                block.record_count()
            );

            temp_block_path
        };

        {
            let block = block_ref.read()?;

            FILE_CACHE.remove(&self.path_to_data(block.block_id()))?;
            FILE_CACHE.rename(&temp_block_path, &self.path_to_data(block.block_id()))?;

            debug!(
                "Block {:?} is replaced with retained records",
                self.path_to_data(block.block_id())
            );
        }

        self.save_block_on_disk(block_ref)
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
    ) -> Result<(FileWeak, u64), ReductError> {
        let path = self.path_to_data(block.block_id());
        let offset = block.get_record(record_timestamp).unwrap().begin;
        let file = FILE_CACHE.write_or_create(&path, SeekFrom::Start(offset))?;
        Ok((file, offset))
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
    pub(crate) fn finish_write_record(
        &mut self,
        block_id: u64,
        state: record::State,
        record_timestamp: u64,
    ) -> Result<(), ReductError> {
        // check if the block is still in cache
        let block_ref = if let Some(block_ref) = self.block_cache.get_write(&block_id) {
            let block = block_ref.read()?;
            if block.block_id() == block_id {
                block_ref.clone()
            } else {
                self.load_block(block_id)?
            }
        } else {
            self.load_block(block_id)?
        };

        {
            let mut block = block_ref.write()?;
            block.change_record_state(record_timestamp, i32::from(state))?;

            // write to WAL
            self.wal.append(
                block_id,
                WalEntry::WriteRecord(block.get_record(record_timestamp).unwrap().clone()),
            )?;
        }

        self.save_block(block_ref)?;

        debug!(
            "Finished writing record {}/{}/{} with state {:?}",
            self.bucket, self.entry, block_id, state
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
    ) -> Result<(FileWeak, u64), ReductError> {
        let path = self.path_to_data(block.block_id());
        let offset = block.get_record(record_timestamp).unwrap().begin;
        let file = FILE_CACHE.read(&path, SeekFrom::Start(offset))?;
        Ok((file, offset))
    }

    pub fn index_mut(&mut self) -> &mut BlockIndex {
        &mut self.block_index
    }

    pub fn index(&self) -> &BlockIndex {
        &self.block_index
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

    fn save_block_on_disk(&mut self, block_ref: Arc<RwLock<Block>>) -> Result<(), ReductError> {
        debug!(
            "Saving block {}/{}/{} on disk and updating index",
            self.bucket,
            self.entry,
            block_ref.read()?.block_id()
        );

        let block = block_ref.write()?;
        let block_id = block.block_id();

        let path = self.path_to_desc(block.block_id());
        let mut buf = BytesMut::new();

        let mut proto = BlockProto::from(block.to_owned());
        proto.encode(&mut buf).map_err(|e| {
            internal_server_error!("Failed to encode block descriptor {:?}: {}", path, e)
        })?;

        trace!("Writing block descriptor {:?}", path);

        // overwrite the file
        let len = {
            let file = FILE_CACHE
                .write_or_create(&path, SeekFrom::Start(0))?
                .upgrade()?;
            let mut lock = file.write()?;
            let len = buf.len() as u64;
            lock.set_len(len)?;
            lock.write_all(&buf)?;
            lock.flush()?;
            len
        };

        trace!("Updating block index");
        // update index
        proto.metadata_size = len; // update metadata size because it changed
        self.block_index.insert_or_update(proto);
        self.block_index.save()?;

        trace!("Block {}/{}/{} saved", self.bucket, self.entry, block_id);
        // clean WAL
        self.wal.remove(block_id)?;
        Ok(())
    }
}

pub type RecordRx = Receiver<Result<Bytes, ReductError>>;
pub type RecordTx = Sender<Result<Option<Bytes>, ReductError>>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::proto::record::Label;
    use crate::storage::proto::Record;
    use crate::storage::proto::{us_to_ts, BlockIndex as BlockIndexProto};
    use prost_wkt_types::Timestamp;
    use reduct_base::error::ErrorCode;
    use rstest::{fixture, rstest};

    use crate::storage::entry::{RecordWriter, WriteRecordContent};
    use crate::storage::storage::MAX_IO_BUFFER_SIZE;
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::io::AsyncWriteExt;
    use tokio::time::sleep;

    mod block_operations {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_starting_block(#[future] block_manager: BlockManager) {
            let mut block_manager = block_manager.await;
            let block_id = 1_000_005;

            let block = block_manager
                .start_new_block(block_id, 1024)
                .await
                .unwrap()
                .read()
                .await
                .clone();
            assert_eq!(block.block_id(), block_id,);

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
            assert_eq!(block_from_file, block.to_owned());
        }

        #[rstest]
        #[tokio::test]
        async fn test_loading_block(#[future] block_manager: BlockManager, block_id: u64) {
            let mut block_manager = block_manager.await;

            block_manager.start_new_block(block_id, 1024).await.unwrap();
            let block_ref = block_manager.start_new_block(20000005, 1024).await.unwrap();
            let block = block_ref.read().await;
            let loaded_block = block_manager.load_block(block.block_id()).await.unwrap();
            assert_eq!(loaded_block.read().await.block_id(), block.block_id());
        }

        #[rstest]
        #[tokio::test]
        async fn test_start_reading(#[future] block_manager: BlockManager, block_id: u64) {
            let mut block_manager = block_manager.await;
            let block = block_manager.start_new_block(block_id, 1024).await.unwrap();
            let block_id = block.read().await.block_id();
            let loaded_block = block_manager.load_block(block_id).await.unwrap();
            assert_eq!(loaded_block.read().await.block_id(), block_id);
        }

        #[rstest]
        #[tokio::test]
        async fn test_finish_block(#[future] block_manager: BlockManager, block_id: u64) {
            let mut block_manager = block_manager.await;
            let block = block_manager
                .start_new_block(block_id + 1, 1024)
                .await
                .unwrap();
            let block_id = block.read().await.block_id();
            let loaded_block = block_manager.load_block(block_id).await.unwrap();
            assert_eq!(loaded_block.read().await.block_id(), block_id);

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
            let block_manager = Arc::new(RwLock::new(block_manager.await));
            let writer = RecordWriter::try_new(Arc::clone(&block_manager), block.await, 0)
                .await
                .unwrap();

            writer.tx().send(Ok(None)).await.unwrap();
            sleep(Duration::from_millis(10)).await; // wait for thread to finish

            let block_ref = block_manager
                .write()
                .await
                .load_block(block_id)
                .await
                .unwrap();
            assert_eq!(block_ref.read().await.get_record(0).unwrap().state, 2);
        }
    }

    mod index_operations {
        use super::*;
        #[rstest]
        #[tokio::test]
        async fn test_update_index_when_start_new_one(
            #[future] block_manager: BlockManager,
            #[future] block: BlockRef,
            block_id: u64,
        ) {
            let block_manager = Arc::new(RwLock::new(block_manager.await));
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
            let block = block.await;
            block.write().await.insert_or_update_record(record.clone());

            let writer = RecordWriter::try_new(Arc::clone(&block_manager), block, 1000_000)
                .await
                .unwrap();
            writer
                .tx()
                .send(Ok(Some(Bytes::from("hallo"))))
                .await
                .unwrap();
            sleep(Duration::from_millis(10)).await; // wait for thread to finish

            // must save record in WAL
            let mut bm = block_manager.write().await;
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

            // drop cache in disk when block is changed
            let _ = bm.start_new_block(block_id + 1, 1024).await.unwrap();
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
            let index = BlockIndex::try_load(bm.path.join(BLOCK_INDEX_FILE))
                .await
                .unwrap();
            assert_eq!(
                index.get_block(block_id).unwrap().metadata_size,
                25,
                "index not updated"
            );

            let block_ref = block.await;
            let record = {
                let lock = block_ref.write().await;
                let mut record = lock.get_record(0).unwrap().clone();
                record.labels = vec![Label {
                    name: "key".to_string(),
                    value: "value".to_string(),
                }];

                record
            };

            bm.update_records(block_id, vec![record]).await.unwrap();
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
        async fn test_update_imdex_when_remove_record(
            #[future] block_manager: BlockManager,
            #[future] block: BlockRef,
            block_id: u64,
        ) {
            let block_manager = Arc::new(RwLock::new(block_manager.await));
            let block = block.await;
            write_record(1, 100, &block_manager, block.clone()).await;

            let mut bm = block_manager.write().await;
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
    }

    mod read_writer_counters {
        use super::*;
        use crate::storage::entry::RecordReader;

        #[rstest]
        #[tokio::test]
        async fn test_remove_with_writers(
            #[future] block_manager: BlockManager,
            #[future] block: BlockRef,
            block_id: u64,
        ) {
            let block_manager = Arc::new(RwLock::new(block_manager.await));
            let writer = RecordWriter::try_new(Arc::clone(&block_manager), block.await, 0)
                .await
                .unwrap();

            assert_eq!(
                block_manager
                    .write()
                    .await
                    .remove_block(block_id)
                    .await
                    .err(),
                Some(internal_server_error!(
                    "Cannot remove block {} because it is still in use",
                    block_id
                ))
            );

            writer
                .tx()
                .send(Ok(Some(Bytes::from(vec![0; MAX_IO_BUFFER_SIZE + 1]))))
                .await
                .unwrap();
            sleep(Duration::from_millis(10)).await; // wait for thread to finish

            assert_eq!(
                block_manager
                    .write()
                    .await
                    .remove_block(block_id)
                    .await
                    .err(),
                None
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_remove_block_with_writers_timeout(
            #[future] block_manager: BlockManager,
            #[future] block: BlockRef,
            block_id: u64,
        ) {
            let block_manager = Arc::new(RwLock::new(block_manager.await));
            RecordWriter::try_new(Arc::clone(&block_manager), block.await, 0)
                .await
                .unwrap();
            assert!(block_manager
                .write()
                .await
                .remove_block(block_id)
                .await
                .err()
                .is_some());

            sleep(IO_OPERATION_TIMEOUT).await;
            assert!(
                block_manager
                    .write()
                    .await
                    .remove_block(block_id)
                    .await
                    .err()
                    .is_none(),
                "Timeout should have unlocked the block"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_remove_block_with_readers(
            #[future] block_manager: BlockManager,
            #[future] block: BlockRef,
            block_id: u64,
        ) {
            let block_manager = Arc::new(RwLock::new(block_manager.await));

            let mut reader =
                RecordReader::try_new(Arc::clone(&block_manager), block.await, 0, false)
                    .await
                    .unwrap();

            assert_eq!(
                block_manager
                    .write()
                    .await
                    .remove_block(block_id)
                    .await
                    .err(),
                Some(internal_server_error!(
                    "Cannot remove block {} because it is still in use",
                    block_id
                ))
            );

            while let Some(Ok(_)) = reader.rx().recv().await {
                // wait for reader to finish
            }

            sleep(Duration::from_millis(10)).await; // wait for thread to finish
            drop(reader);

            assert_eq!(
                block_manager
                    .write()
                    .await
                    .remove_block(block_id)
                    .await
                    .err(),
                None
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_remove_block_with_readers_timeout(
            #[future] block_manager: BlockManager,
            #[future] block: BlockRef,
            block_id: u64,
        ) {
            let block_manager = Arc::new(RwLock::new(block_manager.await));
            let _reader = RecordReader::try_new(Arc::clone(&block_manager), block.await, 0, false)
                .await
                .unwrap();
            assert!(block_manager
                .write()
                .await
                .remove_block(block_id)
                .await
                .err()
                .is_some());

            sleep(IO_OPERATION_TIMEOUT).await;
            assert!(
                block_manager
                    .write()
                    .await
                    .remove_block(block_id)
                    .await
                    .err()
                    .is_none(),
                "Timeout should have unlocked the block"
            );
        }
    }

    mod record_removing {
        use super::*;
        use crate::storage::entry::RecordReader;

        #[rstest]
        #[case(0)]
        #[case(500)]
        #[case(MAX_IO_BUFFER_SIZE+1)]
        #[tokio::test]
        async fn test_remove_records(
            #[case] record_size: usize,
            #[future] block_manager: BlockManager,
            #[future] block: BlockRef,
            block_id: u64,
        ) {
            let block_manager = Arc::new(RwLock::new(block_manager.await));
            let block_ref = block.await;
            let (record, record_body) =
                write_record(1, record_size, &block_manager, block_ref.clone()).await;

            // remove first record
            block_manager
                .write()
                .await
                .remove_records(block_id, vec![0])
                .await
                .unwrap();

            let block = block_ref.read().await;
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
                false,
            )
            .await
            .unwrap();

            let mut received = BytesMut::new();
            while let Some(Ok(chunk)) = reader.rx().recv().await {
                received.extend_from_slice(&chunk);
            }
            assert_eq!(received.len(), record_size);
            assert_eq!(received, record_body.as_bytes());

            assert!(
                block_manager
                    .write()
                    .await
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
            let block_manager = Arc::new(RwLock::new(block_manager.await));
            let block_ref = block.await;
            write_record(1, 5, &block_manager, block_ref.clone()).await;

            let mut bm = block_manager.write().await;

            let block_id = block_ref.read().await.block_id();
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
        block_manager: &Arc<RwLock<BlockManager>>,
        block_ref: BlockRef,
    ) -> (Record, String) {
        let block_size = block_ref.read().await.size();
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
            .insert_or_update_record(record.clone());

        let record_body: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(record_size)
            .map(char::from)
            .collect();

        let writer =
            RecordWriter::try_new(Arc::clone(&block_manager), block_ref.clone(), record_time)
                .await
                .unwrap();
        writer
            .tx()
            .send(Ok(Some(Bytes::from(record_body.clone()))))
            .await
            .unwrap();
        sleep(Duration::from_millis(10)).await; // wait for thread to finish
        block_manager
            .write()
            .await
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
        let bm = block_manager.await;
        bm.load_block(block_id).await.unwrap()
    }

    #[fixture]
    async fn block_manager(block_id: u64) -> BlockManager {
        let path = tempdir().unwrap().into_path();

        let mut bm = BlockManager::new(path.clone(), BlockIndex::new(path.join(BLOCK_INDEX_FILE)));
        let block_ref = bm.start_new_block(block_id, 1024).await.unwrap().clone();
        {
            let mut block = block_ref.write().await;
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

            let (file, _) = bm.begin_write_record(&block, 0).await.unwrap();
            file.upgrade()
                .unwrap()
                .write()
                .await
                .write(&vec![0; MAX_IO_BUFFER_SIZE + 1])
                .await
                .unwrap();
        }

        bm.finish_write_record(block_id, record::State::Finished, 0)
            .await
            .unwrap();
        bm.save_block_on_disk(block_ref).await.unwrap();
        bm
    }
}
