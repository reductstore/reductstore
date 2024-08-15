// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

pub(in crate::storage) mod block;
pub(in crate::storage) mod block_index;
mod use_counter;
pub(in crate::storage) mod wal;

use prost::bytes::{Bytes, BytesMut};
use prost::Message;
use std::cmp::min;

use log::{debug, error};

use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;

use crate::storage::block_manager::block::Block;
use crate::storage::block_manager::use_counter::UseCounter;
use crate::storage::block_manager::wal::{Wal, WalEntry};
use crate::storage::file_cache::{get_global_file_cache, FileRef};
use crate::storage::proto::{record, Block as BlockProto, Record};
use crate::storage::storage::{CHANNEL_BUFFER_SIZE, DEFAULT_MAX_READ_CHUNK, IO_OPERATION_TIMEOUT};
use block_index::BlockIndex;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::RwLock;
use tokio::time::timeout;

pub(crate) type BlockRef = Arc<RwLock<Block>>;

/// Helper class for basic operations on blocks.
///
/// ## Notes
///
/// It is not thread safe and may cause data corruption if used from multiple threads,
/// because it does not lock the block descriptor file. Use it with RwLock<BlockManager>
pub(in crate::storage) struct BlockManager {
    path: PathBuf,
    use_counter: UseCounter,
    bucket: String,
    entry: String,
    cached_block_read: Option<BlockRef>,
    cached_block_write: Option<BlockRef>,
    block_index: BlockIndex,
    wal: Box<dyn Wal + Sync + Send>,
}

pub const DESCRIPTOR_FILE_EXT: &str = ".meta";
pub const DATA_FILE_EXT: &str = ".blk";
pub const BLOCK_INDEX_FILE: &str = "blocks.idx";

impl BlockManager {
    pub(crate) fn new(path: PathBuf, index: BlockIndex) -> Self {
        let (bucket, entry) = {
            let mut parts = path.iter().rev();
            let entry = parts.next().unwrap().to_str().unwrap().to_string();
            let bucket = parts.next().unwrap().to_str().unwrap().to_string();
            (bucket, entry)
        };

        Self {
            path: path.clone(),
            use_counter: UseCounter::new(IO_OPERATION_TIMEOUT),
            bucket,
            entry,
            cached_block_read: None,
            cached_block_write: None,
            block_index: index,
            wal: wal::create_wal(path.clone()),
        }
    }

    /// Begin writing a record to a block.
    ///
    /// # Arguments
    ///
    /// * `block` - Block to write to.
    /// * `record_index` - Index of the record to write.
    ///
    /// # Returns
    ///
    /// * `Ok(file)` - File to write to.
    pub async fn begin_write(
        &mut self,
        block: BlockRef,
        record_timestamp: u64,
    ) -> Result<(FileRef, usize), ReductError> {
        let block = block.read().await;
        self.use_counter.increment(block.block_id());

        let path = self.path_to_data(block.block_id());
        let file = get_global_file_cache().write_or_create(&path).await?;
        let offset = block.get_record(record_timestamp).unwrap().begin;
        Ok((file, offset as usize))
    }

    async fn finish_write_record(
        &mut self,
        block_id: u64,
        state: record::State,
        record_timestamp: u64,
    ) -> Result<(), ReductError> {
        // check if the block is still in cache
        let block_ref = if let Some(block_ref) = self.cached_block_write.clone() {
            let block = block_ref.read().await;
            if block.block_id() == block_id {
                block_ref.clone()
            } else {
                self.load(block_id).await?
            }
        } else {
            self.load(block_id).await?
        };

        {
            let mut block = block_ref.write().await;
            block.change_record_state(record_timestamp, i32::from(state))?;
            self.use_counter.decrement(block_id);

            // write to WAL
            self.wal
                .append(
                    block_id,
                    WalEntry::WriteRecord(block.get_record(record_timestamp).unwrap().clone()),
                )
                .await?;
        }
        self.save(block_ref).await?;

        debug!(
            "Finished writing record {}/{}/{} with state {:?}",
            self.bucket, self.entry, block_id, state
        );

        Ok(())
    }

    pub async fn begin_read(
        &mut self,
        block_ref: BlockRef,
        record_timestamp: u64,
    ) -> Result<(FileRef, usize), ReductError> {
        let block = block_ref.read().await;
        self.use_counter.increment(block.block_id());

        let path = self.path_to_data(block.block_id());
        let file = get_global_file_cache().read(&path).await?;
        let offset = block.get_record(record_timestamp).unwrap().begin;
        Ok((file, offset as usize))
    }

    pub fn finish_read_record(&mut self, block_id: u64) {
        self.use_counter.decrement(block_id);
    }

    pub async fn save_cache_on_disk(&mut self) -> Result<(), ReductError> {
        if self.cached_block_write.is_none() {
            return Ok(());
        }

        let block_ref = self.cached_block_write.as_ref().unwrap().clone();
        self.save_block_on_disk(block_ref).await
    }

    async fn save_block_on_disk(
        &mut self,
        block_ref: Arc<RwLock<Block>>,
    ) -> Result<(), ReductError> {
        let block = block_ref.write().await;
        let block_id = block.block_id();

        let path = self.path_to_desc(block.block_id());
        let mut buf = BytesMut::new();

        let mut proto = BlockProto::from(block.to_owned());
        proto.encode(&mut buf).map_err(|e| {
            internal_server_error!("Failed to encode block descriptor {:?}: {}", path, e)
        })?;

        // overwrite the file
        let file = get_global_file_cache().write_or_create(&path).await?;
        let mut lock = file.write().await;
        lock.set_len(buf.len() as u64).await?;
        lock.seek(SeekFrom::Start(0)).await?;
        lock.write_all(&buf).await?;
        lock.flush().await?;

        // update index
        proto.metadata_size = buf.len() as u64; // update metadata size because it changed
        self.block_index.insert_or_update(proto);
        self.block_index.save().await?;

        // clean WAL
        self.wal.remove(block_id).await?;
        Ok(())
    }

    pub async fn find_block(&mut self, start: u64) -> Result<BlockRef, ReductError> {
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

        self.load(id).await
    }

    pub fn index_mut(&mut self) -> &mut BlockIndex {
        &mut self.block_index
    }

    pub fn index(&self) -> &BlockIndex {
        &self.block_index
    }

    fn path_to_desc(&self, block_id: u64) -> PathBuf {
        self.path
            .join(format!("{}{}", block_id, DESCRIPTOR_FILE_EXT))
    }

    fn path_to_data(&self, block_id: u64) -> PathBuf {
        self.path.join(format!("{}{}", block_id, DATA_FILE_EXT))
    }
}

pub(in crate::storage) trait ManageBlock {
    /// Load block descriptor from disk.
    ///
    /// # Arguments
    /// * `block_id` - ID of the block to load (begin time of the block).
    ///
    /// # Returns
    ///
    /// * `Ok(block)` - Block was loaded successfully.
    async fn load(&mut self, block_id: u64) -> Result<BlockRef, ReductError>;

    /// Save block descriptor in cache and on disk if needed.
    ///
    ///
    /// # Arguments
    ///
    /// * `block` - Block to save.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Block was saved successfully.
    async fn save(&mut self, block: BlockRef) -> Result<(), ReductError>;

    /// Start a new block
    ///
    /// # Arguments
    ///
    /// * `begin_time` - Begin time of the block.
    /// * `max_block_size` - Maximum size of the block.
    ///
    /// # Returns
    ///
    /// * `Ok(block)` - Block was created successfully.
    async fn start(&mut self, block_id: u64, max_block_size: u64) -> Result<BlockRef, ReductError>;

    /// Update a record in a block and save it to disk.
    async fn update_record(&mut self, block: BlockRef, record: Record) -> Result<(), ReductError>;

    /// Finish a block by truncating the file to the actual size.
    ///
    /// # Arguments
    ///
    /// * `block` - Block to finish.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Block was finished successfully.
    async fn finish(&mut self, block: BlockRef) -> Result<(), ReductError>;

    /// Remove a block from disk if there are no readers or writers.
    async fn remove(&mut self, block_id: u64) -> Result<(), ReductError>;

    /// Check if a block exists in the file system.
    async fn exist(&mut self, block_id: u64) -> Result<bool, ReductError>;
}

impl ManageBlock for BlockManager {
    async fn load(&mut self, block_id: u64) -> Result<BlockRef, ReductError> {
        // first check if we have the block in write cache

        let mut cached_block = None;
        if let Some(block) = self.cached_block_write.as_ref() {
            if block.read().await.block_id() == block_id {
                cached_block = Some(block.clone());
            } else if let Some(block) = self.cached_block_read.as_ref() {
                // then check if we have the block in read cache
                if block.read().await.block_id() == block_id {
                    cached_block = Some(block.clone());
                }
            }
        }

        if cached_block.is_none() {
            if self.wal.exists(block_id) {
                // we have a WAL for the block, sync it
                self.save_cache_on_disk().await?;
            }

            let path = self.path_to_desc(block_id);
            let file = get_global_file_cache().read(&path).await?;
            let mut buf = vec![];

            // parse the block descriptor
            let mut lock = file.write().await;
            lock.seek(SeekFrom::Start(0)).await?;
            lock.read_to_end(&mut buf).await?;

            let block_from_disk = BlockProto::decode(Bytes::from(buf)).map_err(|e| {
                internal_server_error!("Failed to decode block descriptor {:?}: {}", path, e)
            })?;
            cached_block = Some(Arc::new(RwLock::new(block_from_disk.into())));
        }

        self.cached_block_read = cached_block.clone();
        Ok(cached_block.unwrap())
    }

    async fn save(&mut self, block: BlockRef) -> Result<(), ReductError> {
        // cache is empty, save the block there first
        if self.cached_block_write.is_none() {
            let _ = self.cached_block_write.insert(block.clone());
            return Ok(());
        }

        // update the cached block if we changed it and do nothing
        // we have all needed information in WAL
        {
            if Arc::ptr_eq(self.cached_block_write.as_ref().unwrap(), &block) {
                let _ = self.cached_block_write.insert(block);
                return Ok(());
            }
        }
        self.save_cache_on_disk().await?;

        // update cache
        let _ = self.cached_block_write.insert(block);

        Ok(())
    }

    async fn start(&mut self, block_id: u64, max_block_size: u64) -> Result<BlockRef, ReductError> {
        let block = Block::new(block_id);

        // create a block with data
        {
            let file = get_global_file_cache()
                .write_or_create(&self.path_to_data(block_id))
                .await?;
            let file = file.write().await;
            file.set_len(max_block_size).await?;
        }

        self.block_index.insert_or_update(block.clone());

        let block_ref = Arc::new(RwLock::new(block));
        self.save(block_ref.clone()).await?;
        Ok(block_ref)
    }

    async fn update_record(&mut self, block: BlockRef, record: Record) -> Result<(), ReductError> {
        self.wal
            .append(
                block.read().await.block_id(),
                WalEntry::UpdateRecord(record),
            )
            .await?;

        self.save_block_on_disk(block).await
    }

    async fn finish(&mut self, block: BlockRef) -> Result<(), ReductError> {
        let block = block.read().await;
        /* resize data block then sync descriptor and data */
        let path = self.path_to_data(block.block_id());
        let file = get_global_file_cache().write_or_create(&path).await?;
        let data_block = file.write().await;
        data_block.set_len(block.size()).await?;
        data_block.sync_all().await?;

        let file = get_global_file_cache()
            .write_or_create(&self.path_to_desc(block.block_id()))
            .await?;
        let descr_block = file.write().await;
        descr_block.sync_all().await?;

        Ok(())
    }

    async fn remove(&mut self, block_id: u64) -> Result<(), ReductError> {
        if !self.use_counter.clean_stale_and_check(block_id) {
            return Err(internal_server_error!(
                "Cannot remove block {} because it is still in use",
                block_id
            ));
        }

        self.wal.append(block_id, WalEntry::RemoveBlock).await?;
        self.save_cache_on_disk().await?;
        if let Some(block) = self.cached_block_read.as_ref() {
            if block.read().await.block_id() == block_id {
                self.cached_block_read = None;
            }
        }

        let path = self.path_to_data(block_id);
        get_global_file_cache().remove(&path).await?;

        let path = self.path_to_desc(block_id);
        get_global_file_cache().remove(&path).await?;

        self.block_index.remove_block(block_id);
        self.block_index.save().await?;

        self.wal.remove(block_id).await?;
        Ok(())
    }

    async fn exist(&mut self, block_id: u64) -> Result<bool, ReductError> {
        let path = self.path_to_desc(block_id);
        Ok(path.try_exists()?)
    }
}

pub type RecordRx = Receiver<Result<Bytes, ReductError>>;
pub type RecordTx = Sender<Result<Option<Bytes>, ReductError>>;

/// Spawn a task that reads a record from a block.
/// The task will send chunks of the record to the receiver or an error if file reading failed
///
/// # Arguments
///
/// * `block_manager` - Block manager to use.
/// * `block` - Block to read from.
/// * `record_index` - Index of the record to read.
///
/// # Returns
///
/// * `Ok(rx)` - Receiver to receive chunks from.
///
/// # Errors
///
/// * `ReductError` - If could not open the block
pub async fn spawn_read_task(
    block_manager: Arc<RwLock<BlockManager>>,
    block_ref: BlockRef,
    record_timestamp: u64,
) -> Result<RecordRx, ReductError> {
    let (file, offset) = block_manager
        .write()
        .await
        .begin_read(block_ref.clone(), record_timestamp)
        .await?;

    let (entry, bucket) = {
        let bm = block_manager.read().await;
        (bm.entry.clone(), bm.bucket.clone())
    };

    let block = block_ref.read().await;
    let record = block.get_record(record_timestamp).unwrap();
    let (tx, rx) = channel(CHANNEL_BUFFER_SIZE);
    let content_size = (record.end - record.begin) as usize;
    let block_id = block.block_id();
    let local_bm = Arc::clone(&block_manager);
    tokio::spawn(async move {
        let mut read_bytes = 0;
        loop {
            let chunk_size = min(content_size - read_bytes, DEFAULT_MAX_READ_CHUNK) as usize;
            let mut buf = vec![0; chunk_size];

            let seek_and_read = async {
                let mut lock = file.write().await;
                lock.seek(SeekFrom::Start((offset + read_bytes) as u64))
                    .await?;
                lock.read(&mut buf).await
            };

            let read = match seek_and_read.await {
                Ok(read) => read,
                Err(e) => {
                    let _ = tx
                        .send(Err(internal_server_error!(
                            "Failed to read record chunk: {}",
                            e
                        )))
                        .await;
                    break;
                }
            };

            if read == 0 {
                let _ = tx
                    .send(Err(internal_server_error!(
                        "Failed to read record chunk: EOF",
                    )))
                    .await;
                break;
            }
            if let Err(e) = tx.send_timeout(Ok(buf.into()), IO_OPERATION_TIMEOUT).await {
                debug!(
                    "Failed to send record {}/{}/{}: {}",
                    bucket, entry, record_timestamp, e
                ); // for some reason the receiver is closed
                break;
            }

            read_bytes += read;
            if read_bytes == content_size {
                break;
            }

            local_bm.write().await.use_counter.update(block_id);
        }

        local_bm.write().await.finish_read_record(block_id);
    });
    Ok(rx)
}

/// Spawn a task that writes a record to a block.
///
/// # Arguments
///
/// * `block_manager` - Block manager to use.
/// * `block` - Block to write to.
/// * `record_index` - Index of the record to write.
///
/// # Returns
///
/// * `Ok(tx)` - Sender to send chunks to.
///
/// # Errors
///
/// * `ReductError` - If the block is invalid or the record is already finished.
pub async fn spawn_write_task(
    block_manager: Arc<RwLock<BlockManager>>,
    block_ref: BlockRef,
    record_timestamp: u64,
) -> Result<RecordTx, ReductError> {
    let (file, offset) = {
        let mut bm = block_manager.write().await;

        {
            let block = block_ref.read().await;
            bm.index_mut().insert_or_update(block.to_owned());
        }

        bm.save(block_ref.clone()).await?;
        bm.begin_write(block_ref.clone(), record_timestamp).await?
    };

    let (tx, mut rx) = channel(CHANNEL_BUFFER_SIZE);
    let bm = Arc::clone(&block_manager);
    let block = block_ref.read().await;
    let block_id = block.block_id();
    let record_index = block.get_record(record_timestamp).unwrap();
    let content_size = (record_index.end - record_index.begin) as usize;

    tokio::spawn(async move {
        let recv = async move {
            let mut written_bytes = 0;
            while let Some(chunk) = timeout(IO_OPERATION_TIMEOUT, rx.recv())
                .await
                .map_err(|_| internal_server_error!("Timeout while reading record"))?
            {
                let chunk: Option<Bytes> = chunk?;
                match chunk {
                    Some(chunk) => {
                        written_bytes += chunk.len();
                        if written_bytes > content_size {
                            return Err(ReductError::bad_request(
                                "Content is bigger than in content-length",
                            ));
                        }

                        {
                            let mut lock = file.write().await;
                            lock.seek(SeekFrom::Start(
                                (offset + written_bytes - chunk.len()) as u64,
                            ))
                            .await?;
                            lock.write_all(chunk.as_ref()).await?;
                        }
                    }
                    None => {
                        break;
                    }
                }

                if written_bytes >= content_size {
                    break;
                }

                block_manager.write().await.use_counter.update(block_id);
            }

            if written_bytes < content_size {
                Err(ReductError::bad_request(
                    "Content is smaller than in content-length",
                ))
            } else {
                file.write().await.flush().await?;
                Ok(())
            }
        };

        let state = match recv.await {
            Ok(_) => record::State::Finished,
            Err(err) => {
                let bm = bm.read().await;
                error!(
                    "Failed to write record {}/{}/{}: {}",
                    bm.bucket, bm.entry, record_timestamp, err
                );
                record::State::Errored
            }
        };

        if let Err(err) = bm
            .write()
            .await
            .finish_write_record(block_id, state, record_timestamp)
            .await
        {
            let bm = bm.read().await;
            error!(
                "Failed to finish writing {}/{}/{} record: {}",
                bm.bucket, bm.entry, record_timestamp, err
            );
        }
    });
    Ok(tx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::proto::record::Label;
    use crate::storage::proto::BlockIndex as BlockIndexProto;
    use crate::storage::proto::Record;
    use prost_wkt_types::Timestamp;
    use reduct_base::error::ErrorCode;
    use rstest::{fixture, rstest};

    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::io::AsyncWriteExt;
    use tokio::time::sleep;

    #[rstest]
    #[tokio::test]
    async fn test_starting_block(#[future] block_manager: BlockManager) {
        let mut block_manager = block_manager.await;
        let block_id = 1_000_005;

        let block = block_manager
            .start(block_id, 1024)
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

        block_manager.start(block_id, 1024).await.unwrap();
        let block_ref = block_manager.start(20000005, 1024).await.unwrap();
        let block = block_ref.read().await;
        let loaded_block = block_manager.load(block.block_id()).await.unwrap();
        assert_eq!(loaded_block.read().await.block_id(), block.block_id());
    }

    #[rstest]
    #[tokio::test]
    async fn test_start_reading(#[future] block_manager: BlockManager, block_id: u64) {
        let mut block_manager = block_manager.await;
        let block = block_manager.start(block_id, 1024).await.unwrap();
        let block_id = block.read().await.block_id();
        let loaded_block = block_manager.load(block_id).await.unwrap();
        assert_eq!(loaded_block.read().await.block_id(), block_id);
    }

    #[rstest]
    #[tokio::test]
    async fn test_finish_block(#[future] block_manager: BlockManager, block_id: u64) {
        let mut block_manager = block_manager.await;
        let block = block_manager.start(block_id + 1, 1024).await.unwrap();
        let block_id = block.read().await.block_id();
        let loaded_block = block_manager.load(block_id).await.unwrap();
        assert_eq!(loaded_block.read().await.block_id(), block_id);

        block_manager.finish(loaded_block).await.unwrap();

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
        let tx = spawn_write_task(Arc::clone(&block_manager), block.await, 0)
            .await
            .unwrap();

        tx.send(Ok(None)).await.unwrap();
        drop(tx);
        sleep(Duration::from_millis(10)).await; // wait for thread to finish

        let block_ref = block_manager.write().await.load(block_id).await.unwrap();
        assert_eq!(block_ref.read().await.get_record(0).unwrap().state, 2);
    }

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

        let tx = spawn_write_task(Arc::clone(&block_manager), block, 1000_000)
            .await
            .unwrap();
        tx.send(Ok(Some(Bytes::from("hallo")))).await.unwrap();
        drop(tx);
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
        let _ = bm.start(block_id + 1, 1024).await.unwrap();
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
            21,
            "index not updated"
        );

        let block = block.await;
        let record = {
            let lock = block.write().await;
            let mut record = lock.get_record(0).unwrap().clone();
            record.labels = vec![Label {
                name: "key".to_string(),
                value: "value".to_string(),
            }];
            record
        };

        block.write().await.insert_or_update_record(record.clone());
        bm.update_record(block, record).await.unwrap();
        let block_index_proto = BlockIndexProto::decode(
            std::fs::read(bm.path.join(BLOCK_INDEX_FILE))
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        assert_eq!(
            block_index_proto.blocks[0].metadata_size, 35,
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
        bm.remove(block_id).await.unwrap();

        let index = BlockIndex::try_load(bm.path.join(BLOCK_INDEX_FILE))
            .await
            .unwrap();
        assert!(index.get_block(block_id).is_none(), "index updated");
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_with_writers(
        #[future] block_manager: BlockManager,
        #[future] block: BlockRef,
        block_id: u64,
    ) {
        let block_manager = Arc::new(RwLock::new(block_manager.await));
        let tx = spawn_write_task(Arc::clone(&block_manager), block.await, 0)
            .await
            .unwrap();

        assert_eq!(
            block_manager.write().await.remove(block_id).await.err(),
            Some(internal_server_error!(
                "Cannot remove block {} because it is still in use",
                block_id
            ))
        );

        tx.send(Ok(Some(Bytes::from("hallo")))).await.unwrap();
        drop(tx);
        sleep(Duration::from_millis(10)).await; // wait for thread to finish
        assert_eq!(
            block_manager.write().await.remove(block_id).await.err(),
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
        spawn_write_task(Arc::clone(&block_manager), block.await, 0)
            .await
            .unwrap();
        assert!(block_manager
            .write()
            .await
            .remove(block_id)
            .await
            .err()
            .is_some());

        sleep(IO_OPERATION_TIMEOUT).await;
        assert!(
            block_manager
                .write()
                .await
                .remove(block_id)
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

        let mut rx = spawn_read_task(Arc::clone(&block_manager), block.await, 0)
            .await
            .unwrap();

        assert_eq!(
            block_manager.write().await.remove(block_id).await.err(),
            Some(internal_server_error!(
                "Cannot remove block {} because it is still in use",
                block_id
            ))
        );

        assert_eq!(rx.recv().await.unwrap().unwrap().as_ref(), b"hallo");
        drop(rx);
        sleep(Duration::from_millis(10)).await; // wait for thread to finish

        assert_eq!(
            block_manager.write().await.remove(block_id).await.err(),
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
        spawn_read_task(Arc::clone(&block_manager), block.await, 0)
            .await
            .unwrap();
        assert!(block_manager
            .write()
            .await
            .remove(block_id)
            .await
            .err()
            .is_some());

        sleep(IO_OPERATION_TIMEOUT).await;
        assert!(
            block_manager
                .write()
                .await
                .remove(block_id)
                .await
                .err()
                .is_none(),
            "Timeout should have unlocked the block"
        );
    }

    #[fixture]
    fn block_id() -> u64 {
        1
    }

    #[fixture]
    async fn block(#[future] block_manager: BlockManager, block_id: u64) -> BlockRef {
        let mut bm = block_manager.await;
        bm.load(block_id).await.unwrap()
    }

    #[fixture]
    async fn block_manager(block_id: u64) -> BlockManager {
        let path = tempdir().unwrap().into_path();

        let mut bm = BlockManager::new(path.clone(), BlockIndex::new(path.join(BLOCK_INDEX_FILE)));
        let block = bm.start(block_id, 1024).await.unwrap().clone();
        block.write().await.insert_or_update_record(Record {
            timestamp: Some(Timestamp {
                seconds: 0,
                nanos: 0,
            }),
            begin: 0,
            end: 5,
            state: 0,
            labels: vec![],
            content_type: "".to_string(),
        });

        bm.save(block.clone()).await.unwrap();

        let (file, _) = bm.begin_write(block.clone(), 0).await.unwrap();
        file.write().await.write(b"hallo").await.unwrap();
        bm.finish_write_record(block_id, record::State::Finished, 0)
            .await
            .unwrap();

        bm.save_block_on_disk(block).await.unwrap();
        bm
    }
}
