// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

pub mod block_index;
mod use_counter;
pub(in crate::storage) mod wal;

use prost::bytes::{Bytes, BytesMut};
use prost::Message;
use prost_wkt_types::Timestamp;
use std::cell::RefCell;
use std::cmp::min;

use log::{debug, error};

use std::collections::BTreeSet;
use std::fs;
use std::io::SeekFrom;
use std::ops::Deref;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use crate::storage::block_manager::use_counter::UseCounter;
use crate::storage::block_manager::wal::{Wal, WalEntry};
use crate::storage::file_cache::{get_global_file_cache, FileCache, FileRef};
use crate::storage::proto::{record, ts_to_us, us_to_ts, Block};
use crate::storage::storage::{CHANNEL_BUFFER_SIZE, DEFAULT_MAX_READ_CHUNK, IO_OPERATION_TIMEOUT};
use block_index::BlockIndex;
use reduct_base::error::{ErrorCode, ReductError};
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
pub struct BlockManager {
    path: PathBuf,
    use_counter: UseCounter,
    bucket: String,
    entry: String,
    cached_block_read: Option<BlockRef>,
    cached_block_write: Option<BlockRef>,
    block_index: BlockIndex,
    write_wal: Arc<RwLock<Box<dyn Wal + Sync + Send>>>,
}

pub const DESCRIPTOR_FILE_EXT: &str = ".meta";
pub const DATA_FILE_EXT: &str = ".blk";
pub const BLOCK_INDEX_FILE: &str = ".block_index";

/// Find the first block id that contains data for a given timestamp  in indexes
///
/// # Arguments
///
/// * `block_index` - Block index to search in.
/// * `start` - Timestamp to search for.
///
/// # Returns
///
/// * `u64` - Block id.
pub fn find_first_block(block_index: &BTreeSet<u64>, start: &u64) -> u64 {
    let start_block_id = block_index.range(start..).next();
    if start_block_id.is_some() && start >= start_block_id.unwrap() {
        start_block_id.unwrap().clone()
    } else {
        block_index
            .range(..start)
            .rev()
            .next()
            .unwrap_or(&0)
            .clone()
    }
}

impl BlockManager {
    pub(crate) fn new(path: PathBuf, index: BlockIndex) -> Self {
        let (bucket, entry) = {
            let mut parts = path.iter().rev();
            let entry = parts.next().unwrap().to_str().unwrap().to_string();
            let bucket = parts.next().unwrap().to_str().unwrap().to_string();
            (bucket, entry)
        };

        let write_wal = Arc::new(RwLock::new(wal::create_wal(path.clone())));

        Self {
            path,
            use_counter: UseCounter::new(IO_OPERATION_TIMEOUT),
            bucket,
            entry,
            cached_block_read: None,
            cached_block_write: None,
            block_index: index,
            write_wal,
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
        block: &Block,
        record_index: usize,
    ) -> Result<(FileRef, usize), ReductError> {
        let ts = block.begin_time.clone().unwrap();
        self.use_counter.increment(ts_to_us(&ts));

        let path = self.path_to_data(&ts);
        let file = get_global_file_cache().write_or_create(&path).await?;
        let offset = block.records[record_index].begin;
        Ok((file, offset as usize))
    }

    async fn finish_write_record(
        &mut self,
        block_id: u64,
        state: record::State,
        record_index: usize,
    ) -> Result<(), ReductError> {
        let mut block_ref = if let Some(block_ref) = self.cached_block_write.clone() {
            let block = block_ref.read().await;
            if ts_to_us(&block.begin_time.clone().unwrap()) == block_id {
                block_ref.clone()
            } else {
                self.load(block_id).await?
            }
        } else {
            self.load(block_id).await?
        };

        let mut block = block_ref.write().await;
        let record = &mut block.records[record_index];
        let block_id = ts_to_us(&record.timestamp.as_ref().unwrap());
        record.state = i32::from(state);
        self.use_counter.decrement(block_id);

        self.write_wal
            .write()
            .await
            .append(block_id, WalEntry::WriteRecord(record.clone()))
            .await?;
        self.save(block.deref()).await?;

        debug!(
            "Finished writing record {}/{}/{} with state {:?}",
            self.bucket, self.entry, block_id, state
        );

        Ok(())
    }

    pub async fn begin_read(
        &mut self,
        block: &Block,
        record_index: usize,
    ) -> Result<(FileRef, usize), ReductError> {
        let ts = block.begin_time.clone().unwrap();

        self.use_counter.increment(ts_to_us(&ts));

        let path = self.path_to_data(&ts);
        let file = get_global_file_cache().read(&path).await?;
        let offset = block.records[record_index].begin;
        Ok((file, offset as usize))
    }

    pub fn finish_read_record(&mut self, block_id: u64) {
        self.use_counter.decrement(block_id);
    }

    pub async fn save_cache_on_disk(&mut self) -> Result<(), ReductError> {
        let block_ref = self.cached_block_write.as_ref().unwrap();
        let block = block_ref.read().await;

        let path = self.path_to_desc(block.begin_time.as_ref().unwrap());
        let mut buf = BytesMut::new();
        block.encode(&mut buf).map_err(|e| {
            ReductError::internal_server_error(&format!(
                "Failed to encode block descriptor {:?}: {}",
                path, e
            ))
        })?;

        // overwrite the file
        let file = get_global_file_cache().write_or_create(&path).await?;
        let mut lock = file.write().await;
        lock.set_len(buf.len() as u64).await?;
        lock.seek(SeekFrom::Start(0)).await?;
        lock.write_all(&buf).await?;
        lock.flush().await?;

        // update index
        self.block_index.insert_from_block(&block);
        self.block_index.save().await?;

        // clean WAL
        self.write_wal
            .write()
            .await
            .clean(ts_to_us(block.begin_time.as_ref().unwrap()))
            .await?;
        Ok(())
    }

    pub fn index_mut(&mut self) -> &mut BlockIndex {
        &mut self.block_index
    }

    pub fn index(&self) -> &BlockIndex {
        &self.block_index
    }

    fn path_to_desc(&self, begin_time: &Timestamp) -> PathBuf {
        let block_id = ts_to_us(&begin_time);
        self.path
            .join(format!("{}{}", block_id, DESCRIPTOR_FILE_EXT))
    }

    fn path_to_data(&self, begin_time: &Timestamp) -> PathBuf {
        let block_id = ts_to_us(&begin_time);
        self.path.join(format!("{}{}", block_id, DATA_FILE_EXT))
    }
}

pub trait ManageBlock {
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
    async fn save(&mut self, block: &Block) -> Result<(), ReductError>;

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

    /// Finish a block by truncating the file to the actual size.
    ///
    /// # Arguments
    ///
    /// * `block` - Block to finish.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Block was finished successfully.
    async fn finish(&mut self, block: &Block) -> Result<(), ReductError>;

    /// Remove a block from disk if there are no readers or writers.
    async fn remove(&mut self, block_id: u64) -> Result<(), ReductError>;
}

impl ManageBlock for BlockManager {
    async fn load(&mut self, block_id: u64) -> Result<BlockRef, ReductError> {
        // first check if we have the block in write cache

        let mut cached_block = None;
        if let Some(block) = self.cached_block_write.take() {
            if ts_to_us(&block.read().await.begin_time.clone().unwrap()) == block_id {
                cached_block = Some(block);
            }
        }

        // then check if we have the block in read cache
        if let Some(block) = self.cached_block_read.take() {
            if ts_to_us(&block.read().await.begin_time.clone().unwrap()) == block_id {
                cached_block = Some(block);
            }
        }

        if cached_block.is_none() {
            let path = self.path_to_desc(&us_to_ts(&block_id));
            let file = get_global_file_cache().read(&path).await?;
            let mut buf = vec![];

            // parse the block descriptor
            let mut lock = file.write().await;
            lock.seek(SeekFrom::Start(0)).await?;
            lock.read_to_end(&mut buf).await?;

            let block_from_disk = Block::decode(Bytes::from(buf)).map_err(|e| {
                ReductError::internal_server_error(&format!(
                    "Failed to decode block descriptor {:?}: {}",
                    path, e
                ))
            })?;
            cached_block = Some(Arc::new(RwLock::new(block_from_disk)));
        }

        self.cached_block_read = cached_block;
        Ok(self.cached_block_read.as_ref().unwrap().clone())
    }

    async fn save(&mut self, block: &Block) -> Result<(), ReductError> {
        // cache is empty, save the block there first
        if self.cached_block_write.is_none() {
            let _ = self
                .cached_block_write
                .insert(Arc::new(RwLock::new(block.clone())));
            return Ok(());
        }

        // update the cached block if we changed it and do nothing
        // we have all needed information in WAL
        {
            let mut cached_block = self
                .cached_block_write
                .as_ref()
                .unwrap()
                .as_ref()
                .write()
                .await;
            if cached_block.begin_time == block.begin_time {
                *cached_block = block.clone();
                return Ok(());
            }

            // we have a new block, let's save the cached one to disk
            debug!(
                "Saving block {:?} and update indexes",
                cached_block.begin_time.as_ref().unwrap()
            );
        }
        self.save_cache_on_disk().await?;
        // update cache
        let _ = self
            .cached_block_write
            .insert(Arc::new(RwLock::new(block.clone())));
        Ok(())
    }

    async fn start(&mut self, block_id: u64, max_block_size: u64) -> Result<BlockRef, ReductError> {
        let mut block = Block::default();
        block.begin_time = Some(us_to_ts(&block_id));

        // create a block with data
        {
            let file = get_global_file_cache()
                .write_or_create(&self.path_to_data(block.begin_time.as_ref().unwrap()))
                .await?;
            let file = file.write().await;
            file.set_len(max_block_size).await?;
        }
        self.save(&block).await?;
        self.block_index.insert_from_block(&block);
        self.load(block_id).await
    }

    async fn finish(&mut self, block: &Block) -> Result<(), ReductError> {
        /* resize data block then sync descriptor and data */
        let path = self.path_to_data(block.begin_time.as_ref().unwrap());
        let file = get_global_file_cache().write_or_create(&path).await?;
        let data_block = file.write().await;
        data_block.set_len(block.size).await?;
        data_block.sync_all().await?;

        let file = get_global_file_cache()
            .write_or_create(&self.path_to_desc(block.begin_time.as_ref().unwrap()))
            .await?;
        let descr_block = file.write().await;
        descr_block.sync_all().await?;

        Ok(())
    }

    async fn remove(&mut self, block_id: u64) -> Result<(), ReductError> {
        if !self.use_counter.clean_stale_and_check(block_id) {
            return Err(ReductError::internal_server_error(&format!(
                "Cannot remove block {} because it is still in use",
                block_id
            )));
        }

        let proto_ts = us_to_ts(&block_id);
        let path = self.path_to_data(&proto_ts);
        get_global_file_cache().remove(&path).await?;
        tokio::fs::remove_file(path).await?;

        let path = self.path_to_desc(&proto_ts);
        get_global_file_cache().remove(&path).await?;
        tokio::fs::remove_file(path).await?;

        Ok(())
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
    block: &Block,
    record_index: usize,
) -> Result<RecordRx, ReductError> {
    let (file, offset) = block_manager
        .write()
        .await
        .begin_read(&block, record_index)
        .await?;

    let (entry, bucket) = {
        let bm = block_manager.read().await;
        (bm.entry.clone(), bm.bucket.clone())
    };

    let record_ts = ts_to_us(&block.records[record_index].timestamp.as_ref().unwrap());
    let (tx, rx) = channel(CHANNEL_BUFFER_SIZE);
    let content_size =
        (block.records[record_index].end - block.records[record_index].begin) as usize;
    let block_id = ts_to_us(&block.begin_time.clone().unwrap());
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
                        .send(Err(ReductError::internal_server_error(&format!(
                            "Failed to read record chunk: {}",
                            e
                        ))))
                        .await;
                    break;
                }
            };

            if read == 0 {
                let _ = tx
                    .send(Err(ReductError::internal_server_error(
                        "Failed to read record chunk: EOF",
                    )))
                    .await;
                break;
            }
            if let Err(e) = tx.send_timeout(Ok(buf.into()), IO_OPERATION_TIMEOUT).await {
                debug!(
                    "Failed to send record {}/{}/{}: {}",
                    bucket, entry, record_ts, e
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
    block: &Block,
    record_index: usize,
) -> Result<RecordTx, ReductError> {
    let (file, offset) = {
        let mut bm = block_manager.write().await;
        bm.save(block).await?;
        bm.begin_write(block, record_index).await?
    };

    let (tx, mut rx) = channel(CHANNEL_BUFFER_SIZE);
    let bm = Arc::clone(&block_manager);
    let block_id = ts_to_us(block.begin_time.as_ref().unwrap());
    let content_size =
        (block.records[record_index].end - block.records[record_index].begin) as usize;
    let record_ts = ts_to_us(&block.records[record_index].timestamp.as_ref().unwrap());

    tokio::spawn(async move {
        let recv = async move {
            let mut written_bytes = 0;
            while let Some(chunk) = timeout(IO_OPERATION_TIMEOUT, rx.recv())
                .await
                .map_err(|_| ReductError::internal_server_error("Timeout while reading record"))?
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
                    bm.bucket, bm.entry, record_ts, err
                );
                if err.status == ErrorCode::InternalServerError {
                    record::State::Invalid
                } else {
                    record::State::Errored
                }
            }
        };

        if let Err(err) = bm
            .write()
            .await
            .finish_write_record(block_id, state, record_index)
            .await
        {
            let bm = bm.read().await;
            error!(
                "Failed to finish writing {}/{}/{} record: {}",
                bm.bucket, bm.entry, record_ts, err
            );
        }
    });
    Ok(tx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::proto::Record;
    use rstest::{fixture, rstest};
    use tempfile::tempdir;
    use tokio::io::AsyncWriteExt;
    use tokio::time::sleep;

    #[rstest]
    #[tokio::test]
    async fn test_starting_block(#[future] block_manager: BlockManager) {
        let mut block_manager = block_manager.await;

        let block = block_manager.start(1_000_005, 1024).await.unwrap();
        let ts = block.begin_time.clone().unwrap();
        assert_eq!(
            ts,
            Timestamp {
                seconds: 1,
                nanos: 5000,
            }
        );

        // Create an empty block
        let file = std::fs::File::open(block_manager.path.join(format!(
            "{}{}",
            ts_to_us(&ts),
            DATA_FILE_EXT
        )))
        .unwrap();
        assert_eq!(file.metadata().unwrap().len(), 1024);

        // Create a block descriptor
        let buf = std::fs::read(block_manager.path.join(format!(
            "{}{}",
            ts_to_us(&ts),
            DESCRIPTOR_FILE_EXT
        )))
        .unwrap();
        let block_from_file = Block::decode(Bytes::from(buf)).unwrap();

        assert_eq!(block_from_file, block);
    }

    #[rstest]
    #[tokio::test]
    async fn test_loading_block(#[future] block_manager: BlockManager, block_id: u64) {
        let mut block_manager = block_manager.await;

        block_manager.start(block_id, 1024).await.unwrap();
        let block = block_manager.start(20000005, 1024).await.unwrap();

        let ts = block.begin_time.clone().unwrap();
        let loaded_block = block_manager.load(ts_to_us(&ts)).await.unwrap();
        assert_eq!(loaded_block, block);
    }

    #[rstest]
    #[tokio::test]
    async fn test_start_reading(#[future] block_manager: BlockManager, block_id: u64) {
        let mut block_manager = block_manager.await;
        let block = block_manager.start(block_id, 1024).await.unwrap();
        let ts = block.begin_time.clone().unwrap();
        let loaded_block = block_manager.load(ts_to_us(&ts)).await.unwrap();
        assert_eq!(loaded_block, block);
    }

    #[rstest]
    #[tokio::test]
    async fn test_finish_block(#[future] block_manager: BlockManager, block_id: u64) {
        let mut block_manager = block_manager.await;
        let block = block_manager.start(block_id + 1, 1024).await.unwrap();
        let ts = block.begin_time.clone().unwrap();
        let loaded_block = block_manager.load(ts_to_us(&ts)).await.unwrap();
        assert_eq!(loaded_block, block);

        block_manager.finish(&loaded_block).await.unwrap();

        let file = std::fs::File::open(block_manager.path.join(format!(
            "{}{}",
            ts_to_us(&ts),
            DATA_FILE_EXT
        )))
        .unwrap();
        assert_eq!(file.metadata().unwrap().len(), 0);
    }

    #[rstest]
    #[tokio::test]
    async fn test_unfinished_writing(
        #[future] block_manager: BlockManager,
        #[future] block: Block,
        block_id: u64,
    ) {
        let block_manager = Arc::new(RwLock::new(block_manager.await));
        let block = block.await;
        let tx = spawn_write_task(Arc::clone(&block_manager), block, 0)
            .await
            .unwrap();

        tx.send(Ok(None)).await.unwrap();
        drop(tx);
        sleep(Duration::from_millis(10)).await; // wait for thread to finish
        assert_eq!(
            block_manager
                .read()
                .await
                .load(block_id)
                .await
                .unwrap()
                .records[0]
                .state,
            2
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_with_writers(
        #[future] block_manager: BlockManager,
        #[future] block: Block,
        block_id: u64,
    ) {
        let block_manager = Arc::new(RwLock::new(block_manager.await));
        let tx = spawn_write_task(Arc::clone(&block_manager), block.await, 0)
            .await
            .unwrap();

        assert_eq!(
            block_manager.write().await.remove(block_id).await.err(),
            Some(ReductError::internal_server_error(&format!(
                "Cannot remove block {} because it is still in use",
                block_id
            )))
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
        #[future] block: Block,
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
        #[future] block: Block,
        block_id: u64,
    ) {
        let block_manager = Arc::new(RwLock::new(block_manager.await));

        let mut rx = spawn_read_task(Arc::clone(&block_manager), &block.await, 0)
            .await
            .unwrap();

        assert_eq!(
            block_manager.write().await.remove(block_id).await.err(),
            Some(ReductError::internal_server_error(&format!(
                "Cannot remove block {} because it is still in use",
                block_id
            )))
        );

        assert_eq!(rx.recv().await.unwrap().unwrap().as_ref(), b"hallo");
        drop(rx);
        sleep(std::time::Duration::from_millis(10)).await; // wait for thread to finish

        assert_eq!(
            block_manager.write().await.remove(block_id).await.err(),
            None
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_block_with_readers_timeout(
        #[future] block_manager: BlockManager,
        #[future] block: Block,
        block_id: u64,
    ) {
        let block_manager = Arc::new(RwLock::new(block_manager.await));
        spawn_read_task(Arc::clone(&block_manager), &block.await, 0)
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
    async fn block(#[future] block_manager: BlockManager, block_id: u64) -> Block {
        block_manager.await.load(block_id).await.unwrap()
    }

    #[fixture]
    async fn block_manager(block_id: u64) -> BlockManager {
        let path = tempdir();
        let mut bm = BlockManager::new(path.unwrap().into_path());
        let mut block = bm.start(block_id, 1024).await.unwrap().clone();
        block.records.push(Record {
            timestamp: Some(Timestamp {
                seconds: 1,
                nanos: 5000,
            }),
            begin: 0,
            end: 5,
            state: 0,
            labels: vec![],
            content_type: "".to_string(),
        });
        bm.save(block.clone()).await.unwrap();

        let mut block_index = BlockIndex::new(bm.path.join(BLOCK_INDEX_FILE));
        block_index.insert_from_block(&block);
        block_index.save().await.unwrap();

        let (file, _) = bm.begin_write(&block, 0).await.unwrap();
        file.write().await.write(b"hallo").await.unwrap();
        bm.finish_write_record(block_id, record::State::Finished, 0)
            .await
            .unwrap();

        bm
    }
}
