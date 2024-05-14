// Copyright 2023-2024 ReductStore
// Licensed under the Business Source License 1.1

mod file_cache;
mod use_counter;

use prost::bytes::{Bytes, BytesMut};
use prost::Message;
use prost_wkt_types::Timestamp;
use std::cmp::min;

use log::{debug, error};

use std::collections::BTreeSet;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::RwLock;

use crate::storage::block_manager::file_cache::FileCache;
use crate::storage::block_manager::use_counter::UseCounter;
use crate::storage::proto::*;
pub(crate) use file_cache::FileRef;
use reduct_base::error::{ErrorCode, ReductError};

pub(crate) const DEFAULT_MAX_READ_CHUNK: usize = 1024 * 512;
const IO_OPERATION_TIMEOUT: Duration = Duration::from_secs(1);
const FILE_CACHE_MAX_SIZE: usize = 64;
const FILE_CACHE_TIME_TO_LIVE: Duration = Duration::from_secs(60);

/// Helper class for basic operations on blocks.
///
/// ## Notes
///
/// It is not thread safe and may cause data corruption if used from multiple threads,
/// because it does not lock the block descriptor file. Use it with RwLock<BlockManager>
pub struct BlockManager {
    path: PathBuf,
    use_counter: UseCounter,
    file_cache: FileCache,
    bucket: String,
    entry: String,
    cached_block: RwLock<Option<Block>>,
}

pub const DESCRIPTOR_FILE_EXT: &str = ".meta";
pub const DATA_FILE_EXT: &str = ".blk";

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
    pub(crate) fn new(path: PathBuf) -> Self {
        let (bucket, entry) = {
            let mut parts = path.iter().rev();
            let entry = parts.next().unwrap().to_str().unwrap().to_string();
            let bucket = parts.next().unwrap().to_str().unwrap().to_string();
            (bucket, entry)
        };

        Self {
            path,
            use_counter: UseCounter::new(IO_OPERATION_TIMEOUT),
            file_cache: FileCache::new(FILE_CACHE_MAX_SIZE, FILE_CACHE_TIME_TO_LIVE),
            bucket,
            entry,

            cached_block: RwLock::new(None),
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
        let file = self.file_cache.write_or_create(&path).await?;
        let offset = block.records[record_index].begin;
        Ok((file, offset as usize))
    }

    async fn finish_write_record(
        &mut self,
        block_id: u64,
        state: record::State,
        record_index: usize,
    ) -> Result<(), ReductError> {
        let mut block = self.load(block_id).await?;
        let record = &mut block.records[record_index];
        let timestamp = ts_to_us(&record.timestamp.as_ref().unwrap());
        record.state = i32::from(state);
        block.invalid = state == record::State::Invalid;

        self.use_counter.decrement(block_id);

        self.save(block).await?;
        debug!(
            "Finished writing record {}/{}/{} with state {:?}",
            self.bucket, self.entry, timestamp, state
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
        let file = self.file_cache.read(&path).await?;
        let offset = block.records[record_index].begin;
        Ok((file, offset as usize))
    }

    pub fn finish_read_record(&mut self, block_id: u64) {
        self.use_counter.decrement(block_id);
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
    async fn load(&self, block_id: u64) -> Result<Block, ReductError>;

    /// Save block descriptor to disk.
    ///
    /// # Arguments
    ///
    /// * `block` - Block to save.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Block was saved successfully.
    async fn save(&mut self, block: Block) -> Result<(), ReductError>;

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
    async fn start(&mut self, block_id: u64, max_block_size: u64) -> Result<Block, ReductError>;

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
    async fn load(&self, block_id: u64) -> Result<Block, ReductError> {
        if let Some(block) = self.cached_block.read().await.as_ref() {
            if ts_to_us(&block.begin_time.clone().unwrap()) == block_id {
                return Ok(block.clone());
            }
        }

        let path = self.path_to_desc(&us_to_ts(&block_id));
        let file = self.file_cache.read(&path).await?;
        let mut buf = vec![];

        // parse the block descriptor
        let mut lock = file.write().await;
        lock.seek(SeekFrom::Start(0)).await?;
        lock.read_to_end(&mut buf).await?;

        let block = Block::decode(Bytes::from(buf)).map_err(|e| {
            ReductError::internal_server_error(&format!(
                "Failed to decode block descriptor {:?}: {}",
                path, e
            ))
        })?;

        let mut lock = self.cached_block.write().await;
        let _ = lock.insert(block.clone());
        Ok(block)
    }

    async fn save(&mut self, block: Block) -> Result<(), ReductError> {
        let path = self.path_to_desc(block.begin_time.as_ref().unwrap());
        let mut buf = BytesMut::new();
        block.encode(&mut buf).map_err(|e| {
            ReductError::internal_server_error(&format!(
                "Failed to encode block descriptor {:?}: {}",
                path, e
            ))
        })?;

        // overwrite the file
        let file = self.file_cache.write_or_create(&path).await?;
        let mut lock = file.write().await;
        lock.set_len(buf.len() as u64).await?;
        lock.seek(SeekFrom::Start(0)).await?;
        lock.write_all(&buf).await?;
        lock.flush().await?;

        let mut lock = self.cached_block.write().await;
        if let Some(last_block) = lock.as_ref() {
            if ts_to_us(&last_block.begin_time.clone().unwrap())
                == ts_to_us(&block.begin_time.clone().unwrap())
            {
                *lock = Some(block.clone());
            }
        }
        Ok(())
    }

    async fn start(&mut self, block_id: u64, max_block_size: u64) -> Result<Block, ReductError> {
        let mut block = Block::default();
        block.begin_time = Some(us_to_ts(&block_id));

        // create a block with data
        {
            let file = self
                .file_cache
                .write_or_create(&self.path_to_data(block.begin_time.as_ref().unwrap()))
                .await?;
            let file = file.write().await;
            file.set_len(max_block_size).await?;
        }
        self.save(block.clone()).await?;

        Ok(block)
    }

    async fn finish(&mut self, block: &Block) -> Result<(), ReductError> {
        /* resize data block then sync descriptor and data */
        let path = self.path_to_data(block.begin_time.as_ref().unwrap());
        let file = self.file_cache.write_or_create(&path).await?;
        let data_block = file.write().await;
        data_block.set_len(block.size).await?;
        data_block.sync_all().await?;

        let file = self
            .file_cache
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
        self.file_cache.remove(&path).await?;
        tokio::fs::remove_file(path).await?;

        let path = self.path_to_desc(&proto_ts);
        self.file_cache.remove(&path).await?;
        tokio::fs::remove_file(path).await?;

        let mut lock = self.cached_block.write().await;
        if let Some(block) = lock.as_ref() {
            if ts_to_us(&block.begin_time.clone().unwrap()) == block_id {
                *lock = None; // invalidate the last block
            }
        }

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

    let record_ts = ts_to_us(&block.records[record_index].timestamp.as_ref().unwrap());
    let (tx, rx) = channel(1);
    let content_size =
        (block.records[record_index].end - block.records[record_index].begin) as usize;
    let block_id = ts_to_us(&block.begin_time.clone().unwrap());
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
            if let Err(e) = tx.send(Ok(Bytes::from(buf))).await {
                let bm = block_manager.read().await;
                debug!(
                    "Failed to send record {}/{}/{}: {}",
                    bm.bucket, bm.entry, record_ts, e
                ); // for some reason the receiver is closed
                break;
            }

            read_bytes += read;
            if read_bytes == content_size {
                break;
            }

            block_manager.write().await.use_counter.update(block_id);
        }

        block_manager.write().await.finish_read_record(block_id);
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
    block: Block,
    record_index: usize,
) -> Result<RecordTx, ReductError> {
    let (file, offset) = {
        let mut bm = block_manager.write().await;
        bm.save(block.clone()).await?;
        bm.begin_write(&block, record_index).await?
    };

    let (tx, mut rx) = channel(1);
    let bm = Arc::clone(&block_manager);
    let block_id = ts_to_us(block.begin_time.as_ref().unwrap());
    let content_size =
        (block.records[record_index].end - block.records[record_index].begin) as usize;
    let record_ts = ts_to_us(&block.records[record_index].timestamp.as_ref().unwrap());

    tokio::spawn(async move {
        let recv = async move {
            let mut written_bytes = 0;
            while let Some(chunk) = rx.recv().await {
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

        let (file, _) = bm.begin_write(&block, 0).await.unwrap();
        file.write().await.write(b"hallo").await.unwrap();
        bm.finish_write_record(block_id, record::State::Finished, 0)
            .await
            .unwrap();

        bm
    }
}
