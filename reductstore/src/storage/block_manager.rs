// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use prost::bytes::{Bytes, BytesMut};
use prost::Message;
use prost_wkt_types::Timestamp;
use std::cmp::min;

use log::error;
use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap};
use std::io::{SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::RwLock;

use crate::storage::proto::*;
use reduct_base::error::{ErrorCode, ReductError};

pub const DEFAULT_MAX_READ_CHUNK: usize = 1024 * 512;

/// Helper class for basic operations on blocks.
///
/// ## Notes
///
/// It is not thread safe and may cause data corruption if used from multiple threads,
/// because it does not lock the block descriptor file. Use it with RwLock<BlockManager>
pub struct BlockManager {
    path: PathBuf,
    reader_count: HashMap<u64, usize>,
    writer_count: HashMap<u64, usize>,

    last_block: Option<Block>,
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
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            reader_count: HashMap::new(),
            writer_count: HashMap::new(),
            last_block: None,
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
    ) -> Result<File, ReductError> {
        let ts = block.begin_time.clone().unwrap();
        let path = self.path_to_data(&ts);

        let block_id = ts_to_us(&ts);
        match self.writer_count.entry(block_id) {
            Entry::Occupied(mut e) => {
                *e.get_mut() += 1;
            }
            Entry::Vacant(e) => {
                e.insert(1);
            }
        }

        let mut file = File::options().write(true).create(true).open(path).await?;
        let offset = block.records[record_index].begin;
        file.seek(SeekFrom::Start(offset)).await?;

        Ok(file)
    }

    pub fn finish_write_record(
        &mut self,
        block_id: u64,
        state: record::State,
        record_index: usize,
    ) -> Result<(), ReductError> {
        let mut block = self.load(block_id)?;
        block.records[record_index].state = i32::from(state);
        block.invalid = state == record::State::Invalid;

        *self.writer_count.get_mut(&block_id).unwrap() -= 1;
        self.clean_readers_or_writers(block_id);
        self.save(block)
    }

    pub async fn begin_read(
        &mut self,
        block: &Block,
        record_index: usize,
    ) -> Result<File, ReductError> {
        let ts = block.begin_time.clone().unwrap();
        let path = self.path_to_data(&ts);

        let block_id = ts_to_us(&ts);
        match self.reader_count.entry(block_id) {
            Entry::Occupied(mut e) => {
                *e.get_mut() += 1;
            }
            Entry::Vacant(e) => {
                e.insert(1);
            }
        }

        let mut file = File::options().read(true).open(path).await?;
        let offset = block.records[record_index].begin;
        file.seek(SeekFrom::Start(offset)).await?;
        Ok(file)
    }

    pub fn finish_read_record(&mut self, block_id: u64) {
        *self.reader_count.get_mut(&block_id).unwrap() -= 1;
        self.clean_readers_or_writers(block_id);
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

    /// Remove done or expired readers/writers of a block.
    ///
    /// # Arguments
    ///
    /// * `block_id` - ID of the block to clean.
    ///
    /// # Returns
    ///
    /// * `true` - If there are no more readers or writers.
    fn clean_readers_or_writers(&mut self, block_id: u64) -> bool {
        let readers_empty = match self.reader_count.get_mut(&block_id) {
            Some(count) => {
                if *count == 0 {
                    self.reader_count.remove(&block_id);
                    true
                } else {
                    false
                }
            }
            None => true,
        };

        let writers_empty = match self.writer_count.get_mut(&block_id) {
            Some(count) => {
                if *count == 0 {
                    self.writer_count.remove(&block_id);
                    true
                } else {
                    false
                }
            }
            None => true,
        };

        readers_empty && writers_empty
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
    fn load(&self, block_id: u64) -> Result<Block, ReductError>;

    /// Save block descriptor to disk.
    ///
    /// # Arguments
    ///
    /// * `block` - Block to save.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Block was saved successfully.
    fn save(&mut self, block: Block) -> Result<(), ReductError>;

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
    fn start(&mut self, block_id: u64, max_block_size: u64) -> Result<Block, ReductError>;

    /// Finish a block by truncating the file to the actual size.
    ///
    /// # Arguments
    ///
    /// * `block` - Block to finish.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Block was finished successfully.
    fn finish(&mut self, block: &Block) -> Result<(), ReductError>;

    /// Remove a block from disk if there are no readers or writers.
    fn remove(&mut self, block_id: u64) -> Result<(), ReductError>;
}

impl ManageBlock for BlockManager {
    fn load(&self, block_id: u64) -> Result<Block, ReductError> {
        if let Some(block) = self.last_block.as_ref() {
            if ts_to_us(&block.begin_time.clone().unwrap()) == block_id {
                return Ok(block.clone());
            }
        }

        let proto_ts = us_to_ts(&block_id);
        let buf = std::fs::read(self.path_to_desc(&proto_ts))?;
        let block = Block::decode(Bytes::from(buf)).map_err(|e| {
            ReductError::internal_server_error(&format!("Failed to decode block descriptor: {}", e))
        })?;

        Ok(block)
    }

    fn save(&mut self, block: Block) -> Result<(), ReductError> {
        let path = self.path_to_desc(block.begin_time.as_ref().unwrap());
        let mut buf = BytesMut::new();
        block.encode(&mut buf).map_err(|e| {
            ReductError::internal_server_error(&format!("Failed to encode block descriptor: {}", e))
        })?;
        let mut file = std::fs::File::create(path.clone())?;
        file.write_all(&buf)?;

        self.last_block = Some(block);
        Ok(())
    }

    fn start(&mut self, block_id: u64, max_block_size: u64) -> Result<Block, ReductError> {
        let mut block = Block::default();
        block.begin_time = Some(us_to_ts(&block_id));

        // create a block with data
        let file = std::fs::File::create(self.path_to_data(block.begin_time.as_ref().unwrap()))?;

        file.set_len(max_block_size)?;
        self.save(block.clone())?;

        Ok(block)
    }

    fn finish(&mut self, block: &Block) -> Result<(), ReductError> {
        /* resize data block then sync descriptor and data */
        let path = self.path_to_data(block.begin_time.as_ref().unwrap());
        let data_block = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
        data_block.set_len(block.size)?;
        data_block.sync_all()?;

        let descr_block = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.path_to_desc(block.begin_time.as_ref().unwrap()))?;
        descr_block.sync_all()?;

        self.last_block = None;
        Ok(())
    }

    fn remove(&mut self, block_id: u64) -> Result<(), ReductError> {
        if !self.clean_readers_or_writers(block_id) {
            return Err(ReductError::internal_server_error(&format!(
                "Cannot remove block {} because it is still in use",
                block_id
            )));
        }

        let proto_ts = us_to_ts(&block_id);
        let path = self.path_to_data(&proto_ts);
        std::fs::remove_file(path)?;
        let path = self.path_to_desc(&proto_ts);
        std::fs::remove_file(path)?;

        if let Some(block) = self.last_block.as_ref() {
            if ts_to_us(&block.begin_time.clone().unwrap()) == block_id {
                self.last_block = None;
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
    let file = block_manager
        .write()
        .await
        .begin_read(&block, record_index)
        .await?;

    let (tx, rx) = channel(1);
    let content_size =
        (block.records[record_index].end - block.records[record_index].begin) as usize;
    let block_id = ts_to_us(&block.begin_time.clone().unwrap());
    tokio::spawn(async move {
        let mut file = file;
        let mut offset = 0;
        loop {
            let chunk_size = min(content_size - offset, DEFAULT_MAX_READ_CHUNK) as usize;
            let mut buf = vec![0; chunk_size];

            let read = match file.read(&mut buf).await {
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
                error!("Failed to send record chunk: {}", e);
                break;
            }

            offset += read;
            if offset == content_size {
                break;
            }
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
    let mut file = {
        let mut bm = block_manager.write().await;
        bm.save(block.clone())?;
        bm.begin_write(&block, record_index).await?
    };

    let (tx, mut rx) = channel(1);
    let bm = Arc::clone(&block_manager);
    let block_id = ts_to_us(block.begin_time.as_ref().unwrap());
    let content_size =
        (block.records[record_index].end - block.records[record_index].begin) as usize;
    tokio::spawn(async move {
        let recv = async move {
            let written_bytes = None;
            while let Some(chunk) = rx.recv().await {
                let written_bytes =
                    write_transaction(&content_size, &mut file, written_bytes, chunk).await?;

                if written_bytes.is_none() || written_bytes >= Some(content_size) {
                    break;
                }
            }

            if written_bytes.is_some_and(|x| x < content_size) {
                Err(ReductError::bad_request(
                    "Content is smaller than in content-length",
                ))
            } else {
                file.flush().await?;
                Ok(())
            }
        };

        let state = match recv.await {
            Ok(_) => record::State::Finished,
            Err(err) => {
                error!("Failed to write record: {}", err);
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
        {
            error!("Failed to finish writing record: {}", err);
        }
    });
    Ok(tx)
}

/// Write a transaction to a file.
///
/// # Arguments
///
/// * `content_size` - Size of the content to write.
/// * `file` - File to write to.
/// * `written_bytes` - Number of bytes already written.
/// * `chunk` - Chunk to write.
///
/// # Returns
///
/// * `Ok(written_bytes)` - Number of bytes written.
/// * `Ok(None)` - If the chunk is None. This means that the transaction is finished.
async fn write_transaction(
    content_size: &usize,
    file: &mut File,
    written_bytes: Option<usize>,
    chunk: Result<Option<Bytes>, ReductError>,
) -> Result<Option<usize>, ReductError> {
    let chunk = chunk?;
    if chunk.is_none() {
        return Ok(None);
    }

    let mut written_bytes = written_bytes.unwrap_or(0);

    let chunk = chunk.unwrap();
    written_bytes += chunk.len();
    if written_bytes > *content_size {
        return Err(ReductError::bad_request(
            "Content is bigger than in content-length",
        ));
    }
    file.write_all(chunk.as_ref()).await?;
    Ok(Some(written_bytes))
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

        let block = block_manager.start(1_000_005, 1024).unwrap();
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

        block_manager.start(block_id, 1024).unwrap();
        let block = block_manager.start(20000005, 1024).unwrap();

        let ts = block.begin_time.clone().unwrap();
        let loaded_block = block_manager.load(ts_to_us(&ts)).unwrap();
        assert_eq!(loaded_block, block);
    }

    #[rstest]
    #[tokio::test]
    async fn test_start_reading(#[future] block_manager: BlockManager, block_id: u64) {
        let mut block_manager = block_manager.await;
        let block = block_manager.start(block_id, 1024).unwrap();
        let ts = block.begin_time.clone().unwrap();
        let loaded_block = block_manager.load(ts_to_us(&ts)).unwrap();
        assert_eq!(loaded_block, block);
    }

    #[rstest]
    #[tokio::test]
    async fn test_finish_block(#[future] block_manager: BlockManager, block_id: u64) {
        let mut block_manager = block_manager.await;
        let block = block_manager.start(block_id + 1, 1024).unwrap();
        let ts = block.begin_time.clone().unwrap();
        let loaded_block = block_manager.load(ts_to_us(&ts)).unwrap();
        assert_eq!(loaded_block, block);

        block_manager.finish(&loaded_block).unwrap();

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
            block_manager.write().await.remove(block_id).err(),
            Some(ReductError::internal_server_error(&format!(
                "Cannot remove block {} because it is still in use",
                block_id
            )))
        );

        tx.send(Ok(Some(Bytes::from("hallo")))).await.unwrap();
        drop(tx);
        sleep(std::time::Duration::from_millis(10)).await; // wait for thread to finish
        assert_eq!(block_manager.write().await.remove(block_id).err(), None);
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
            block_manager.write().await.remove(block_id).err(),
            Some(ReductError::internal_server_error(&format!(
                "Cannot remove block {} because it is still in use",
                block_id
            )))
        );

        assert_eq!(rx.recv().await.unwrap().unwrap().as_ref(), b"hallo");
        drop(rx);
        sleep(std::time::Duration::from_millis(10)).await; // wait for thread to finish

        assert_eq!(block_manager.write().await.remove(block_id).err(), None);
    }

    #[fixture]
    fn block_id() -> u64 {
        1
    }

    #[fixture]
    async fn block(#[future] block_manager: BlockManager, block_id: u64) -> Block {
        block_manager.await.load(block_id).unwrap()
    }

    #[fixture]
    async fn block_manager(block_id: u64) -> BlockManager {
        let path = tempdir();
        let mut bm = BlockManager::new(path.unwrap().into_path());
        let mut block = bm.start(block_id, 1024).unwrap().clone();
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
        bm.save(block.clone()).unwrap();

        let mut file = bm.begin_write(&block, 0).await.unwrap();
        file.write(b"hallo").await.unwrap();
        bm.finish_write_record(block_id, record::State::Finished, 0)
            .unwrap();

        bm
    }
}
