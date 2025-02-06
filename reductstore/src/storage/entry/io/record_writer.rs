// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::core::file_cache::FileWeak;
use crate::core::thread_pool::GroupDepth::BLOCK;
use crate::core::thread_pool::{group_from_path, shared_child_isolated};
use crate::storage::block_manager::{BlockManager, BlockRef, RecordTx};
use crate::storage::proto::record;
use crate::storage::storage::{CHANNEL_BUFFER_SIZE, MAX_IO_BUFFER_SIZE};
use async_trait::async_trait;
use bytes::Bytes;
use log::error;
use reduct_base::bad_request;
use reduct_base::error::ReductError;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{channel, Receiver};
use tokio::time::error::Elapsed;

type Chunk = Result<Option<Bytes>, ReductError>;
type Rx = Receiver<Chunk>;

#[async_trait]
pub(crate) trait WriteRecordContent {
    /// Sends a chunk of the record content.
    ///
    /// Stops the writer if the chunk is an error or None.
    async fn send(&mut self, chunk: Chunk) -> Result<(), SendError<Chunk>>;

    #[cfg(test)]
    fn blocking_send(&mut self, chunk: Chunk) -> Result<(), SendError<Chunk>>;

    async fn send_timeout(
        &mut self,
        chunk: Chunk,
        timeout: Duration,
    ) -> Result<Result<(), SendError<Chunk>>, Elapsed>;
}

/// RecordWriter is responsible for writing the content of a record to the storage.
pub(crate) struct RecordWriter {
    tx: RecordTx,
    lazy_write: Option<(Rx, WriteContext)>, // we write to file when the writer is dropped for small records
}

struct WriteContext {
    bucket_name: String,
    entry_name: String,
    block_id: u64,
    record_timestamp: u64,
    file_ref: FileWeak,
    offset: u64,
    content_size: u64,
    block_manager: Arc<RwLock<BlockManager>>,
    task_group: String,
}

impl RecordWriter {
    /// Creates a new RecordWriter.
    ///
    /// It spins up a new task to write the record content to the storage and provides a channel to send the record content.
    ///
    /// # Arguments
    ///
    /// * `block_manager` - The block manager.
    /// * `block_ref` - The block reference.
    /// * `time` - The timestamp of the record.
    ///
    /// # Returns
    ///
    /// * `RecordWriter` - The record writer.
    pub(in crate::storage) fn try_new(
        block_manager: Arc<RwLock<BlockManager>>,
        block_ref: BlockRef,
        time: u64,
    ) -> Result<Self, ReductError> {
        let (file_ref, offset, bucket_name, entry_name, task_group) = {
            let mut bm = block_manager.write()?;
            let block = block_ref.read()?;

            let (file, offset) = {
                bm.index_mut().insert_or_update(block.to_owned());
                bm.begin_write_record(&block, time)?
            };

            bm.save_block(block_ref.clone())?;

            let entry_path = bm.path();
            let task_group =
                group_from_path(&entry_path.join(&block.block_id().to_string()), BLOCK);
            (
                file,
                offset,
                bm.bucket_name().to_string(),
                bm.entry_name().to_string(),
                task_group,
            )
        };

        let block = block_ref.read()?;
        let block_id = block.block_id();
        let record_index = block.get_record(time).unwrap();
        let content_size = record_index.end - record_index.begin;

        let (tx, rx) = channel(CHANNEL_BUFFER_SIZE);

        let mut me = RecordWriter {
            tx,
            lazy_write: None,
        };

        let ctx = WriteContext {
            bucket_name,
            entry_name,
            block_id,
            record_timestamp: time,
            file_ref,
            offset,
            content_size,
            block_manager,
            task_group,
        };

        if content_size >= MAX_IO_BUFFER_SIZE as u64 {
            shared_child_isolated(&ctx.task_group.clone(), "write record content", move || {
                Self::receive(rx, ctx);
            });
        } else {
            me.lazy_write = Some((rx, ctx));
        }

        Ok(me)
    }

    fn receive(mut rx: Rx, ctx: WriteContext) {
        let mut recv = || {
            let mut written_bytes = 0u64;
            while let Some(chunk) = rx.blocking_recv() {
                let chunk: Option<Bytes> = chunk?;
                match chunk {
                    Some(chunk) => {
                        written_bytes += chunk.len() as u64;
                        if written_bytes > ctx.content_size {
                            return Err(bad_request!("Content is bigger than in content-length",));
                        }

                        {
                            let rc = ctx.file_ref.upgrade()?;
                            let mut lock = rc.write()?;
                            lock.seek(SeekFrom::Start(
                                ctx.offset + written_bytes - chunk.len() as u64,
                            ))?;
                            lock.write_all(chunk.as_ref())?;
                        }
                    }
                    None => {
                        break;
                    }
                }
            }

            if written_bytes < ctx.content_size {
                Err(bad_request!("Content is smaller than in content-length",))
            } else {
                ctx.file_ref.upgrade()?.write()?.flush()?;
                Ok(())
            }
        };

        let state = match recv() {
            Ok(_) => record::State::Finished,
            Err(err) => {
                error!(
                    "Failed to write record {}/{}/{}: {}",
                    ctx.bucket_name, ctx.entry_name, ctx.record_timestamp, err
                );
                record::State::Errored
            }
        };

        if let Err(err) = ctx.block_manager.write().unwrap().finish_write_record(
            ctx.block_id,
            state,
            ctx.record_timestamp,
        ) {
            error!(
                "Failed to finish writing {}/{}/{} record: {}",
                ctx.bucket_name, ctx.entry_name, ctx.record_timestamp, err
            );
        }
    }
}

/// Drains the record content and discards it.
pub(crate) struct RecordDrainer {}

impl RecordDrainer {
    pub fn new() -> Self {
        RecordDrainer {}
    }
}

#[async_trait]
impl WriteRecordContent for RecordDrainer {
    async fn send(
        &mut self,
        _chunk: Result<Option<Bytes>, ReductError>,
    ) -> Result<(), SendError<Chunk>> {
        Ok(())
    }

    #[cfg(test)]
    fn blocking_send(&mut self, _chunk: Chunk) -> Result<(), SendError<Chunk>> {
        Ok(())
    }

    async fn send_timeout(
        &mut self,
        _chunk: Chunk,
        _timeout: Duration,
    ) -> Result<Result<(), SendError<Chunk>>, Elapsed> {
        Ok(Ok(()))
    }
}

#[async_trait]
impl WriteRecordContent for RecordWriter {
    async fn send(
        &mut self,
        chunk: Result<Option<Bytes>, ReductError>,
    ) -> Result<(), SendError<Chunk>> {
        let stop = chunk.is_err() || chunk.as_ref().unwrap().is_none();
        self.tx.send(chunk).await?;

        if stop {
            if let Some((rx, ctx)) = self.lazy_write.take() {
                // tokio::spawn(async move {  Self::receive(rx, ctx); });
                tokio::task::spawn_blocking(|| Self::receive(rx, ctx));
                // Self::receive(rx, ctx);
            }
            self.tx.closed().await;
        }

        Ok(())
    }

    #[cfg(test)]
    fn blocking_send(&mut self, chunk: Chunk) -> Result<(), SendError<Chunk>> {
        let stop = chunk.is_err() || chunk.as_ref().unwrap().is_none();
        self.tx.blocking_send(chunk)?;

        if stop {
            if let Some((rx, ctx)) = self.lazy_write.take() {
                Self::receive(rx, ctx);
            }
        }

        Ok(())
    }

    async fn send_timeout(
        &mut self,
        chunk: Chunk,
        timeout: Duration,
    ) -> Result<Result<(), SendError<Chunk>>, Elapsed> {
        tokio::time::timeout(timeout, self.send(chunk)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    mod record_writer {
        use super::*;
        use crate::core::thread_pool::find_task_group;
        use crate::storage::block_manager::block_index::BlockIndex;
        use crate::storage::entry::tests::get_task_group;
        use crate::storage::proto::{us_to_ts, Record};
        use rstest::fixture;
        use std::fs;
        use std::io::Read;
        use std::path::PathBuf;
        use std::time::Duration;
        use tempfile::tempdir;
        use tokio::time::sleep;

        const SMALL_RECORD_TIME: u64 = 1;
        const BIG_RECORD_TIME: u64 = 2;

        #[rstest]
        #[tokio::test]
        async fn test_small_ok(block_manager: Arc<RwLock<BlockManager>>, block_ref: BlockRef) {
            let mut writer =
                RecordWriter::try_new(block_manager.clone(), block_ref, SMALL_RECORD_TIME).unwrap();

            writer.send(Ok(Some(Bytes::from("te")))).await.unwrap();
            writer.send(Ok(Some(Bytes::from("st")))).await.unwrap();
            writer.send(Ok(None)).await.unwrap();

            let block_ref = block_manager
                .read()
                .unwrap()
                .load_block(SMALL_RECORD_TIME)
                .unwrap();
            assert_eq!(
                block_ref
                    .read()
                    .unwrap()
                    .get_record(SMALL_RECORD_TIME)
                    .unwrap()
                    .state,
                record::State::Finished as i32
            );

            let mut content = vec![0u8; 4];
            fs::File::open(block_manager.read().unwrap().path().join("1.blk"))
                .unwrap()
                .read(&mut content)
                .unwrap();

            assert_eq!(content, b"test");
        }

        #[rstest]
        #[tokio::test]
        async fn test_big_ok(block_manager: Arc<RwLock<BlockManager>>, block_ref: BlockRef) {
            let mut writer =
                RecordWriter::try_new(block_manager.clone(), block_ref, BIG_RECORD_TIME).unwrap();

            sleep(Duration::from_millis(100)).await;
            let path = block_manager.read().unwrap().path().clone();
            assert!(
                find_task_group(&get_task_group(&path, 1)).is_some(),
                "task is running"
            );

            let content = vec![0xaau8; MAX_IO_BUFFER_SIZE + 1];
            writer
                .send(Ok(Some(Bytes::from(content.clone()))))
                .await
                .unwrap();
            writer.send(Ok(None)).await.unwrap();

            sleep(Duration::from_millis(100)).await;
            assert!(
                find_task_group(&get_task_group(&path, 1)).is_none(),
                "task is finished"
            );

            let block_ref = block_manager.read().unwrap().load_block(1).unwrap();
            assert_eq!(
                block_ref
                    .read()
                    .unwrap()
                    .get_record(BIG_RECORD_TIME)
                    .unwrap()
                    .state,
                record::State::Finished as i32
            );

            let mut block_content = vec![0u8; content.len() + 4];
            fs::File::open(block_manager.read().unwrap().path().join("1.blk"))
                .unwrap()
                .read(&mut block_content)
                .unwrap();

            assert_eq!(content, block_content[4..]);
        }

        #[rstest]
        #[tokio::test]
        async fn test_too_long(block_manager: Arc<RwLock<BlockManager>>, block_ref: BlockRef) {
            let mut writer =
                RecordWriter::try_new(block_manager.clone(), block_ref, SMALL_RECORD_TIME).unwrap();
            writer
                .send(Ok(Some(Bytes::from("xxxxxxxxx"))))
                .await
                .unwrap();
            writer.send(Ok(None)).await.unwrap();

            tokio::time::sleep(Duration::from_millis(100)).await;

            let block_ref = block_manager.read().unwrap().load_block(1).unwrap();
            assert_eq!(
                block_ref.read().unwrap().get_record(1).unwrap().state,
                record::State::Errored as i32
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_too_short(block_manager: Arc<RwLock<BlockManager>>, block_ref: BlockRef) {
            let mut writer =
                RecordWriter::try_new(block_manager.clone(), block_ref, SMALL_RECORD_TIME).unwrap();
            writer.send(Ok(Some(Bytes::from("xx")))).await.unwrap();
            writer.send(Ok(None)).await.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;

            let block_ref = block_manager.read().unwrap().load_block(1).unwrap();
            assert_eq!(
                block_ref.read().unwrap().get_record(1).unwrap().state,
                record::State::Errored as i32
            );
        }

        #[fixture]
        fn path() -> PathBuf {
            tempdir().unwrap().into_path().join("bucket").join("entry")
        }

        #[fixture]
        fn block_manager(path: PathBuf) -> Arc<RwLock<BlockManager>> {
            Arc::new(RwLock::new(BlockManager::new(
                path.clone(),
                BlockIndex::new(path.clone()),
            )))
        }

        #[fixture]
        fn block_ref(block_manager: Arc<RwLock<BlockManager>>) -> BlockRef {
            let block_ref = block_manager
                .write()
                .unwrap()
                .start_new_block(1, 1000)
                .unwrap();
            block_ref.write().unwrap().insert_or_update_record(Record {
                timestamp: Some(us_to_ts(&SMALL_RECORD_TIME)),
                begin: 0,
                end: 4,
                state: record::State::Started as i32,
                labels: vec![],
                content_type: "".to_string(),
            });

            block_ref.write().unwrap().insert_or_update_record(Record {
                timestamp: Some(us_to_ts(&BIG_RECORD_TIME)),
                begin: 4,
                end: MAX_IO_BUFFER_SIZE as u64 + 5,
                state: record::State::Started as i32,
                labels: vec![],
                content_type: "".to_string(),
            });
            block_ref
        }
    }

    mod record_drainer {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_does_nothing() {
            let mut drainer = RecordDrainer::new();
            drainer.send(Ok(Some(Bytes::from("test")))).await.unwrap();
            drainer.send(Ok(None)).await.unwrap();
        }
    }
}
