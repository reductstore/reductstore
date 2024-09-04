// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::block_manager::{BlockManager, BlockRef, RecordTx};
use crate::storage::file_cache::FileWeak;
use crate::storage::proto::record;
use crate::storage::storage::{CHANNEL_BUFFER_SIZE, IO_OPERATION_TIMEOUT};
use bytes::Bytes;
use log::error;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::io::SeekFrom;
use std::sync::Arc;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc::{channel, Receiver};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::timeout;

pub(crate) trait WriteRecordContent {
    fn tx(&self) -> &RecordTx;
}

/// RecordWriter is responsible for writing the content of a record to the storage.
pub(crate) struct RecordWriter {
    io_task_handle: Option<JoinHandle<()>>,
    tx: RecordTx,
}

impl Drop for RecordWriter {
    fn drop(&mut self) {
        if let Some(handle) = self.io_task_handle.take() {
            handle.abort(); // Abort the task if it is still running
        }
    }
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
    pub(in crate::storage) async fn try_new(
        block_manager: Arc<RwLock<BlockManager>>,
        block_ref: BlockRef,
        time: u64,
    ) -> Result<Self, ReductError> {
        let block = block_ref.read().await;
        let (file_ref, offset, bucket_name, entry_name) = {
            let mut bm = block_manager.write().await;

            let (file, offset) = {
                bm.index_mut().insert_or_update(block.to_owned());
                bm.begin_write_record(&block, time).await?
            };

            bm.save_block(block_ref.clone()).await?;
            (
                file,
                offset,
                bm.bucket_name().to_string(),
                bm.entry_name().to_string(),
            )
        };

        let block_id = block.block_id();
        let record_index = block.get_record(time).unwrap();
        let content_size = record_index.end - record_index.begin;

        let (tx, rx) = channel(CHANNEL_BUFFER_SIZE);

        let mut me = RecordWriter {
            io_task_handle: None,
            tx,
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
        };

        // Spawn a task to write the record if it is bigger than the buffer size
        let handle = tokio::spawn(async move {
            Self::receive(rx, ctx).await;
        });

        me.io_task_handle = Some(handle);

        Ok(me)
    }

    async fn receive(mut rx: Receiver<Result<Option<Bytes>, ReductError>>, ctx: WriteContext) {
        let recv = async {
            let mut written_bytes = 0u64;
            while let Some(chunk) = timeout(IO_OPERATION_TIMEOUT, rx.recv())
                .await
                .map_err(|_| internal_server_error!("Timeout while reading record"))?
            {
                let chunk: Option<Bytes> = chunk?;
                match chunk {
                    Some(chunk) => {
                        written_bytes += chunk.len() as u64;
                        if written_bytes > ctx.content_size {
                            return Err(ReductError::bad_request(
                                "Content is bigger than in content-length",
                            ));
                        }

                        {
                            let mut rc = ctx.file_ref.upgrade()?;
                            let mut lock = rc.write().await;
                            lock.seek(SeekFrom::Start(
                                ctx.offset + written_bytes - chunk.len() as u64,
                            ))
                            .await?;
                            lock.write_all(chunk.as_ref()).await?;
                        }
                    }
                    None => {
                        break;
                    }
                }

                if written_bytes >= ctx.content_size {
                    break;
                }

                ctx.block_manager
                    .write()
                    .await
                    .use_counter_mut()
                    .update(ctx.block_id);
            }

            if written_bytes < ctx.content_size {
                Err(ReductError::bad_request(
                    "Content is smaller than in content-length",
                ))
            } else {
                ctx.file_ref.upgrade()?.write().await.flush().await?;
                Ok(())
            }
        };

        let state = match recv.await {
            Ok(_) => record::State::Finished,
            Err(err) => {
                error!(
                    "Failed to write record {}/{}/{}: {}",
                    ctx.bucket_name, ctx.entry_name, ctx.record_timestamp, err
                );
                record::State::Errored
            }
        };

        if let Err(err) = ctx
            .block_manager
            .write()
            .await
            .finish_write_record(ctx.block_id, state, ctx.record_timestamp)
            .await
        {
            error!(
                "Failed to finish writing {}/{}/{} record: {}",
                ctx.bucket_name, ctx.entry_name, ctx.record_timestamp, err
            );
        }
    }
}

/// Drains the record content and discards it.
pub(crate) struct RecordDrainer {
    tx: RecordTx,
    io_task_handle: JoinHandle<()>,
}

impl Drop for RecordDrainer {
    fn drop(&mut self) {
        self.io_task_handle.abort();
    }
}

impl RecordDrainer {
    pub fn new() -> Self {
        let (tx, mut rx) = channel(1);
        let handle = tokio::spawn(async move {
            while let Some(chunk) = rx.recv().await {
                if let Ok(None) = chunk {
                    // sync the channel
                    break;
                }
            }
        });

        RecordDrainer {
            tx,
            io_task_handle: handle,
        }
    }
}

impl WriteRecordContent for RecordDrainer {
    fn tx(&self) -> &RecordTx {
        &self.tx
    }
}

impl WriteRecordContent for RecordWriter {
    fn tx(&self) -> &RecordTx {
        &self.tx
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    mod record_writer {
        use super::*;
        use crate::storage::block_manager::block_index::BlockIndex;
        use crate::storage::proto::{us_to_ts, Record};
        use rstest::fixture;
        use std::fs;
        use std::io::Read;
        use std::path::PathBuf;
        use std::time::Duration;
        use tempfile::tempdir;

        #[rstest]
        #[tokio::test]
        async fn test_ok(block_manager: Arc<RwLock<BlockManager>>, #[future] block_ref: BlockRef) {
            let block_ref = block_ref.await;

            let writer = RecordWriter::try_new(block_manager.clone(), block_ref, 1)
                .await
                .unwrap();
            writer.tx().send(Ok(Some(Bytes::from("te")))).await.unwrap();
            writer.tx().send(Ok(Some(Bytes::from("st")))).await.unwrap();
            writer.tx().send(Ok(None)).await.unwrap();

            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            assert!(
                writer.io_task_handle.as_ref().unwrap().is_finished(),
                "task is finished"
            );

            let block_ref = block_manager.read().await.load_block(1).await.unwrap();
            assert_eq!(
                block_ref.read().await.get_record(1).unwrap().state,
                record::State::Finished as i32
            );

            let mut content = vec![0u8; 4];
            fs::File::open(block_manager.read().await.path().join("1.blk"))
                .unwrap()
                .read(&mut content)
                .unwrap();

            assert_eq!(content, b"test");
        }

        #[rstest]
        #[tokio::test]
        async fn test_too_long(
            block_manager: Arc<RwLock<BlockManager>>,
            #[future] block_ref: BlockRef,
        ) {
            let block_ref = block_ref.await;

            let writer = RecordWriter::try_new(block_manager.clone(), block_ref, 1)
                .await
                .unwrap();
            writer
                .tx()
                .send(Ok(Some(Bytes::from("xxxxxxxxx"))))
                .await
                .unwrap();
            writer.tx().send(Ok(None)).await.unwrap();

            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(
                writer.io_task_handle.as_ref().unwrap().is_finished(),
                "task is finished"
            );

            let block_ref = block_manager.read().await.load_block(1).await.unwrap();
            assert_eq!(
                block_ref.read().await.get_record(1).unwrap().state,
                record::State::Errored as i32
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_too_short(
            block_manager: Arc<RwLock<BlockManager>>,
            #[future] block_ref: BlockRef,
        ) {
            let block_ref = block_ref.await;

            let writer = RecordWriter::try_new(block_manager.clone(), block_ref, 1)
                .await
                .unwrap();
            writer.tx().send(Ok(Some(Bytes::from("xx")))).await.unwrap();
            writer.tx().send(Ok(None)).await.unwrap();

            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(
                writer.io_task_handle.as_ref().unwrap().is_finished(),
                "task is finished"
            );

            let block_ref = block_manager.read().await.load_block(1).await.unwrap();
            assert_eq!(
                block_ref.read().await.get_record(1).unwrap().state,
                record::State::Errored as i32
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_io_timeout(
            block_manager: Arc<RwLock<BlockManager>>,
            #[future] block_ref: BlockRef,
        ) {
            let block_ref = block_ref.await;

            let writer = RecordWriter::try_new(block_manager.clone(), block_ref, 1)
                .await
                .unwrap();
            writer.tx().send(Ok(Some(Bytes::from("te")))).await.unwrap();

            tokio::time::sleep(IO_OPERATION_TIMEOUT).await;
            tokio::time::sleep(Duration::from_millis(100)).await;

            assert!(
                writer.io_task_handle.as_ref().unwrap().is_finished(),
                "task is finished"
            );

            let block_ref = block_manager.read().await.load_block(1).await.unwrap();
            assert_eq!(
                block_ref.read().await.get_record(1).unwrap().state,
                record::State::Errored as i32
            );
        }

        #[fixture]
        fn path() -> PathBuf {
            tempdir().unwrap().into_path()
        }

        #[fixture]
        fn block_manager(path: PathBuf) -> Arc<RwLock<BlockManager>> {
            Arc::new(RwLock::new(BlockManager::new(
                path.clone(),
                BlockIndex::new(path.clone()),
            )))
        }

        #[fixture]
        async fn block_ref(block_manager: Arc<RwLock<BlockManager>>) -> BlockRef {
            let block_ref = block_manager
                .write()
                .await
                .start_new_block(1, 1000)
                .await
                .unwrap();
            block_ref.write().await.insert_or_update_record(Record {
                timestamp: Some(us_to_ts(&1)),
                begin: 0,
                end: 4,
                state: record::State::Started as i32,
                labels: vec![],
                content_type: "".to_string(),
            });
            block_ref
        }
    }

    mod record_drainer {
        use super::*;
        use std::time::Duration;
        use tokio::time::sleep;

        #[rstest]
        #[tokio::test]
        async fn test_drop() {
            let drainer = RecordDrainer::new();
            assert!(!drainer.io_task_handle.is_finished(), "wait for data");

            drainer.tx().send(Ok(None)).await.unwrap();
            sleep(Duration::from_millis(100)).await;
            assert!(drainer.io_task_handle.is_finished(), "task is finished");
        }
    }
}
