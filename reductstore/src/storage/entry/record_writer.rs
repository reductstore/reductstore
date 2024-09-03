// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::block_manager::{BlockManager, BlockRef, RecordRx, RecordTx};
use crate::storage::file_cache::FileRef;
use crate::storage::proto::record;
use crate::storage::storage::{CHANNEL_BUFFER_SIZE, IO_OPERATION_TIMEOUT, MAX_IO_BUFFER_SIZE};
use bytes::Bytes;
use futures_util::stream::Concat;
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
    file_ref: FileRef,
    offset: u64,
    content_size: u64,
    block_manager: Arc<RwLock<BlockManager>>,
}
impl RecordWriter {
    pub async fn try_new(
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
                            let mut lock = ctx.file_ref.write().await;
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
                ctx.file_ref.write().await.flush().await?;
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
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
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
