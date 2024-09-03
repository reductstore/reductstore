// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::block_manager::{BlockManager, BlockRef, RecordRx};
use crate::storage::file_cache::FileRef;
use crate::storage::proto::record::Label;
use crate::storage::proto::{ts_to_us, Record};
use crate::storage::storage::{CHANNEL_BUFFER_SIZE, IO_OPERATION_TIMEOUT, MAX_IO_BUFFER_SIZE};
use bytes::Bytes;
use log::debug;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::cmp::min;
use std::io::SeekFrom;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::mpsc::error::SendTimeoutError;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

/// RecordReader is responsible for reading the content of a record from the storage.
pub(crate) struct RecordReader {
    rx: Option<RecordRx>,
    io_task_handle: Option<JoinHandle<()>>,
    record: Record,
    last: bool,
}

struct ReadContext {
    bucket_name: String,
    entry_name: String,
    block_id: u64,
    record_timestamp: u64,
    file_ref: FileRef,
    offset: u64,
    content_size: u64,
    block_manager: Arc<RwLock<BlockManager>>,
}

impl Drop for RecordReader {
    fn drop(&mut self) {
        if let Some(io) = self.io_task_handle.take() {
            io.abort(); // Abort the task if it is still running
        }
    }
}

impl RecordReader {
    /// Create a new record reader.
    ///
    /// The reader spawns a task to read the record content in chunks. If the size of the record is less than or equal to the maximum IO buffer size, the reader reads the record content in one go.
    ///
    /// # Arguments
    ///
    /// * `block_manager` - The block manager to read the record from
    /// * `block_ref` - The block reference to read the record from
    /// * `record_timestamp` - The timestamp of the record
    ///
    /// # Returns
    ///
    /// * `Result<RecordReader, ReductError>` - The record reader to read the record content in chunks
    pub(in crate::storage) async fn try_new(
        block_manager: Arc<RwLock<BlockManager>>,
        block_ref: BlockRef,
        record_timestamp: u64,
        last: bool,
    ) -> Result<Self, ReductError> {
        let (record, ctx) = async move {
            let mut bm = block_manager.write().await;
            let block = block_ref.read().await;

            let (file_ref, offset) = bm.begin_read_record(&block, record_timestamp).await?;

            let record = block.get_record(record_timestamp).unwrap();
            let content_size = record.end - record.begin;
            let block_id = block.block_id();
            let bucket_name = bm.bucket_name().to_string();
            let entry_name = bm.entry_name().to_string();

            drop(bm);
            Ok::<(Record, ReadContext), ReductError>((
                record.clone(),
                ReadContext {
                    bucket_name,
                    entry_name,
                    block_id,
                    record_timestamp,
                    file_ref,
                    offset,
                    content_size,
                    block_manager,
                },
            ))
        }
        .await?;

        let (tx, rx) = channel(CHANNEL_BUFFER_SIZE);

        let io_task_handle = if ctx.content_size <= MAX_IO_BUFFER_SIZE as u64 {
            Self::read(tx, ctx).await;
            None
        } else {
            Some(tokio::spawn(async move {
                Self::read(tx, ctx).await;
            }))
        };

        Ok(RecordReader {
            rx: Some(rx),
            record,
            last,
            io_task_handle,
        })
    }

    /// Create a new record reader for a record with no content.
    ///
    /// We need it to read only metadata.
    ///
    /// # Arguments
    ///
    /// * `record` - The record to read
    /// * `last` - Whether this is the last record in the entry
    ///
    /// # Returns
    ///
    /// * `RecordReader` - The record reader to read the record content in chunks
    pub fn form_record(record: Record, last: bool) -> Self {
        RecordReader {
            rx: None,
            record,
            last,
            io_task_handle: None,
        }
    }

    #[cfg(test)]
    pub fn form_record_with_rx(rx: RecordRx, record: Record, last: bool) -> Self {
        RecordReader {
            rx: Some(rx),
            record,
            last,
            io_task_handle: None,
        }
    }

    pub fn timestamp(&self) -> u64 {
        ts_to_us(self.record.timestamp.as_ref().unwrap())
    }

    pub fn content_type(&self) -> &str {
        self.record.content_type.as_str()
    }

    pub fn labels(&self) -> &Vec<Label> {
        &self.record.labels
    }

    pub fn content_length(&self) -> u64 {
        self.record.end - self.record.begin
    }

    pub fn only_metadata(&self) -> bool {
        self.rx.is_none()
    }

    /// Get the receiver to read the record content
    ///
    /// # Returns
    ///
    /// * `&mut Receiver<Result<Bytes, ReductError>>` - The receiver to read the record content
    ///
    /// # Panics
    ///
    /// Panics if the receiver isn't set (we read only metadata)
    pub fn rx(&mut self) -> &mut Receiver<Result<Bytes, ReductError>> {
        self.rx.as_mut().unwrap()
    }

    pub fn last(&self) -> bool {
        self.last
    }

    pub fn set_last(&mut self, last: bool) {
        self.last = last;
    }

    pub fn record(&self) -> &Record {
        &self.record
    }

    async fn read(tx: Sender<Result<Bytes, ReductError>>, ctx: ReadContext) {
        let mut read_bytes = 0;

        let read_all = async {
            while read_bytes < ctx.content_size {
                let (buf, read) =
                    match read_in_chunks(&ctx.file_ref, ctx.offset, ctx.content_size, read_bytes)
                        .await
                    {
                        Ok((buf, read)) => (buf, read),
                        Err(e) => {
                            tx.send_timeout(Err(e), IO_OPERATION_TIMEOUT).await?;
                            break;
                        }
                    };

                tx.send_timeout(Ok(buf.into()), IO_OPERATION_TIMEOUT)
                    .await?;

                read_bytes += read as u64;
                ctx.block_manager
                    .write()
                    .await
                    .use_counter_mut()
                    .update(ctx.block_id);
            }

            Ok::<(), SendTimeoutError<_>>(())
        };

        if let Err(e) = read_all.await {
            debug!(
                "Failed to send record {}/{}/{}: {}",
                ctx.bucket_name, ctx.entry_name, ctx.record_timestamp, e
            )
        }

        ctx.block_manager
            .write()
            .await
            .finish_read_record(ctx.block_id);
    }
}

/// Read a chunk of the record content
///
/// # Arguments
///
/// * `file` - The file reference to read from
/// * `offset` - The offset to start reading from
/// * `content_size` - The size of the content to read
///
/// # Returns
///
/// * `Result<(Vec<u8>, usize), ReductError>` - The read buffer and the number of bytes read
pub async fn read_in_chunks(
    file: &FileRef,
    offset: u64,
    content_size: u64,
    read_bytes: u64,
) -> Result<(Vec<u8>, usize), ReductError> {
    let chunk_size = min(content_size - read_bytes, MAX_IO_BUFFER_SIZE as u64);
    let mut buf = vec![0; chunk_size as usize];

    let seek_and_read = async {
        let mut lock = file.write().await;
        lock.seek(SeekFrom::Start(offset + read_bytes)).await?;
        lock.read(&mut buf).await
    };

    let read = match seek_and_read.await {
        Ok(read) => read,
        Err(e) => {
            return Err(internal_server_error!("Failed to read record chunk: {}", e));
        }
    };

    if read == 0 {
        return Err(internal_server_error!("Failed to read record chunk: EOF"));
    }
    Ok((buf, read))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::entry::tests::{entry, write_record, write_stub_record};
    use crate::storage::file_cache::FILE_CACHE;
    use crate::storage::storage::MAX_IO_BUFFER_SIZE;
    use rstest::{fixture, rstest};

    mod read_in_chunks {
        use super::*;

        use std::io::SeekFrom;
        use std::path::PathBuf;
        use tempfile::tempdir;

        #[rstest]
        #[tokio::test]
        async fn test_ok(file_to_read: PathBuf, content_size: usize) {
            let file_ref = FILE_CACHE
                .read(&file_to_read, SeekFrom::Start(0))
                .await
                .unwrap();
            let content_size = content_size as u64;
            let (data, len) = read_in_chunks(&file_ref, 0, content_size, 0).await.unwrap();
            assert_eq!(len, MAX_IO_BUFFER_SIZE);

            let (data, len) = read_in_chunks(&file_ref, 0, content_size, len as u64)
                .await
                .unwrap();
            assert_eq!(len, content_size as usize - MAX_IO_BUFFER_SIZE);
        }

        #[rstest]
        #[tokio::test]
        async fn test_eof(file_to_read: PathBuf, content_size: usize) {
            let file_ref = FILE_CACHE
                .read(&file_to_read, SeekFrom::Start(0))
                .await
                .unwrap();
            let content_size = content_size as u64;
            let err = read_in_chunks(&file_ref, content_size, content_size, 0)
                .await
                .err()
                .unwrap();
            assert_eq!(
                err,
                internal_server_error!("Failed to read record chunk: EOF")
            );
        }

        #[fixture]
        fn content_size() -> usize {
            MAX_IO_BUFFER_SIZE + 1
        }
        #[fixture]
        fn file_to_read(content_size: usize) -> PathBuf {
            let tmp_file = tempdir().unwrap().into_path().join("test_file");
            std::fs::write(&tmp_file, vec![0; content_size]).unwrap();

            tmp_file
        }
    }

    mod reader {
        use super::*;
        use crate::storage::entry::Entry;
        use std::thread::sleep;
        use std::time::Duration;

        #[rstest]
        #[tokio::test]
        async fn test_no_tokio_task(mut entry: Entry) {
            write_stub_record(&mut entry, 1000).await.unwrap();

            let mut reader = entry.begin_read(1000).await.unwrap();
            assert!(
                reader.io_task_handle.is_none(),
                "We don't spawn a task for small records"
            );
            assert_eq!(
                reader.rx().recv().await.unwrap().unwrap(),
                Bytes::from("0123456789")
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_with_tokio_task(mut entry: Entry) {
            write_record(&mut entry, 1000, vec![0; MAX_IO_BUFFER_SIZE + 1])
                .await
                .unwrap();

            let mut reader = entry.begin_read(1000).await.unwrap();

            assert!(
                reader.io_task_handle.is_some(),
                "We spawn a task for big records"
            );
            assert_eq!(
                reader.rx().recv().await.unwrap().unwrap().len(),
                MAX_IO_BUFFER_SIZE
            );

            tokio::time::sleep(Duration::from_millis(100)).await; // Wait for the task to finish

            assert!(
                reader.io_task_handle.as_ref().unwrap().is_finished(),
                "The task should finish after reading the record"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_with_io_timeout(mut entry: Entry) {
            write_record(&mut entry, 1000, vec![0; MAX_IO_BUFFER_SIZE + 1])
                .await
                .unwrap();

            let mut reader = entry.begin_read(1000).await.unwrap();
            tokio::time::sleep(IO_OPERATION_TIMEOUT).await;
            tokio::time::sleep(Duration::from_millis(100)).await; // Wait for the task to finish

            assert!(
                reader.io_task_handle.as_ref().unwrap().is_finished(),
                "The task should finish after reading the record"
            );
        }
    }
}
