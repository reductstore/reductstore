// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::core::file_cache::FileWeak;
use crate::core::thread_pool::shared_child_isolated;
use crate::storage::block_manager::{BlockManager, BlockRef, RecordRx};
use crate::storage::proto::Record;
use crate::storage::storage::{CHANNEL_BUFFER_SIZE, IO_OPERATION_TIMEOUT, MAX_IO_BUFFER_SIZE};
use async_trait::async_trait;
use bytes::Bytes;
use log::{debug, error};
use reduct_base::error::ReductError;
use reduct_base::io::{ReadChunk, ReadRecord, RecordMeta};
use reduct_base::{internal_server_error, timeout};
use std::cmp::min;
use std::io::Read;
use std::io::{Seek, SeekFrom};
use std::sync::{Arc, RwLock};
use std::thread::sleep;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::mpsc::error::{SendError, TrySendError};
use tokio::sync::mpsc::{channel, Sender};

/// RecordReader is responsible for reading the content of a record from the storage.
pub(crate) struct RecordReader {
    rx: Option<RecordRx>,
    meta: RecordMeta,
}

struct ReadContext {
    bucket_name: String,
    entry_name: String,
    record_timestamp: u64,
    file_ref: FileWeak,
    offset: u64,
    content_size: u64,
    task_group: String,
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
    pub(in crate::storage) fn try_new(
        block_manager: Arc<RwLock<BlockManager>>,
        block_ref: BlockRef,
        record_timestamp: u64,
        last: bool,
    ) -> Result<Self, ReductError> {
        let (record, ctx) = {
            let bm = block_manager.write()?;
            let block = block_ref.read()?;

            let (file_ref, offset) = bm.begin_read_record(&block, record_timestamp)?;

            let record = block.get_record(record_timestamp).unwrap();
            let content_size = record.end - record.begin;
            let block_id = block.block_id();
            let bucket_name = bm.bucket_name().to_string();
            let entry_name = bm.entry_name().to_string();

            let storage = bm
                .path()
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .file_name()
                .unwrap()
                .to_str()
                .unwrap();
            let task_group = [storage, &bucket_name, &entry_name, &block_id.to_string()].join("/");

            drop(bm);
            Ok::<(Record, ReadContext), ReductError>((
                record.clone(),
                ReadContext {
                    bucket_name,
                    entry_name,
                    record_timestamp,
                    file_ref,
                    offset,
                    content_size,
                    task_group,
                },
            ))
        }?;

        let (tx, rx) = channel(CHANNEL_BUFFER_SIZE);

        if ctx.content_size <= MAX_IO_BUFFER_SIZE as u64 {
            Self::read(tx, ctx);
        } else {
            shared_child_isolated(&ctx.task_group.clone(), "read record content", move || {
                Self::read(tx, ctx);
            });
        };

        Ok(Self::form_record_with_rx(rx, record, last))
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
        let mut meta: RecordMeta = record.into();
        meta.set_last(last);
        RecordReader { rx: None, meta }
    }

    pub fn form_record_with_rx(rx: RecordRx, record: Record, last: bool) -> Self {
        let mut me = Self::form_record(record, last);
        me.rx = Some(rx);
        me
    }

    pub fn set_last(&mut self, last: bool) {
        self.meta.set_last(last);
    }

    fn read(tx: Sender<Result<Bytes, ReductError>>, ctx: ReadContext) {
        let mut read_bytes = 0;
        let path = format!(
            "{}/{}/{}",
            ctx.bucket_name, ctx.entry_name, ctx.record_timestamp
        );
        let mut read_all = || {
            while read_bytes < ctx.content_size {
                let (buf, read) =
                    match read_in_chunks(&ctx.file_ref, ctx.offset, ctx.content_size, read_bytes) {
                        Ok((buf, read)) => (buf, read),
                        Err(e) => {
                            error!("Failed to read record {}: {}", path, e);
                            Self::blocking_send_with_timeout(&tx, Err(e), IO_OPERATION_TIMEOUT)??;
                            break;
                        }
                    };

                Self::blocking_send_with_timeout(&tx, Ok(Bytes::from(buf)), IO_OPERATION_TIMEOUT)??;
                read_bytes += read as u64;
            }

            Ok::<Result<(), SendError<_>>, ReductError>(Ok::<
                (),
                SendError<Result<Bytes, ReductError>>,
            >(()))
        };

        if let Err(e) = read_all() {
            // it's debug level because extensions may stop reading records intentionally
            debug!(
                "Failed to send record {}/{}/{}: {}",
                ctx.bucket_name, ctx.entry_name, ctx.record_timestamp, e.message
            )
        }
    }

    fn blocking_send_with_timeout(
        tx: &Sender<Result<Bytes, ReductError>>,
        mut msg: Result<Bytes, ReductError>,
        timeout: Duration,
    ) -> Result<Result<(), SendError<Result<Bytes, ReductError>>>, ReductError> {
        let now = Instant::now();

        while now.elapsed() < timeout {
            match tx.try_send(msg) {
                Ok(_) => {
                    return Ok(Ok(()));
                }
                Err(TrySendError::Full(ret)) => {
                    sleep(std::time::Duration::from_millis(1));
                    msg = ret;
                }
                Err(TrySendError::Closed(ret)) => {
                    return Ok(Err(SendError { 0: ret }));
                }
            }
        }

        Err(timeout!("Channel send timeout: {} s", timeout.as_secs()))
    }
}

#[async_trait]
impl ReadRecord for RecordReader {
    async fn read(&mut self) -> ReadChunk {
        if let Some(rx) = &mut self.rx {
            rx.recv().await
        } else {
            None
        }
    }

    async fn read_timeout(&mut self, timeout: std::time::Duration) -> ReadChunk {
        match tokio::time::timeout(timeout, self.read()).await {
            Ok(chunk) => chunk,
            Err(er) => Some(Err(internal_server_error!(
                "Timeout reading record: {}",
                er
            ))),
        }
    }

    fn blocking_read(&mut self) -> ReadChunk {
        if let Some(rx) = &mut self.rx {
            rx.blocking_recv()
        } else {
            None
        }
    }

    fn meta(&self) -> &RecordMeta {
        &self.meta
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
pub(in crate::storage) fn read_in_chunks(
    file: &FileWeak,
    offset: u64,
    content_size: u64,
    read_bytes: u64,
) -> Result<(Vec<u8>, usize), ReductError> {
    let chunk_size = min(content_size - read_bytes, MAX_IO_BUFFER_SIZE as u64);
    let mut buf = vec![0; chunk_size as usize];

    let seek_and_read = {
        let rc = file.upgrade()?;
        let mut lock = rc.write()?;
        lock.seek(SeekFrom::Start(offset + read_bytes))?;
        let read = lock.read(&mut buf)?;
        Ok::<usize, ReductError>(read)
    };

    let read = match seek_and_read {
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
    use crate::core::file_cache::FILE_CACHE;
    use crate::storage::entry::tests::{entry, write_record, write_stub_record};
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
            let file_ref = FILE_CACHE.read(&file_to_read, SeekFrom::Start(0)).unwrap();
            let content_size = content_size as u64;
            let (_data, len) = read_in_chunks(&file_ref, 0, content_size, 0).unwrap();
            assert_eq!(len, MAX_IO_BUFFER_SIZE);

            let (_data, len) = read_in_chunks(&file_ref, 0, content_size, len as u64).unwrap();
            assert_eq!(len, content_size as usize - MAX_IO_BUFFER_SIZE);
        }

        #[rstest]
        #[tokio::test]
        async fn test_eof(file_to_read: PathBuf, content_size: usize) {
            let file_ref = FILE_CACHE.read(&file_to_read, SeekFrom::Start(0)).unwrap();
            let content_size = content_size as u64;
            let err = read_in_chunks(&file_ref, content_size, content_size, 0)
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
            let tmp_file = tempdir().unwrap().keep().join("test_file");
            std::fs::write(&tmp_file, vec![0; content_size]).unwrap();

            tmp_file
        }
    }

    mod reader {
        use super::*;
        use crate::storage::entry::Entry;
        use std::fs;
        use std::thread::sleep;

        use crate::core::thread_pool::find_task_group;
        use crate::storage::entry::tests::get_task_group;

        use prost_wkt_types::Timestamp;
        use std::time::Duration;

        #[rstest]
        fn test_no_task(mut entry: Entry) {
            write_stub_record(&mut entry, 1000);
            let mut reader = entry.begin_read(1000).wait().unwrap();
            assert!(
                find_task_group(&get_task_group(entry.path(), 1000)).is_none(),
                "We don't spawn a task for small records"
            );
            assert_eq!(
                reader.blocking_read().unwrap().unwrap(),
                Bytes::from("0123456789")
            );
        }

        #[rstest]
        fn test_with_task(mut entry: Entry) {
            write_record(
                &mut entry,
                1000,
                vec![0; MAX_IO_BUFFER_SIZE * CHANNEL_BUFFER_SIZE + 1],
            );

            let mut reader = entry.begin_read(1000).wait().unwrap();
            let task_group = get_task_group(entry.path(), 1000);
            sleep(Duration::from_millis(100)); // Wait for the task to start

            assert!(
                find_task_group(&task_group).is_some(),
                "We spawn a task for big records"
            );
            assert_eq!(
                reader.blocking_read().unwrap().unwrap().len(),
                MAX_IO_BUFFER_SIZE
            );

            sleep(Duration::from_millis(100)); // Wait for the task to finish

            assert!(
                find_task_group(&task_group).is_none(),
                "The task should finish after reading the record"
            );
        }

        #[rstest]
        fn test_read_with_error(mut entry: Entry) {
            write_record(&mut entry, 1000, vec![0; 100]);

            fs::write(entry.path().join("1000.blk"), "").unwrap();
            let mut reader = entry.begin_read(1000).wait().unwrap();

            assert_eq!(
                reader.blocking_read().unwrap().err().unwrap(),
                internal_server_error!("Failed to read record chunk: EOF")
            );
        }

        #[rstest]
        fn test_state(mut record: Record) {
            record.state = 1;
            let reader = RecordReader::form_record(record, false);
            assert_eq!(reader.meta().state(), 1);
        }

        #[rstest]
        #[tokio::test]
        async fn test_read_timeout(record: Record) {
            let (_tx, rx) = channel(CHANNEL_BUFFER_SIZE);
            let mut reader = RecordReader::form_record_with_rx(rx, record, false);

            let result = reader.read_timeout(Duration::from_millis(100)).await;
            assert_eq!(
                result,
                Some(Err(internal_server_error!(
                    "Timeout reading record: deadline has elapsed"
                )))
            );
        }

        #[rstest]
        fn test_channel_timeout() {
            let msg = Ok(Bytes::from("test"));
            let (tx, _rx) = channel(1);

            tx.blocking_send(msg.clone()).unwrap(); // full
            let result =
                RecordReader::blocking_send_with_timeout(&tx, msg, Duration::from_millis(1));

            assert_eq!(result, Err(timeout!("Channel send timeout: 0 s")));
        }

        #[fixture]
        fn record() -> Record {
            let mut record = Record::default();
            record.timestamp = Some(Timestamp {
                seconds: 1000,
                nanos: 0,
            });
            record
        }
    }
}
