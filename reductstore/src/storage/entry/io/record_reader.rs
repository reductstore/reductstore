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
use std::io;
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
    meta: RecordMeta,
    file_ref: Option<FileWeak>,
    offset: u64,
    content_size: u64,
    read_bytes: u64,
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
    /// * `processed_record` - The metadata of the record, if any then we use it instead of reading from the block
    ///
    /// # Returns
    ///
    /// * `Result<RecordReader, ReductError>` - The record reader to read the record content in chunks
    pub(in crate::storage) fn try_new(
        block_manager: Arc<RwLock<BlockManager>>,
        block_ref: BlockRef,
        record_timestamp: u64,
        processed_record: Option<Record>,
    ) -> Result<Self, ReductError> {
        let bm = block_manager.write()?;
        let block = block_ref.read()?;

        let (file_ref, offset) = bm.begin_read_record(&block, record_timestamp)?;

        let record = block.get_record(record_timestamp).unwrap();
        let content_size = record.end - record.begin;

        let meta: RecordMeta = if let Some(processed_record) = processed_record {
            processed_record.into()
        } else {
            record.clone().into()
        };

        Ok(Self {
            meta,
            file_ref: Some(file_ref),
            offset,
            content_size,
            read_bytes: 0,
        })
    }

    /// Create a new record reader for a record with no content.
    ///
    /// We need it to read only metadata.
    ///
    /// # Arguments
    ///
    /// * `record` - The record to read
    ///
    /// # Returns
    ///
    /// * `RecordReader` - The record reader to read the record content in chunks
    pub fn form_record(record: Record) -> Self {
        let meta: RecordMeta = record.into();
        RecordReader {
            meta,
            file_ref: None,
            offset: 0,
            content_size: 0,
            read_bytes: 0,
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

impl Read for RecordReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let file_ref = match &self.file_ref {
            Some(file_ref) => file_ref,
            None => return Ok(0),
        };

        let rc = file_ref.upgrade().map_err(|e| {
            error!("Failed to upgrade file reference: {}", e);
            io::Error::new(io::ErrorKind::Other, "Failed to upgrade file reference")
        })?;

        let mut lock = rc.write().unwrap();

        lock.seek(SeekFrom::Start(self.offset + self.read_bytes))?;
        let read = lock.read(buf)?;

        self.read_bytes += read as u64;
        Ok(read)
    }
}

impl Seek for RecordReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(offset) => offset,
            SeekFrom::End(offset) => {
                if offset >= 0 {
                    self.content_size + offset as u64
                } else {
                    self.content_size - (-offset) as u64
                }
            }
            SeekFrom::Current(offset) => {
                if offset >= 0 {
                    self.read_bytes + offset as u64
                } else {
                    self.read_bytes - (-offset) as u64
                }
            }
        };

        if new_pos > self.content_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Seek position out of bounds",
            ));
        }

        self.read_bytes = new_pos;
        Ok(self.read_bytes)
    }
}

#[async_trait]
impl ReadRecord for RecordReader {
    fn meta(&self) -> &RecordMeta {
        &self.meta
    }

    fn meta_mut(&mut self) -> &mut RecordMeta {
        &mut self.meta
    }
}

/// Read a chunk of the record content
///
/// # Arguments
///
/// * `file` - The file reference to read from
/// * `offset` - The offset to start reading from
/// * `content_size` - The size of the content to read
/// * `read_bytes` - The number of bytes already read
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

    let read = seek_and_read?;
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
            let reader = RecordReader::form_record(record);
            assert_eq!(reader.meta().state(), 1);
        }

        #[rstest]
        fn test_meta_mut(record: Record) {
            let mut reader = RecordReader::form_record(record);
            let meta_mut = reader.meta_mut();
            meta_mut
                .labels_mut()
                .insert("test_key".to_string(), "test_value".to_string());
            assert_eq!(
                reader.meta().labels().get("test_key"),
                Some(&"test_value".to_string())
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_read_timeout(record: Record) {
            let (_tx, rx) = channel(CHANNEL_BUFFER_SIZE);
            let mut reader = RecordReader::form_record_with_rx(rx, record);

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
