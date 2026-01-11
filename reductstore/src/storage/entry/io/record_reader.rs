// Copyright 2024-2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::core::file_cache::FileWeak;
use crate::core::sync::AsyncRwLock;
use crate::storage::block_manager::{BlockManager, BlockRef};
use crate::storage::engine::MAX_IO_BUFFER_SIZE;
use crate::storage::proto::Record;
use async_trait::async_trait;
use bytes::Bytes;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use reduct_base::io::{ReadChunk, ReadRecord, RecordMeta};
use std::cmp::min;
use std::io;
use std::io::Read;
use std::io::{Seek, SeekFrom};
use std::sync::Arc;

/// RecordReader is responsible for reading the content of a record from the storage.
pub(crate) struct RecordReader {
    meta: RecordMeta,
    file_ref: Option<FileWeak>,
    offset: u64,
    content_size: u64,
    pos: u64,
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
    pub(in crate::storage) async fn try_new(
        block_manager: Arc<AsyncRwLock<BlockManager>>,
        block_ref: BlockRef,
        record_timestamp: u64,
        processed_record: Option<Record>,
    ) -> Result<Self, ReductError> {
        let bm = block_manager.write().await?;
        let block = block_ref.read()?;

        let (file_ref, offset) = bm.begin_read_record(&block, record_timestamp)?;

        let record = block.get_record(record_timestamp).unwrap();
        let content_size = record.end - record.begin;

        let meta: RecordMeta = {
            let meta = if let Some(processed_record) = processed_record {
                processed_record.into()
            } else {
                record.clone().into()
            };

            RecordMeta::builder_from(meta)
                .entry_name(bm.entry_name().clone())
                .build()
        };

        Ok(Self {
            meta,
            file_ref: Some(file_ref),
            offset,
            content_size,
            pos: 0,
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
            pos: 0,
        }
    }
}

impl Read for RecordReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let file_ref = match &self.file_ref {
            Some(file_ref) => file_ref,
            None => return Ok(0),
        };

        let rc = file_ref
            .upgrade()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.message))?;
        let mut lock = rc.write().unwrap();

        lock.seek(SeekFrom::Start(self.offset + self.pos))?;
        let read = lock.read(buf)?;

        self.pos += read as u64;
        Ok(read)
    }
}

impl Seek for RecordReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new_pos: i64 = match pos {
            SeekFrom::Start(offset) => offset as i64,
            SeekFrom::End(offset) => self.content_size as i64 + offset,
            SeekFrom::Current(offset) => self.pos as i64 + offset,
        };

        if new_pos > self.content_size as i64 || new_pos < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Seek position out of bounds",
            ));
        }

        self.pos = new_pos as u64;
        Ok(self.pos)
    }
}

#[async_trait]
impl ReadRecord for RecordReader {
    fn read_chunk(&mut self) -> ReadChunk {
        let mut buf =
            vec![0; min(self.content_size - self.pos, MAX_IO_BUFFER_SIZE as u64) as usize];
        if buf.is_empty() {
            return None;
        }

        match self.read(&mut buf) {
            Ok(0) => Some(Err(internal_server_error!(
                "Failed to read record chunk: EOF"
            ))),
            Ok(n) => {
                buf.truncate(n);
                Some(Ok(Bytes::from(buf)))
            }
            Err(e) => Some(Err(internal_server_error!(
                "Failed to read record chunk: {}",
                e
            ))),
        }
    }

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
pub(crate) mod tests {
    use super::*;
    use crate::core::file_cache::FILE_CACHE;
    use crate::storage::engine::MAX_IO_BUFFER_SIZE;
    use crate::storage::entry::tests::{entry, write_record, write_stub_record};
    use mockall::mock;
    use rstest::{fixture, rstest};

    mod read_in_chunks {
        use super::*;

        use crate::backend::Backend;
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
            let path = tempdir().unwrap().keep();
            FILE_CACHE.set_storage_backend(
                Backend::builder()
                    .local_data_path(path.clone())
                    .try_build()
                    .unwrap(),
            );

            let tmp_file = path.join("test_file");
            std::fs::write(&tmp_file, vec![0; content_size]).unwrap();

            tmp_file
        }
    }

    mod reader {
        use super::*;
        use crate::storage::entry::Entry;
        use prost_wkt_types::Timestamp;
        use std::fs;
        use std::sync::Arc;

        #[rstest]
        #[tokio::test]
        async fn test_read_chunk(entry: Arc<Entry>) {
            write_stub_record(&entry, 1000).await;
            let mut reader = entry.begin_read(1000).await.unwrap();
            assert_eq!(
                reader.read_chunk().unwrap().unwrap(),
                Bytes::from("0123456789")
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_read(entry: Arc<Entry>) {
            write_stub_record(&entry, 1000).await;
            let mut reader = entry.begin_read(1000).await.unwrap();
            let mut buf = vec![0; 4];
            let read = reader.read(&mut buf).unwrap();
            assert_eq!(read, 4);
            assert_eq!(buf, b"0123");
        }

        #[rstest]
        #[tokio::test]
        async fn test_empty_body(record: Record) {
            let mut reader = RecordReader::form_record(record);
            let mut buf = vec![0; 4];
            let read = reader.read(&mut buf).unwrap();
            assert_eq!(read, 0);
            assert_eq!(buf, [0; 4]);
            assert_eq!(reader.read_chunk(), None);
        }

        #[rstest]
        #[case(SeekFrom::Start(0), 0, 4, b"0123")]
        #[case(SeekFrom::Start(5), 5, 4, b"5678")]
        #[case(SeekFrom::End(-5), 5, 4, b"5678")]
        #[case(SeekFrom::Current(2), 2, 4, b"2345")]
        #[tokio::test]
        async fn test_seek(
            entry: Arc<Entry>,
            #[case] seek_from: SeekFrom,
            #[case] expected_pos: u64,
            #[case] read_size: usize,
            #[case] expected_data: &[u8],
        ) {
            write_stub_record(&entry, 1000).await;
            let mut reader = entry.begin_read(1000).await.unwrap();
            let new_pos = reader.seek(seek_from).unwrap();
            assert_eq!(new_pos, expected_pos);
            let mut buf = vec![0; read_size];
            let read = reader.read(&mut buf).unwrap();
            assert_eq!(read, read_size);
            assert_eq!(&buf, expected_data);
        }

        #[rstest]
        #[case(SeekFrom::Start(20))]
        #[case(SeekFrom::End(1))]
        #[case(SeekFrom::Current(-1))]
        #[tokio::test]
        async fn test_seek_wrong(entry: Arc<Entry>, #[case] seek_from: SeekFrom) {
            write_stub_record(&entry, 1000).await;
            let mut reader = entry.begin_read(1000).await.unwrap();
            let err = reader.seek(seek_from).err().unwrap();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
            assert_eq!(err.to_string(), "Seek position out of bounds");
        }

        #[rstest]
        #[tokio::test]
        async fn test_read_with_error(entry: Arc<Entry>) {
            write_record(&entry, 1000, vec![0; 100]).await;

            fs::write(entry.path().join("1000.blk"), "").unwrap();
            let mut reader = entry.begin_read(1000).await.unwrap();

            assert_eq!(
                reader.read_chunk().unwrap().err().unwrap(),
                internal_server_error!("Failed to read record chunk: EOF")
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_state(mut record: Record) {
            record.state = 1;
            let reader = RecordReader::form_record(record);
            assert_eq!(reader.meta().state(), 1);
        }

        #[rstest]
        #[tokio::test]
        async fn test_meta_mut(record: Record) {
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

    mock! {
        // Used in other tests
        pub(crate) Record {}

        impl Read for Record {
          fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error>;
        }

        impl Seek for Record {
          fn seek(&mut self, pos: std::io::SeekFrom) -> Result<u64, std::io::Error>;
            }

        #[async_trait]
        impl ReadRecord for Record {
            fn read_chunk(&mut self) -> ReadChunk;

          fn meta(&self) -> &RecordMeta;

          fn meta_mut(&mut self) -> &mut RecordMeta;
        }
    }
}
