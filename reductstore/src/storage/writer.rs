// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use bytes::Bytes;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crate::storage::block_manager::ManageBlock;
use crate::storage::proto::{record, ts_to_us, Block};
use reduct_base::error::{ErrorCode, ReductError};

/// Chunk is a chunk of data to write into a record.
pub enum Chunk {
    /// chunk of data to write into a record
    Data(Bytes),
    /// last chunk of data to write into a record
    Last(Bytes),
    /// error while writing a record. The writer marks the record as errored and it will be ignored
    Error,
}

pub trait WriteChunk {
    fn write(&mut self, chunk: Chunk) -> Result<(), ReductError>;
    fn content_length(&self) -> usize;
    fn written(&self) -> usize;
    fn is_done(&self) -> bool;
}

/// RecordWriter is used to write a record to a file.
pub struct RecordWriter {
    file: File,
    written_bytes: usize,
    content_length: usize,
    record_index: usize,
    block_id: u64,
    block_manager: Arc<RwLock<dyn ManageBlock + Send + Sync>>,
}

impl WriteChunk for RecordWriter {
    fn write(&mut self, chunk: Chunk) -> Result<(), ReductError> {
        let (data, last) = match chunk {
            Chunk::Data(data) => (data, false),
            Chunk::Last(data) => (data, true),
            Chunk::Error => {
                self.on_update(record::State::Errored);
                self.content_length = self.written_bytes; // we make it done
                return Ok(());
            }
        };

        self.write_impl(data, last).map_err(|e| {
            if e.status == ErrorCode::InternalServerError {
                self.on_update(record::State::Invalid);
            } else {
                self.on_update(record::State::Errored);
            }
            e
        })?;

        if last {
            self.on_update(record::State::Finished);
        }

        Ok(())
    }

    fn content_length(&self) -> usize {
        self.content_length
    }

    fn written(&self) -> usize {
        self.written_bytes
    }

    fn is_done(&self) -> bool {
        self.written_bytes == self.content_length
    }
}

impl RecordWriter {
    pub fn new<T>(
        path: PathBuf,
        block: &Block,
        record_index: usize,
        content_length: usize,
        block_manager: Arc<RwLock<T>>,
    ) -> Result<RecordWriter, ReductError>
    where
        T: ManageBlock + 'static + Send + Sync,
    {
        let mut file = OpenOptions::new().write(true).create(true).open(path)?;
        let offset = block.records[record_index].begin;
        file.seek(SeekFrom::Start(offset))?;

        Ok(Self {
            file,
            written_bytes: 0,
            content_length,
            record_index,
            block_id: ts_to_us(&block.begin_time.clone().unwrap()),
            block_manager,
        })
    }

    fn write_impl(&mut self, buf: Bytes, last: bool) -> Result<(), ReductError> {
        let mut writer = &self.file;

        self.written_bytes += buf.len();
        if self.written_bytes > self.content_length {
            return Err(ReductError::bad_request(
                "Content is bigger than in content-length",
            ));
        }

        writer.write_all(buf.as_ref())?;

        if last {
            if self.written_bytes < self.content_length {
                return Err(ReductError::bad_request(
                    "Content is smaller than in content-length",
                ));
            }

            writer.flush()?;
        }

        Ok(())
    }

    fn on_update(&mut self, state: record::State) {
        let mut block = match self.block_manager.read().unwrap().load(self.block_id) {
            Ok(block) => block,
            Err(e) => {
                log::error!("Failed to load block: {}", e);
                return;
            }
        };

        block.records[self.record_index].state = state as i32;
        block.invalid = state == record::State::Invalid;

        self.block_manager
            .write()
            .unwrap()
            .save(block)
            .map_err(|e| {
                log::error!("Failed to save block: {}", e);
            })
            .ok();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::proto::Record;
    use mockall::{mock, predicate::*};
    use prost_wkt_types::Timestamp;
    use tempfile::tempdir;

    mock! {
        BlockManager {}

        impl ManageBlock for BlockManager {
            fn load(&self, begin_time: u64) -> Result<Block, ReductError>;
            fn save(&mut self, block: Block) -> Result<(), ReductError>;
            fn start(&mut self, begin_time: u64, max_block_size: u64) -> Result<Block, ReductError>;
            fn finish(&mut self, block: &Block) -> Result<(), ReductError>;
            fn remove(&mut self, block_id: u64) -> Result<(), ReductError>;

        }
    }

    #[test]
    fn test_normal_flow() {
        let (path, mut block_manager, block) = setup();

        block_manager
            .expect_save()
            .withf(|block| block.records[0].state == record::State::Finished as i32)
            .times(1)
            .returning(|_| Ok(()));

        let same_block = block.clone();
        block_manager
            .expect_load()
            .times(1)
            .returning(move |_| Ok(same_block.clone()));

        let bm_ref = Arc::new(RwLock::new(block_manager));
        let mut writer = RecordWriter::new(path, &block, 0, 10, bm_ref).unwrap();
        writer.write(Chunk::Data(Bytes::from("67890"))).unwrap();
        writer.write(Chunk::Last(Bytes::from("12345"))).unwrap();
    }

    #[test]
    fn test_too_short_content() {
        let (path, mut block_manager, block) = setup();

        block_manager
            .expect_save()
            .withf(|block| block.records[0].state == record::State::Errored as i32)
            .times(1)
            .returning(|_| Ok(()));

        let same_block = block.clone();
        block_manager
            .expect_load()
            .times(1)
            .returning(move |_| Ok(same_block.clone()));

        let bm_ref = Arc::new(RwLock::new(block_manager));
        let mut writer = RecordWriter::new(path, &block, 0, 10, bm_ref).unwrap();
        writer.write(Chunk::Data(Bytes::from("67890"))).unwrap();

        assert_eq!(
            writer.write(Chunk::Last(Bytes::from("1234"))),
            Err(ReductError::bad_request(
                "Content is smaller than in content-length"
            ))
        );
    }

    #[test]
    fn test_too_long_content() {
        let (path, mut block_manager, block) = setup();

        block_manager
            .expect_save()
            .withf(|block| block.records[0].state == record::State::Errored as i32)
            .times(1)
            .returning(|_| Ok(()));

        let same_block = block.clone();
        block_manager
            .expect_load()
            .times(1)
            .returning(move |_| Ok(same_block.clone()));

        let bm_ref = Arc::new(RwLock::new(block_manager));
        let mut writer = RecordWriter::new(path, &block, 0, 10, bm_ref).unwrap();
        writer.write(Chunk::Data(Bytes::from("67890"))).unwrap();

        assert_eq!(
            writer.write(Chunk::Last(Bytes::from("123400000"))),
            Err(ReductError::bad_request(
                "Content is bigger than in content-length"
            ))
        );
    }

    #[test]
    fn test_errored_chunk() {
        let (path, mut block_manager, block) = setup();

        block_manager
            .expect_save()
            .withf(|block| block.records[0].state == record::State::Errored as i32)
            .times(1)
            .returning(|_| Ok(()));

        let same_block = block.clone();
        block_manager
            .expect_load()
            .times(1)
            .returning(move |_| Ok(same_block.clone()));

        let bm_ref = Arc::new(RwLock::new(block_manager));
        let mut writer = RecordWriter::new(path, &block, 0, 10, bm_ref).unwrap();
        writer.write(Chunk::Error).unwrap();
    }

    fn setup() -> (PathBuf, MockBlockManager, Block) {
        let path = tempdir().unwrap().into_path().join("test");
        let block_manager = MockBlockManager::new();
        let block = Block {
            begin_time: Some(Timestamp {
                seconds: 1,
                nanos: 0,
            }),
            records: vec![Record {
                timestamp: Option::from(Timestamp {
                    seconds: 1,
                    nanos: 0,
                }),
                begin: 0,
                end: 10,
                state: record::State::Started as i32,
                labels: vec![],
                content_type: "".to_string(),
            }],
            ..Block::default()
        };
        (path, block_manager, block)
    }
}
