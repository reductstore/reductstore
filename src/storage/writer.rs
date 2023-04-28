// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;

use crate::core::status::{HTTPError, HTTPStatus};
use crate::storage::block_manager::ManageBlock;
use crate::storage::proto::{record, ts_to_us, Block};

/// RecordWriter is used to write a record to a file.
pub struct RecordWriter {
    file: File,
    written_bytes: u64,
    content_length: u64,
    record_index: usize,
    block_id: u64,
    block_manager: Box<dyn ManageBlock>,
}

impl RecordWriter {
    pub fn new(
        path: PathBuf,
        block: &Block,
        record_index: usize,
        content_length: u64,
        block_manager: Box<dyn ManageBlock>,
    ) -> Result<RecordWriter, HTTPError> {
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

    pub fn write(&mut self, buf: &[u8], last: bool) -> Result<(), HTTPError> {
        self.write_impl(buf, last).map_err(|e| {
            if e.status == HTTPStatus::InternalServerError {
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

    fn write_impl(&mut self, buf: &[u8], last: bool) -> Result<(), HTTPError> {
        let mut writer = &self.file;

        self.written_bytes += buf.len() as u64;
        if self.written_bytes > self.content_length {
            return Err(HTTPError::bad_request(
                "Content is bigger than in content-length",
            ));
        }

        writer.write_all(buf)?;

        if last {
            if self.written_bytes < self.content_length {
                return Err(HTTPError::bad_request(
                    "Content is smaller than in content-length",
                ));
            }

            writer.flush()?;
        }

        Ok(())
    }

    pub fn is_done(&self) -> bool {
        self.written_bytes == self.content_length
    }

    fn on_update(&mut self, state: record::State) {
        let mut block = match self.block_manager.load(self.block_id) {
            Ok(block) => block.clone(), // TODO: a block may have many labels and could be expensive
            Err(e) => {
                log::error!("Failed to load block: {}", e);
                return;
            }
        };

        block.records[self.record_index].state = state as i32;
        block.invalid = state == record::State::Invalid;

        self.block_manager
            .save(&block)
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
    use tempfile::tempdir;

    mock! {
        BlockManager {}

        impl ManageBlock for BlockManager {
            fn load(&self, begin_time: u64) -> Result<Block, HTTPError>;
            fn save(&self, block: &Block) -> Result<(), HTTPError>;
            fn start(&self, begin_time: u64, max_block_size: u64) -> Result<Block, HTTPError>;
            fn finish(&self, block: &Block) -> Result<(), HTTPError>;
            fn remove(&mut self, block_id: u64) -> Result<(), HTTPError>;

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

        let mut writer = RecordWriter::new(path, &block, 0, 10, Box::new(block_manager)).unwrap();
        writer.write(b"67890", false).unwrap();
        writer.write(b"12345", true).unwrap();
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

        let mut writer = RecordWriter::new(path, &block, 0, 10, Box::new(block_manager)).unwrap();
        writer.write(b"67890", false).unwrap();

        assert_eq!(
            writer.write(b"1234", true),
            Err(HTTPError::bad_request(
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

        let mut writer = RecordWriter::new(path, &block, 0, 10, Box::new(block_manager)).unwrap();
        writer.write(b"67890", false).unwrap();

        assert_eq!(
            writer.write(b"123400000", true),
            Err(HTTPError::bad_request(
                "Content is bigger than in content-length"
            ))
        );
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
