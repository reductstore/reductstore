// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::core::status::HTTPError;
use crate::storage::block_manager::ManageBlock;
use crate::storage::proto::{ts_to_us, Block};
use std::cell::RefCell;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::rc::Rc;

pub struct RecordReader<'a> {
    file: File,
    written_bytes: u64,
    content_length: u64,
    chunk_size: u64,
    block_id: u64,
    block_manager: RefCell<&'a mut dyn ManageBlock>,
}

pub struct DataChunk {
    pub data: Vec<u8>,
    pub last: bool,
}

impl<'a> RecordReader<'a> {
    /// Create a new RecordReader.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the block of data.
    /// * `block` - The block descriptor that contains the record.
    /// * `record_index` - The index of the record in the block.
    /// * `content_length` - The length of the record.
    /// * `block_manager` - The block manager that manages the block.
    ///
    /// # Errors
    ///
    /// * `HTTPError` - If the file cannot be opened.
    pub fn new(
        path: PathBuf,
        block: &Block,
        record_index: usize,
        chunk_size: u64,
        block_manager: RefCell<&'a mut dyn ManageBlock>,
    ) -> Result<RecordReader<'a>, HTTPError> {
        let mut file = OpenOptions::new().read(true).open(path)?;
        let record = &block.records[record_index];
        let offset = record.begin;
        file.seek(SeekFrom::Start(offset))?;

        Ok(Self {
            file,
            written_bytes: 0,
            content_length: record.end - record.begin,
            chunk_size,
            block_id: ts_to_us(&block.begin_time.clone().unwrap()),
            block_manager,
        })
    }

    /// Read the next chunk of data from the record.
    ///
    /// # Returns
    ///
    /// * `DataChunk` - The next chunk of data.
    /// * `HTTPError` - If the data cannot be read.
    pub fn read(&mut self) -> Result<DataChunk, HTTPError> {
        let buffer_size = std::cmp::min(self.chunk_size, self.content_length - self.written_bytes);
        let mut buf = vec![0u8; buffer_size as usize];
        let read = self.file.read(&mut *buf)?;
        self.written_bytes += read as u64;
        Ok(DataChunk {
            data: buf[..read].to_vec(),
            last: self.written_bytes == self.content_length,
        })
    }
}

impl Drop for RecordReader<'_> {
    fn drop(&mut self) {
        self.block_manager.borrow_mut().unregister(self.block_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::proto::{record, Record};
    use mockall::mock;
    use mockall::predicate::eq;
    use prost_wkt_types::Timestamp;
    use std::io::Write;
    use tempfile::tempdir;

    mock! {
        BlockManager {}

        impl ManageBlock for BlockManager {
            fn load(&mut self, begin_time: u64) -> Result<Rc<Block>, HTTPError>;
            fn get(&self, begin_time: u64) -> Result<Rc<Block>, HTTPError>;
            fn save(&mut self, block: &Block) -> Result<(), HTTPError>;
            fn start(&mut self, begin_time: u64, max_block_size: u64) -> Result<Rc<Block>, HTTPError>;
            fn finish(&self, block: &Block) -> Result<(), HTTPError>;
            fn unregister(&mut self, block_id: u64);
        }
    }

    #[test]
    fn test_read() {
        let path = tempdir().unwrap().into_path().join("test");
        let mut block_manager = MockBlockManager::new();
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

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
        file.write_all("1234567890".as_bytes()).unwrap();

        block_manager
            .expect_unregister()
            .times(1)
            .returning(|_| ())
            .with(eq(ts_to_us(block.begin_time.as_ref().unwrap())));

        {
            let mut reader =
                RecordReader::new(path.clone(), &block, 0, 5, RefCell::new(&mut block_manager))
                    .unwrap();
            let chunk = reader.read().unwrap();
            assert_eq!(chunk.data, "12345".as_bytes());
            assert_eq!(chunk.last, false);

            let chunk = reader.read().unwrap();
            assert_eq!(chunk.data, "67890".as_bytes());
            assert_eq!(chunk.last, true);
        }
    }
}
