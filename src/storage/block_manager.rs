// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use prost::bytes::{Bytes, BytesMut};
use prost::Message;
use prost_wkt_types::Timestamp;
use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, RwLock, Weak};

use crate::core::status::HttpError;
use crate::storage::proto::*;
use crate::storage::reader::RecordReader;
use crate::storage::writer::RecordWriter;

pub const DEFAULT_MAX_READ_CHUNK: u64 = 1024 * 1024 * 512;

/// Helper class for basic operations on blocks.

pub struct BlockManager {
    path: PathBuf,
    readers: HashMap<u64, Vec<Weak<RwLock<RecordReader>>>>,
    writers: HashMap<u64, Vec<Weak<RwLock<RecordWriter>>>>,
}

pub const DESCRIPTOR_FILE_EXT: &str = ".meta";
pub const DATA_FILE_EXT: &str = ".blk";

impl BlockManager {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            readers: HashMap::new(),
            writers: HashMap::new(),
        }
    }

    /// Begin write a record
    ///
    /// # Arguments
    ///
    /// * `block` - Block to write to.
    /// * `record` - Record to write.
    ///
    /// # Returns
    ///
    /// * `Ok(RecordWriter)` - weak reference to the record writer.
    pub fn begin_write(
        &mut self,
        block: &Block,
        record_index: usize,
    ) -> Result<Arc<RwLock<RecordWriter>>, HttpError> {
        let ts = block.begin_time.clone().unwrap();
        let path = self.path_to_data(&ts);

        let content_length = block.records[record_index].end - block.records[record_index].begin;
        let writer = Arc::new(RwLock::new(RecordWriter::new(
            path,
            block,
            record_index,
            content_length,
            Box::new(Self::new(self.path.clone())),
        )?));

        let block_id = ts_to_us(&ts);
        match self.writers.entry(block_id) {
            Entry::Occupied(mut e) => {
                e.get_mut().push(Arc::downgrade(&writer));
            }
            Entry::Vacant(e) => {
                e.insert(vec![Arc::downgrade(&writer)]);
            }
        }

        self.clean_readers_or_writers(block_id);

        Ok(writer)
    }

    pub fn begin_read(
        &mut self,
        block: &Block,
        record_index: usize,
    ) -> Result<Arc<RwLock<RecordReader>>, HttpError> {
        let ts = block.begin_time.clone().unwrap();
        let path = self.path_to_data(&ts);
        let reader = Arc::new(RwLock::new(RecordReader::new(
            path,
            block,
            record_index,
            DEFAULT_MAX_READ_CHUNK,
        )?));

        let block_id = ts_to_us(&ts);
        match self.readers.entry(block_id) {
            Entry::Occupied(mut e) => {
                e.get_mut().push(Arc::downgrade(&reader));
            }
            Entry::Vacant(e) => {
                e.insert(vec![Arc::downgrade(&reader)]);
            }
        }

        self.clean_readers_or_writers(block_id);
        Ok(reader)
    }

    fn path_to_desc(&self, begin_time: &Timestamp) -> PathBuf {
        let block_id = ts_to_us(&begin_time);
        self.path
            .join(format!("{}{}", block_id, DESCRIPTOR_FILE_EXT))
    }

    fn path_to_data(&self, begin_time: &Timestamp) -> PathBuf {
        let block_id = ts_to_us(&begin_time);
        self.path.join(format!("{}{}", block_id, DATA_FILE_EXT))
    }

    /// Remove done or expired readers/writers of a block.
    ///
    /// # Arguments
    ///
    /// * `block_id` - ID of the block to clean.
    ///
    /// # Returns
    ///
    /// * `true` - If there are no more readers or writers.
    fn clean_readers_or_writers(&mut self, block_id: u64) -> bool {
        let readers_empty = match self.readers.get_mut(&block_id) {
            Some(readers) => {
                readers.retain(|r| {
                    let reader = r.upgrade();
                    reader.is_some() && !reader.unwrap().try_read().map_or(false, |r| r.is_done())
                });
                readers.is_empty()
            }
            None => true,
        };

        let writers_empty = match self.writers.get_mut(&block_id) {
            Some(writers) => {
                writers.retain(|w| {
                    let writer = w.upgrade();
                    writer.is_some() && !writer.unwrap().try_read().map_or(false, |w| w.is_done())
                });
                writers.is_empty()
            }
            None => true,
        };

        readers_empty && writers_empty
    }
}

pub trait ManageBlock {
    /// Load block descriptor from disk.
    ///
    /// # Arguments
    /// * `block_id` - ID of the block to load (begin time of the block).
    ///
    /// # Returns
    ///
    /// * `Ok(block)` - Block was loaded successfully.
    fn load(&self, block_id: u64) -> Result<Block, HttpError>;

    /// Save block descriptor to disk.
    ///
    /// # Arguments
    ///
    /// * `block` - Block to save.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Block was saved successfully.
    fn save(&self, block: &Block) -> Result<(), HttpError>;

    /// Start a new block
    ///
    /// # Arguments
    ///
    /// * `begin_time` - Begin time of the block.
    /// * `max_block_size` - Maximum size of the block.
    ///
    /// # Returns
    ///
    /// * `Ok(block)` - Block was created successfully.
    fn start(&self, block_id: u64, max_block_size: u64) -> Result<Block, HttpError>;

    /// Finish a block by truncating the file to the actual size.
    ///
    /// # Arguments
    ///
    /// * `block` - Block to finish.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Block was finished successfully.
    fn finish(&self, block: &Block) -> Result<(), HttpError>;

    /// Remove a block from disk if there are no readers or writers.
    fn remove(&mut self, block_id: u64) -> Result<(), HttpError>;
}

impl ManageBlock for BlockManager {
    fn load(&self, block_id: u64) -> Result<Block, HttpError> {
        let proto_ts = us_to_ts(&block_id);
        let buf = std::fs::read(self.path_to_desc(&proto_ts))?;
        Block::decode(Bytes::from(buf)).map_err(|e| {
            HttpError::internal_server_error(&format!("Failed to decode block descriptor: {}", e))
        })
    }

    fn save(&self, block: &Block) -> Result<(), HttpError> {
        let path = self.path_to_desc(block.begin_time.as_ref().unwrap());
        let mut buf = BytesMut::new();
        block.encode(&mut buf).map_err(|e| {
            HttpError::internal_server_error(&format!("Failed to encode block descriptor: {}", e))
        })?;
        let mut file = std::fs::File::create(path)?;
        file.write_all(&buf)?;

        Ok(())
    }

    fn start(&self, block_id: u64, max_block_size: u64) -> Result<Block, HttpError> {
        let mut block = Block::default();
        block.begin_time = Some(us_to_ts(&block_id));

        // create a block with data
        let file = std::fs::File::create(self.path_to_data(block.begin_time.as_ref().unwrap()))?;

        file.set_len(max_block_size)?;
        self.save(&block)?;

        Ok(block)
    }

    fn finish(&self, block: &Block) -> Result<(), HttpError> {
        let path = self.path_to_data(block.begin_time.as_ref().unwrap());
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
        file.set_len(block.size as u64)?;
        Ok(())
    }

    fn remove(&mut self, block_id: u64) -> Result<(), HttpError> {
        if !self.clean_readers_or_writers(block_id) {
            return Err(HttpError::internal_server_error(&format!(
                "Cannot remove block {} because it is still in use",
                block_id
            )));
        }

        let proto_ts = us_to_ts(&block_id);
        let path = self.path_to_data(&proto_ts);
        std::fs::remove_file(path)?;
        let path = self.path_to_desc(&proto_ts);
        std::fs::remove_file(path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_starting_block() {
        let bm = setup();
        let block = bm.start(1_000_005, 1024).unwrap();

        let ts = block.begin_time.clone().unwrap();
        assert_eq!(
            ts,
            Timestamp {
                seconds: 1,
                nanos: 5000,
            }
        );

        // Create an empty block
        let file = std::fs::File::open(bm.path.join(format!("{}{}", ts_to_us(&ts), DATA_FILE_EXT)))
            .unwrap();
        assert_eq!(file.metadata().unwrap().len(), 1024);

        // Create a block descriptor
        let buf = std::fs::read(
            bm.path
                .join(format!("{}{}", ts_to_us(&ts), DESCRIPTOR_FILE_EXT)),
        )
        .unwrap();
        let block_from_file = Block::decode(Bytes::from(buf)).unwrap();

        assert_eq!(block_from_file, block);
    }

    #[test]
    fn test_loading_block() {
        let bm = setup();

        bm.start(1, 1024).unwrap();
        let block = bm.start(20000005, 1024).unwrap();

        let ts = block.begin_time.clone().unwrap();
        let loaded_block = bm.load(ts_to_us(&ts)).unwrap();
        assert_eq!(loaded_block, block);
    }

    #[test]
    fn test_start_reading() {
        let bm = setup();

        let block = bm.start(1, 1024).unwrap();
        let ts = block.begin_time.clone().unwrap();
        let loaded_block = bm.load(ts_to_us(&ts)).unwrap();
        assert_eq!(loaded_block, block);
    }

    #[test]
    fn test_finish_block() {
        let bm = setup();

        let block = bm.start(1, 1024).unwrap();
        let ts = block.begin_time.clone().unwrap();
        let loaded_block = bm.load(ts_to_us(&ts)).unwrap();
        assert_eq!(loaded_block, block);

        bm.finish(&loaded_block).unwrap();

        let file = std::fs::File::open(bm.path.join(format!("{}{}", ts_to_us(&ts), DATA_FILE_EXT)))
            .unwrap();
        assert_eq!(file.metadata().unwrap().len(), 0);
    }

    #[test]
    fn test_start_writing() {
        let mut bm = setup();

        let block_id = 1;
        let mut block = bm.start(block_id, 1024).unwrap().clone();
        block.records.push(Record {
            timestamp: Some(Timestamp {
                seconds: 1,
                nanos: 5000,
            }),
            begin: 0,
            end: 5,
            state: 0,
            labels: vec![],
            content_type: "".to_string(),
        });

        bm.save(&block).unwrap();

        {
            let writer = bm.begin_write(&block, 0).unwrap();
            writer
                .write()
                .unwrap()
                .write("hello".as_bytes(), true)
                .unwrap();
        }

        bm.finish(&block).unwrap();
    }

    #[test]
    fn test_remove_with_writers() {
        let mut bm = setup();
        let block_id = 1;

        {
            let mut block = bm.start(block_id, 1024).unwrap().clone();
            block.records.push(Record {
                timestamp: Some(Timestamp {
                    seconds: 1,
                    nanos: 5000,
                }),
                begin: 0,
                end: 5,
                state: 0,
                labels: vec![],
                content_type: "".to_string(),
            });

            let writer = bm.begin_write(&block, 0).unwrap();
            assert!(!writer.read().unwrap().is_done());

            assert_eq!(
                bm.remove(block_id).err(),
                Some(HttpError::internal_server_error(&format!(
                    "Cannot remove block {} because it is still in use",
                    block_id
                )))
            );
        }
    }

    #[test]
    fn test_remove_block_with_readers() {
        let mut bm = setup();
        let block_id = 1;

        {
            let mut block = bm.start(block_id, 1024).unwrap().clone();
            block.records.push(Record {
                timestamp: Some(Timestamp {
                    seconds: 1,
                    nanos: 5000,
                }),
                begin: 0,
                end: 5,
                state: 0,
                labels: vec![],
                content_type: "".to_string(),
            });
            let reader = bm.begin_read(&block, 0).unwrap();
            assert!(!reader.read().unwrap().is_done());

            assert_eq!(
                bm.remove(block_id).err(),
                Some(HttpError::internal_server_error(&format!(
                    "Cannot remove block {} because it is still in use",
                    block_id
                )))
            );
        }
    }

    fn setup() -> BlockManager {
        let path = tempdir();
        let bm = BlockManager::new(path.unwrap().into_path());
        bm
    }
}
