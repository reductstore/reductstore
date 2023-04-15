// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use prost::bytes::{Bytes, BytesMut};
use prost::Message;
use prost_wkt_types::Timestamp;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::rc::Rc;

use crate::core::status::HTTPError;
use crate::storage::proto::*;
use crate::storage::writer::RecordWriter;

/// Helper class for basic operations on blocks.
pub struct BlockManager {
    path: PathBuf,
    counters: HashMap<u64, u64>,
    current_block: Option<Rc<Block>>,
}

const DESCRIPTOR_FILE_EXT: &str = ".meta";
const DATA_FILE_EXT: &str = ".blk";

impl BlockManager {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            counters: HashMap::new(),
            current_block: None
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
    fn begin_write(
        &mut self,
        block: &Block,
        record_index: usize,
        content_length: u64,
    ) -> Result<RecordWriter, HTTPError> {
        let ts = block.begin_time.clone().unwrap();
        let path = self.build_path(&ts);
        self.counters.get_mut(&ts_to_u64(&ts)).map(|c| *c += 1);

        let writer = RecordWriter::new(
            path,
            block,
            record_index,
            content_length,
            RefCell::new(self),
        )?;

        Ok(writer)
    }

    fn build_path(&self, begin_time: &Timestamp) -> PathBuf {
        let block_id = ts_to_u64(&begin_time);
        self.path.join(format!("{}{}", block_id, DATA_FILE_EXT))
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
    fn load(&mut self, block_id: u64) -> Result<Rc<Block>, HTTPError>;
    /// Save block descriptor to disk.
    ///
    /// # Arguments
    ///
    /// * `block` - Block to save.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Block was saved successfully.
    fn save(&self, block: &Block) -> Result<(), HTTPError>;
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
    fn start(&mut self, begin_time: Timestamp, max_block_size: u64) -> Result<Rc<Block>, HTTPError>;
    /// Finish a block by truncating the file to the actual size.
    ///
    /// # Arguments
    ///
    /// * `block` - Block to finish.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Block was finished successfully.
    fn finish(&self, block: &Block) -> Result<(), HTTPError>;
    /// Unregister writer or reader.
    fn unregister(&mut self, block_id: u64);
}

impl ManageBlock for BlockManager {
    /// Load block descriptor from disk.
    ///
    /// # Arguments
    /// * `block_id` - ID of the block to load (begin time of the block).
    ///
    /// # Returns
    ///
    /// * `Ok(block)` - Block was loaded successfully.
    fn load(&mut self, block_id: u64) -> Result<Rc<Block>, HTTPError> {
        if self.current_block.is_some() && ts_to_u64((*self.current_block.as_ref().unwrap()).begin_time.as_ref().unwrap()) == block_id {
            return Ok(self.current_block.clone().unwrap());
        }
        let path = self
            .path
            .join(format!("{}.{}", block_id, DESCRIPTOR_FILE_EXT));
        let buf = std::fs::read(path)?;
        let block = Block::decode(Bytes::from(buf)).map_err(|e| {
            HTTPError::internal_server_error(&format!("Failed to decode block descriptor: {}", e))
        })?;

        self.current_block = Some(Rc::new(block));
        return Ok(self.current_block.clone().unwrap());
    }
    /// Save block descriptor to disk.
    ///
    /// # Arguments
    ///
    /// * `block` - Block to save.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Block was saved successfully.
    fn save(&self, block: &Block) -> Result<(), HTTPError> {
        let path = self.build_path(block.begin_time.as_ref().unwrap());
        let mut buf = BytesMut::new();
        block.encode(&mut buf).map_err(|e| {
            HTTPError::internal_server_error(&format!("Failed to encode block descriptor: {}", e))
        })?;
        let mut file = std::fs::File::create(path)?;
        file.write_all(&buf)?;
        Ok(())
    }
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
    fn start(&mut self, begin_time: Timestamp, max_block_size: u64) -> Result<Rc<Block>, HTTPError> {
        let block_id = ts_to_u64(&begin_time);
        let mut block = Block::default();
        block.begin_time = Some(begin_time);

        // create a block with data
        let file = std::fs::File::create(self.path.join(format!("{}{}", block_id, DATA_FILE_EXT)))?;
        file.set_len(max_block_size)?;

        self.save(&block)?;

        self.current_block = Some(Rc::new(block));
        Ok(self.current_block.clone().unwrap())
    }
    /// Finish a block by truncating the file to the actual size.
    ///
    /// # Arguments
    ///
    /// * `block` - Block to finish.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Block was finished successfully.
    fn finish(&self, block: &Block) -> Result<(), HTTPError> {
        let path = self.build_path(block.begin_time.as_ref().unwrap());
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
        file.set_len(block.size as u64)?;
        Ok(())
    }
    /// Unregister writer or reader.
    fn unregister(&mut self, block_id: u64) {
        self.counters.get_mut(&block_id).map(|c| *c -= 1);
        if self
            .counters
            .get(&block_id)
            .map(|c| *c == 0)
            .unwrap_or(false)
        {
            self.counters.remove(&block_id);
        }
    }
}
