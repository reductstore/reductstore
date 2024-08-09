// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::entry::Entry;
use crate::storage::file_cache::get_global_file_cache;
use crate::storage::proto::block_index::Block as BlockEntry;
use crate::storage::proto::{ts_to_us, Block, BlockIndex as BlockIndexProto, MinimalBlock};
use bytes::Bytes;
use crc64fast::Digest;
use hex::decode;
use prost::Message;
use reduct_base::error::ErrorCode::InternalServerError;
use reduct_base::error::ReductError;
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::PathBuf;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

pub(super) struct BlockIndex {
    path_buf: PathBuf,
    index_info: HashMap<u64, BlockEntry>,
    index: BTreeSet<u64>,
    size: u64,
    record_count: u64,
}

impl BlockIndex {
    pub fn new(path_buf: PathBuf) -> Self {
        let index = BlockIndex {
            path_buf,
            index_info: HashMap::new(),
            index: BTreeSet::new(),
            size: 0,
            record_count: 0,
        };

        if !index.path_buf.exists() {
            let index_proto = BlockIndexProto {
                blocks: Vec::new(),
                crc64: 0,
            };
            fs::write(index.path_buf.clone(), index_proto.encode_to_vec()).unwrap();
        }

        index
    }

    pub fn insert_from_block(&mut self, block: &Block) {
        let entry = BlockEntry {
            block_id: ts_to_us(&block.begin_time.unwrap()),
            size: block.size,
            record_count: block.record_count,
            metadata_size: block.metadata_size,
            latest_record_time: block.latest_record_time,
        };

        self.insert(entry);
    }

    pub fn insert_from_min_block(&mut self, block: &MinimalBlock) {
        let entry = BlockEntry {
            block_id: ts_to_us(&block.begin_time.unwrap()),
            size: block.size,
            record_count: block.record_count,
            metadata_size: block.metadata_size,
            latest_record_time: block.latest_record_time,
        };

        self.insert(entry);
    }

    pub fn get(&self, block_id: u64) -> Option<&BlockEntry> {
        self.index_info.get(&block_id)
    }

    pub fn remove(&mut self, block_id: u64) -> Option<BlockEntry> {
        let block = self.index_info.remove(&block_id);

        if let Some(block) = &block {
            self.size -= block.size + block.metadata_size;
            self.record_count -= block.record_count;
        }
        self.index.remove(&block_id);

        block
    }

    pub async fn try_load(path: PathBuf) -> Result<Self, ReductError> {
        if !path.exists() {
            return Err(ReductError::new(
                InternalServerError,
                &format!("Block index {:?} not found", path),
            ));
        }

        let block_index_proto = {
            let file = get_global_file_cache().read(&path).await?;
            let mut lock = file.write().await;
            let mut buf = Vec::new();
            lock.read_to_end(&mut buf).await.map_err(|err| {
                ReductError::new(
                    InternalServerError,
                    &format!("Failed to read block index {:?}: {}", path, err),
                )
            })?;

            BlockIndexProto::decode(Bytes::from(buf))
        };

        if let Err(err) = block_index_proto {
            return Err(ReductError::new(
                InternalServerError,
                &format!("Failed to decode block index {:?}: {}", path, err),
            ));
        }

        let block_index_proto = block_index_proto.unwrap();
        let mut block_index = BlockIndex {
            path_buf: path.clone(),
            index_info: HashMap::new(),
            index: BTreeSet::new(),
            size: 0,
            record_count: 0,
        };

        let mut crc = Digest::new();
        block_index_proto.blocks.into_iter().for_each(|block| {
            // Count total numbers
            block_index.index_info.insert(block.block_id, block);
            block_index.record_count += block.record_count;
            block_index.size += block.size + block.metadata_size;

            // Update CRC
            crc.write(&block.block_id.to_be_bytes());
            crc.write(&block.size.to_be_bytes());
            crc.write(&block.record_count.to_be_bytes());
            crc.write(&block.metadata_size.to_be_bytes());
        });

        if crc.sum64() != block_index_proto.crc64 {
            return Err(ReductError::new(
                InternalServerError,
                &format!("Block index {:?} is corrupted", path),
            ));
        }

        Ok(block_index)
    }

    pub async fn save(&self) -> Result<(), ReductError> {
        let mut block_index_proto = BlockIndexProto {
            blocks: Vec::new(),
            crc64: 0,
        };

        block_index_proto.blocks = self
            .index_info
            .values()
            .map(|block| {
                let mut block_entry = BlockEntry::default();
                block_entry.block_id = block.block_id;
                block_entry.size = block.size;
                block_entry.record_count = block.record_count;
                block_entry.metadata_size = block.metadata_size;
                block_entry
            })
            .collect();

        let mut crc = Digest::new();
        block_index_proto.blocks.iter().for_each(|block| {
            crc.write(&block.block_id.to_be_bytes());
            crc.write(&block.size.to_be_bytes());
            crc.write(&block.record_count.to_be_bytes());
            crc.write(&block.metadata_size.to_be_bytes());
        });

        block_index_proto.crc64 = crc.sum64();
        let buf = block_index_proto.encode_to_vec();

        let mut file = get_global_file_cache()
            .write_or_create(&self.path_buf)
            .await?;
        let mut lock = file.write().await;
        lock.write_all(&buf).await.map_err(|err| {
            ReductError::new(
                InternalServerError,
                &format!("Failed to write block index {:?}: {}", self.path_buf, err),
            )
        })?;

        lock.sync_all().await?;

        Ok(())
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn record_count(&self) -> u64 {
        self.record_count
    }

    pub fn tree(&self) -> &BTreeSet<u64> {
        &self.index
    }

    fn insert(&mut self, new_block: BlockEntry) {
        match self.index_info.insert(new_block.block_id, new_block) {
            Some(block) => {
                // Remove old block
                self.size -= block.size + block.metadata_size;
                self.record_count -= block.record_count;
            }
            None => {}
        }

        self.size += new_block.size + new_block.metadata_size;
        self.record_count += new_block.record_count;
        self.index.insert(new_block.block_id);
    }
}
