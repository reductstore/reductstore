// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use bytes::Bytes;
use crc64fast::Digest;
use prost::Message;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::collections::{BTreeSet, HashMap};
use std::fs::read_dir;
use std::io::{Read, SeekFrom, Write};
use std::path::PathBuf;

use crate::core::file_cache::FILE_CACHE;
use crate::storage::block_manager::block::Block;
use crate::storage::block_manager::DESCRIPTOR_FILE_EXT;
use crate::storage::proto::block_index::Block as BlockEntry;
use crate::storage::proto::{
    ts_to_us, us_to_ts, Block as BlockProto, BlockIndex as BlockIndexProto, MinimalBlock,
};

#[derive(Debug)]
pub(in crate::storage) struct BlockIndex {
    path_buf: PathBuf,
    index_info: HashMap<u64, BlockEntry>,
    index: BTreeSet<u64>,
}

impl Into<BlockEntry> for MinimalBlock {
    fn into(self) -> BlockEntry {
        BlockEntry {
            block_id: ts_to_us(&self.begin_time.unwrap()),
            size: self.size,
            record_count: self.record_count,
            metadata_size: self.metadata_size,
            latest_record_time: self.latest_record_time,
            crc64: None,
        }
    }
}

impl Into<BlockEntry> for BlockProto {
    fn into(self) -> BlockEntry {
        BlockEntry {
            block_id: ts_to_us(&self.begin_time.unwrap()),
            size: self.size,
            record_count: self.record_count,
            metadata_size: self.metadata_size,
            latest_record_time: self.latest_record_time,
            crc64: None,
        }
    }
}

impl Into<BlockEntry> for Block {
    fn into(self) -> BlockEntry {
        BlockEntry {
            block_id: self.block_id(),
            size: self.size(),
            record_count: self.record_count(),
            metadata_size: self.metadata_size(),
            latest_record_time: Some(us_to_ts(&self.latest_record_time())),
            crc64: None,
        }
    }
}

impl BlockIndex {
    pub fn new(path_buf: PathBuf) -> Self {
        let index = BlockIndex {
            path_buf,
            index_info: HashMap::new(),
            index: BTreeSet::new(),
        };

        index
    }

    /// Insert  or update a new block entry into the index.
    ///
    /// # Arguments
    ///
    /// * `entry` - The block entry to insert.
    ///
    pub fn insert_or_update<T>(&mut self, entry: T)
    where
        T: Into<BlockEntry>,
    {
        self.insert(entry.into());
    }

    /// Insert or update a new block entry into the index with CRC.
    ///
    /// Must be used when the block is written to disk.
    ///
    /// # Arguments
    ///
    /// * `entry` - The block entry to insert.
    /// * `crc` - The CRC value.
    ///
    pub fn insert_or_update_with_crc<T>(&mut self, entry: T, crc: u64)
    where
        T: Into<BlockEntry>,
    {
        let mut block = entry.into();
        block.crc64 = Some(crc);
        self.insert(block);
    }

    pub fn get_block(&self, block_id: u64) -> Option<&BlockEntry> {
        self.index_info.get(&block_id)
    }

    pub fn remove_block(&mut self, block_id: u64) -> Option<BlockEntry> {
        let block = self.index_info.remove(&block_id);
        self.index.remove(&block_id);

        block
    }

    pub fn try_load(path: PathBuf) -> Result<Self, ReductError> {
        if !path.try_exists()? {
            return Err(internal_server_error!("Block index {:?} not found", path));
        }

        let block_index_proto = {
            let file = FILE_CACHE.read(&path, SeekFrom::Start(0))?.upgrade()?;
            let mut lock = file.write()?;
            let mut buf = Vec::new();
            if let Err(err) = lock.read_to_end(&mut buf) {
                return Err(internal_server_error!(
                    "Failed to read block index {:?}: {}",
                    path,
                    err
                ));
            };

            let has_block_descriptors = read_dir(&path.parent().unwrap())?.any(|file| {
                file.map(|f| {
                    f.file_name()
                        .to_str()
                        .unwrap()
                        .ends_with(DESCRIPTOR_FILE_EXT)
                })
                .unwrap_or(false)
            });

            if lock.metadata()?.len() == 0 && has_block_descriptors {
                return Err(internal_server_error!("Block index {:?} is empty", path));
            }

            BlockIndexProto::decode(Bytes::from(buf))
        };

        if let Err(err) = block_index_proto {
            return Err(internal_server_error!(
                "Failed to decode block index {:?}: {}",
                path,
                err
            ));
        }

        let block_index: BlockIndex = BlockIndex::from_proto(path, block_index_proto.unwrap())?;
        Ok(block_index)
    }

    pub fn from_proto(path: PathBuf, value: BlockIndexProto) -> Result<Self, ReductError> {
        let mut block_index = BlockIndex {
            path_buf: path.clone(),
            index_info: HashMap::new(),
            index: BTreeSet::new(),
        };

        let mut crc = Digest::new();
        value.blocks.into_iter().for_each(|block| {
            // Count total numbers
            block_index.index_info.insert(block.block_id, block);

            // Update CRC
            crc.write(&block.block_id.to_be_bytes());
            crc.write(&block.size.to_be_bytes());
            crc.write(&block.record_count.to_be_bytes());
            crc.write(&block.metadata_size.to_be_bytes());
            crc.write(&ts_to_us(&block.latest_record_time.unwrap()).to_be_bytes());

            if let Some(crc64) = block.crc64 {
                crc.write(&crc64.to_be_bytes());
            }

            block_index.index.insert(block.block_id);
            block_index.index_info.insert(block.block_id, block);
        });

        if crc.sum64() != value.crc64 {
            return Err(internal_server_error!(
                "Block index {:?} is corrupted",
                path
            ));
        }

        Ok(block_index)
    }

    pub fn save(&self) -> Result<(), ReductError> {
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
                block_entry.latest_record_time = block.latest_record_time;
                block_entry.crc64 = block.crc64;
                block_entry
            })
            .collect();

        let mut crc = Digest::new();
        block_index_proto.blocks.iter().for_each(|block| {
            crc.write(&block.block_id.to_be_bytes());
            crc.write(&block.size.to_be_bytes());
            crc.write(&block.record_count.to_be_bytes());
            crc.write(&block.metadata_size.to_be_bytes());
            crc.write(&ts_to_us(&block.latest_record_time.unwrap()).to_be_bytes());

            if let Some(crc64) = block.crc64 {
                crc.write(&crc64.to_be_bytes());
            }
        });

        block_index_proto.crc64 = crc.sum64();
        let buf = block_index_proto.encode_to_vec();

        let file = FILE_CACHE
            .write_or_create(&self.path_buf, SeekFrom::Start(0))?
            .upgrade()?;
        let mut lock = file.write()?;
        lock.set_len(0)?;
        lock.write_all(&buf).map_err(|err| {
            internal_server_error!("Failed to write block index {:?}: {}", self.path_buf, err)
        })?;

        lock.flush()?;
        lock.sync_all()?;

        Ok(())
    }

    pub fn size(&self) -> u64 {
        self.index_info
            .iter()
            .fold(0, |acc, (_, block)| acc + block.size + block.metadata_size)
    }

    pub fn record_count(&self) -> u64 {
        self.index_info
            .iter()
            .fold(0, |acc, (_, block)| acc + block.record_count)
    }

    pub fn tree(&self) -> &BTreeSet<u64> {
        &self.index
    }

    fn insert(&mut self, block: BlockEntry) {
        self.index_info.insert(block.block_id, block);
        self.index.insert(block.block_id);
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use prost_wkt_types::Timestamp;
    use rstest::rstest;
    use tempfile::tempdir;

    use crate::storage::block_manager::BLOCK_INDEX_FILE;
    use crate::storage::proto::block_index::Block as BlockEntry;

    use super::*;

    mod try_load {
        use super::*;

        #[rstest]
        fn test_ok() {
            let path = tempdir().unwrap().keep().join(BLOCK_INDEX_FILE);

            let block_index_proto = BlockIndexProto {
                blocks: vec![BlockEntry {
                    block_id: 1,
                    size: 1,
                    record_count: 1,
                    metadata_size: 1,
                    latest_record_time: Some(Timestamp::default()),
                    crc64: None,
                }],
                crc64: 294433432134063049,
            };
            fs::write(&path, block_index_proto.encode_to_vec()).unwrap();

            let block_index = BlockIndex::try_load(path.clone()).unwrap();
            assert_eq!(block_index.size(), 2);
            assert_eq!(block_index.record_count(), 1);
            assert_eq!(block_index.tree().len(), 1);
            assert_eq!(block_index.path_buf, path);
        }

        #[rstest]
        fn test_index_file_not_found() {
            let path = PathBuf::from("not_found");
            let block_index = BlockIndex::try_load(path.clone()).err().unwrap();
            assert_eq!(
                block_index,
                internal_server_error!("Block index {:?} not found", path)
            );
        }

        #[rstest]
        fn test_index_file_corrupted() {
            let path = tempdir().unwrap().keep().join(BLOCK_INDEX_FILE);

            let block_index_proto = BlockIndexProto {
                blocks: vec![BlockEntry {
                    block_id: 1,
                    size: 1,
                    record_count: 1,
                    metadata_size: 1,
                    latest_record_time: Some(Timestamp::default()),
                    crc64: None,
                }],
                crc64: 0,
            };
            fs::write(&path, block_index_proto.encode_to_vec()).unwrap();

            let block_index = BlockIndex::try_load(path.clone()).err().unwrap();
            assert_eq!(
                block_index,
                internal_server_error!("Block index {:?} is corrupted", path)
            );
        }

        #[rstest]
        fn test_decode_err() {
            let path = tempdir().unwrap().keep().join(BLOCK_INDEX_FILE);
            fs::write(&path, vec![0, 1, 2, 3]).unwrap();

            let block_index = BlockIndex::try_load(path.clone()).err().unwrap();
            assert_eq!(block_index, internal_server_error!("Failed to decode block index {:?}: failed to decode Protobuf message: invalid tag value: 0", path));
        }
    }

    mod save {
        use super::*;

        #[rstest]
        fn test_ok() {
            let path = tempdir().unwrap().keep().join(BLOCK_INDEX_FILE);

            let mut block_index = BlockIndex::new(path.clone());
            block_index.insert_or_update(BlockEntry {
                block_id: 1,
                size: 1,
                record_count: 1,
                metadata_size: 1,
                latest_record_time: Some(Timestamp::default()),
                crc64: None,
            });

            block_index.save().unwrap();

            let block_index_proto = BlockIndex::try_load(path.clone()).unwrap();
            assert_eq!(block_index_proto.size(), 2);
            assert_eq!(block_index_proto.record_count(), 1);
            assert_eq!(block_index_proto.tree().len(), 1);
        }
    }
}
