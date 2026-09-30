// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use bytes::Bytes;
use crc64fast::Digest;
use prost::Message;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::collections::{BTreeSet, HashMap};
use std::io::{Read, SeekFrom, Write};
use std::path::PathBuf;

use crate::core::file_cache::FILE_CACHE;
use crate::storage::block_manager::block::Block;
use crate::storage::block_manager::{COMPRESSED_DESCRIPTOR_FILE_EXT, DESCRIPTOR_FILE_EXT};
use crate::storage::proto::block_index::Block as BlockEntry;
use crate::storage::proto::{
    ts_to_us, us_to_ts, Block as BlockProto, BlockIndex as BlockIndexProto, MinimalBlock,
};

#[derive(Debug)]
pub(in crate::storage) struct BlockIndex {
    path_buf: PathBuf,
    index_info: HashMap<u64, BlockEntry>,
    index: BTreeSet<u64>,
    publication_generation: Option<u64>,
    publication_incarnation: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::storage) struct Publication {
    pub generation: u64,
    pub incarnation: String,
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
            compression: None,
            corrupted: self.corrupted,
            version: self.version,
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
            compression: None,
            corrupted: self.corrupted,
            version: self.version,
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
            compression: None,
            corrupted: None,
            version: None,
        }
    }
}

impl BlockIndex {
    pub fn new(path_buf: PathBuf) -> Self {
        let index = BlockIndex {
            path_buf,
            index_info: HashMap::new(),
            index: BTreeSet::new(),
            publication_generation: None,
            publication_incarnation: None,
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

    pub fn get_block_mut(&mut self, block_id: u64) -> Option<&mut BlockEntry> {
        self.index_info.get_mut(&block_id)
    }

    pub fn remove_block(&mut self, block_id: u64) -> Option<BlockEntry> {
        let block = self.index_info.remove(&block_id);
        self.index.remove(&block_id);

        block
    }

    pub fn is_corrupted(&self, block_id: u64) -> bool {
        self.index_info
            .get(&block_id)
            .is_some_and(|block| block.corrupted == Some(true))
    }

    pub fn mark_corrupted(&mut self, block_id: u64) {
        if let Some(block) = self.index_info.get_mut(&block_id) {
            block.corrupted = Some(true);
        }
    }

    pub fn corrupted_block_count(&self) -> usize {
        self.corrupted_block_ids().len()
    }

    pub fn corrupted_block_ids(&self) -> Vec<u64> {
        self.index
            .iter()
            .copied()
            .filter(|block_id| self.is_corrupted(*block_id))
            .collect()
    }

    pub fn active_tree(&self) -> BTreeSet<u64> {
        self.index
            .iter()
            .copied()
            .filter(|block_id| !self.is_corrupted(*block_id))
            .collect()
    }

    pub async fn try_load(path: PathBuf) -> Result<Self, ReductError> {
        if !FILE_CACHE.try_exists(&path).await? {
            return Err(internal_server_error!("Block index {:?} not found", path));
        }

        let mut lock = FILE_CACHE.read(&path, SeekFrom::Start(0)).await?;
        let mut buf = Vec::new();
        if let Err(err) = lock.read_to_end(&mut buf) {
            return Err(internal_server_error!(
                "Failed to read block index {:?}: {}",
                path,
                err
            ));
        };

        if lock.metadata()?.len() == 0 {
            // If the index file is empty, check if there are any block descriptors.
            // If there are, the index file is corrupted.
            let has_block_descriptors = FILE_CACHE
                .read_dir(&path.parent().unwrap().into())
                .await?
                .iter()
                .any(|path| {
                    path.ends_with(DESCRIPTOR_FILE_EXT)
                        || path.ends_with(COMPRESSED_DESCRIPTOR_FILE_EXT)
                });

            if has_block_descriptors {
                return Err(internal_server_error!("Block index {:?} is empty", path));
            }
        }

        let block_index_proto = BlockIndexProto::decode(Bytes::from(buf)).map_err(|err| {
            internal_server_error!("Failed to decode block index {:?}: {}", path, err)
        })?;

        let block_index: BlockIndex = BlockIndex::from_proto(path, block_index_proto)?;
        Ok(block_index)
    }

    pub fn from_proto(path: PathBuf, value: BlockIndexProto) -> Result<Self, ReductError> {
        let mut block_index = BlockIndex {
            path_buf: path.clone(),
            index_info: HashMap::new(),
            index: BTreeSet::new(),
            publication_generation: value.publication_generation,
            publication_incarnation: value.publication_incarnation,
        };

        block_index.validate_publication()?;

        let mut crc = Digest::new();
        value.blocks.into_iter().for_each(|block| {
            let latest_record_time = ts_to_us(block.latest_record_time.as_ref().unwrap());

            crc.write(&block.block_id.to_be_bytes());
            crc.write(&block.size.to_be_bytes());
            crc.write(&block.record_count.to_be_bytes());
            crc.write(&block.metadata_size.to_be_bytes());
            crc.write(&latest_record_time.to_be_bytes());

            if let Some(crc64) = block.crc64 {
                crc.write(&crc64.to_be_bytes());
            }

            if let Some(corrupted) = block.corrupted {
                crc.write(&(corrupted as u8).to_be_bytes());
            }

            if let Some(version) = block.version {
                crc.write(&version.to_be_bytes());
            }

            block_index.index.insert(block.block_id);
            block_index.index_info.insert(block.block_id, block);
        });

        if let Some(publication) = block_index.publication() {
            Self::write_publication_crc(&mut crc, &publication);
        }

        if crc.sum64() != value.crc64 {
            return Err(internal_server_error!(
                "Block index {:?} is corrupted",
                path
            ));
        }

        Ok(block_index)
    }

    pub async fn save(&self) -> Result<(), ReductError> {
        let mut block_index_proto = BlockIndexProto {
            blocks: Vec::new(),
            crc64: 0,
            publication_generation: self.publication_generation,
            publication_incarnation: self.publication_incarnation.clone(),
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
                block_entry.compression = block.compression;
                block_entry.corrupted = block.corrupted;
                block_entry.version = block.version;
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

            if let Some(corrupted) = block.corrupted {
                crc.write(&(corrupted as u8).to_be_bytes());
            }

            if let Some(version) = block.version {
                crc.write(&version.to_be_bytes());
            }
        });

        if let Some(publication) = self.publication() {
            Self::write_publication_crc(&mut crc, &publication);
        }

        block_index_proto.crc64 = crc.sum64();
        let buf = block_index_proto.encode_to_vec();

        let mut lock = FILE_CACHE
            .write_or_create(&self.path_buf, SeekFrom::Start(0))
            .await?;
        lock.set_len(0)?;
        lock.write_all(&buf).map_err(|err| {
            internal_server_error!("Failed to write block index {:?}: {}", self.path_buf, err)
        })?;

        lock.flush_local().await?;

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

    pub fn info(&self) -> &HashMap<u64, BlockEntry> {
        &self.index_info
    }

    pub async fn sync_all(&mut self) -> Result<(), ReductError> {
        let mut lock = FILE_CACHE
            .write_or_create(&self.path_buf, SeekFrom::Start(0))
            .await?;
        lock.sync_all().await?;
        Ok(())
    }

    pub fn publication(&self) -> Option<Publication> {
        match (
            self.publication_generation,
            self.publication_incarnation.as_ref(),
        ) {
            (Some(generation), Some(incarnation)) => Some(Publication {
                generation,
                incarnation: incarnation.clone(),
            }),
            _ => None,
        }
    }

    pub fn set_publication(&mut self, publication: Publication) -> Result<(), ReductError> {
        if publication.generation == 0 || publication.incarnation.is_empty() {
            return Err(internal_server_error!(
                "Invalid block index publication state"
            ));
        }

        self.publication_generation = Some(publication.generation);
        self.publication_incarnation = Some(publication.incarnation);
        Ok(())
    }

    fn validate_publication(&self) -> Result<(), ReductError> {
        match (
            self.publication_generation,
            self.publication_incarnation.as_deref(),
        ) {
            (None, None) => Ok(()),
            (Some(0), _) | (_, Some("")) | (None, Some(_)) | (Some(_), None) => {
                Err(internal_server_error!(
                    "Block index {:?} has invalid publication state",
                    self.path_buf
                ))
            }
            (Some(_), Some(_)) => Ok(()),
        }
    }

    fn write_publication_crc(crc: &mut Digest, publication: &Publication) {
        // Legacy indexes omit these bytes entirely. The tag and explicit length
        // make the extension unambiguous without changing legacy CRCs.
        crc.write(b"publication\0");
        crc.write(&publication.generation.to_be_bytes());
        crc.write(&(publication.incarnation.len() as u64).to_be_bytes());
        crc.write(publication.incarnation.as_bytes());
    }

    fn insert(&mut self, block: BlockEntry) {
        self.index_info.insert(block.block_id, block);
        self.index.insert(block.block_id);
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::storage::block_manager::BLOCK_INDEX_FILE;
    use crate::storage::proto::block_index::Block as BlockEntry;
    use prost_wkt_types::Timestamp;
    use rstest::rstest;
    use tempfile::tempdir;

    use super::*;

    mod try_load {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_ok() {
            let path = tempdir().unwrap().keep().join(BLOCK_INDEX_FILE);

            let block_index_proto = BlockIndexProto {
                blocks: vec![BlockEntry {
                    block_id: 1,
                    size: 1,
                    record_count: 1,
                    metadata_size: 1,
                    latest_record_time: Some(Timestamp::default()),
                    crc64: None,
                    compression: None,
                    corrupted: None,
                    version: None,
                }],
                crc64: 294433432134063049,
                publication_generation: None,
                publication_incarnation: None,
            };
            fs::write(&path, block_index_proto.encode_to_vec()).unwrap();

            let block_index = BlockIndex::try_load(path.clone()).await.unwrap();
            assert_eq!(block_index.size(), 2);
            assert_eq!(block_index.record_count(), 1);
            assert_eq!(block_index.tree().len(), 1);
            assert_eq!(block_index.path_buf, path);
        }

        #[rstest]
        #[tokio::test]
        async fn test_index_file_not_found() {
            let path = PathBuf::from("not_found");
            let block_index = BlockIndex::try_load(path.clone()).await.err().unwrap();
            assert_eq!(
                block_index,
                internal_server_error!("Block index {:?} not found", path)
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_index_file_corrupted() {
            let path = tempdir().unwrap().keep().join(BLOCK_INDEX_FILE);

            let block_index_proto = BlockIndexProto {
                blocks: vec![BlockEntry {
                    block_id: 1,
                    size: 1,
                    record_count: 1,
                    metadata_size: 1,
                    latest_record_time: Some(Timestamp::default()),
                    crc64: None,
                    compression: None,
                    corrupted: None,
                    version: None,
                }],
                crc64: 0,
                publication_generation: None,
                publication_incarnation: None,
            };
            fs::write(&path, block_index_proto.encode_to_vec()).unwrap();

            let block_index = BlockIndex::try_load(path.clone()).await.err().unwrap();
            assert_eq!(
                block_index,
                internal_server_error!("Block index {:?} is corrupted", path)
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_decode_err() {
            let path = tempdir().unwrap().keep().join(BLOCK_INDEX_FILE);
            fs::write(&path, vec![0, 1, 2, 3]).unwrap();

            let block_index = BlockIndex::try_load(path.clone()).await.err().unwrap();
            assert_eq!(block_index, internal_server_error!("Failed to decode block index {:?}: failed to decode Protobuf message: invalid tag value: 0", path));
        }
    }

    mod save {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_ok() {
            let path = tempdir().unwrap().keep().join(BLOCK_INDEX_FILE);

            let mut block_index = BlockIndex::new(path.clone());
            block_index.insert_or_update(BlockEntry {
                block_id: 1,
                size: 1,
                record_count: 1,
                metadata_size: 1,
                latest_record_time: Some(Timestamp::default()),
                crc64: None,
                compression: None,
                corrupted: None,
                version: None,
            });

            block_index.save().await.unwrap();

            let block_index_proto = BlockIndex::try_load(path.clone()).await.unwrap();
            assert_eq!(block_index_proto.size(), 2);
            assert_eq!(block_index_proto.record_count(), 1);
            assert_eq!(block_index_proto.tree().len(), 1);
        }

        #[rstest]
        #[tokio::test]
        async fn test_corrupted_round_trip() {
            let path = tempdir().unwrap().keep().join(BLOCK_INDEX_FILE);

            let mut block_index = BlockIndex::new(path.clone());
            block_index.insert_or_update(BlockEntry {
                block_id: 1,
                size: 1,
                record_count: 1,
                metadata_size: 1,
                latest_record_time: Some(Timestamp::default()),
                crc64: None,
                compression: None,
                corrupted: None,
                version: None,
            });
            block_index.mark_corrupted(1);

            assert!(block_index.is_corrupted(1));
            assert_eq!(block_index.corrupted_block_count(), 1);
            assert_eq!(block_index.corrupted_block_ids(), vec![1]);

            block_index.save().await.unwrap();

            let block_index = BlockIndex::try_load(path).await.unwrap();
            assert!(block_index.is_corrupted(1));
            assert_eq!(block_index.corrupted_block_count(), 1);
            assert_eq!(block_index.corrupted_block_ids(), vec![1]);
        }

        #[rstest]
        #[tokio::test]
        async fn preserves_publication_state_and_includes_it_in_crc() {
            let path = tempdir().unwrap().keep().join(BLOCK_INDEX_FILE);
            let mut block_index = BlockIndex::new(path.clone());
            block_index
                .set_publication(Publication {
                    generation: 2,
                    incarnation: "writer-a".to_string(),
                })
                .unwrap();

            block_index.save().await.unwrap();

            let index = BlockIndex::try_load(path.clone()).await.unwrap();
            assert_eq!(
                index.publication(),
                Some(Publication {
                    generation: 2,
                    incarnation: "writer-a".to_string(),
                })
            );

            let mut proto = BlockIndexProto::decode(fs::read(&path).unwrap().as_slice()).unwrap();
            proto.publication_generation = Some(4);
            fs::write(&path, proto.encode_to_vec()).unwrap();
            assert!(BlockIndex::try_load(path).await.is_err());
        }

        #[rstest]
        #[case(None, Some("writer"))]
        #[case(Some(2), None)]
        #[case(Some(0), Some("writer"))]
        #[case(Some(2), Some(""))]
        fn rejects_invalid_publication_state(
            #[case] generation: Option<u64>,
            #[case] incarnation: Option<&str>,
        ) {
            let proto = BlockIndexProto {
                blocks: vec![],
                crc64: 0,
                publication_generation: generation,
                publication_incarnation: incarnation.map(str::to_string),
            };

            assert!(BlockIndex::from_proto(PathBuf::from("blocks.idx"), proto).is_err());
        }
    }
}
