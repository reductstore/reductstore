// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::block_manager::{
    BlockManager, BLOCK_INDEX_FILE, DATA_FILE_EXT, DESCRIPTOR_FILE_EXT,
};
use crate::storage::entry::{Entry, EntrySettings};
use crate::storage::proto::{block_index, ts_to_us, Block, BlockIndex, MinimalBlock};
use bytes::Bytes;
use bytesize::ByteSize;
use crc64fast::Digest;
use log::{debug, error, warn};
use prost::Message;
use reduct_base::error::ErrorCode::InternalServerError;
use reduct_base::error::ReductError;
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

pub(super) struct EntryLoader {}

impl EntryLoader {
    // Restore the entry from the given path
    pub fn restore_entry(path: PathBuf, options: EntrySettings) -> Result<Entry, ReductError> {
        let start_time = Instant::now();

        let entry = match Self::try_restore_entry_from_index(path.clone(), options.clone()) {
            Ok(entry) => return Ok(entry),
            Err(err) => {
                error!("{:}", err);
                Self::restore_entry_from_blocks(path, options)
            }
        }?;

        debug!(
            "Restored entry `{}` in {}ms: size={}, records={}",
            entry.name,
            start_time.elapsed().as_millis(),
            ByteSize::b(entry.size),
            entry.record_count
        );

        Ok(entry)
    }

    /// Restore the entry from blocks and create a new block index
    fn restore_entry_from_blocks(
        path: PathBuf,
        options: EntrySettings,
    ) -> Result<Entry, ReductError> {
        let mut record_count = 0;
        let mut size = 0;
        let mut block_index = BTreeSet::new();

        warn!("Failed to restore from block index. Trying to rebuild the entry from blocks");

        let block_index_path = path.join(BLOCK_INDEX_FILE);
        let mut block_index_proto = BlockIndex {
            blocks: vec![],
            crc64: 0,
        };

        let mut crc = Digest::new();
        for filein in fs::read_dir(path.clone())? {
            let file = filein?;
            let path = file.path();
            if path.is_dir() {
                continue;
            }

            let name = path.file_name().unwrap().to_str().unwrap();
            if !name.ends_with(DESCRIPTOR_FILE_EXT) {
                continue;
            }

            macro_rules! remove_bad_block {
                ($err:expr) => {{
                    error!("Failed to decode block {:?}: {}", path, $err);
                    warn!("Removing meta block {:?}", path);
                    let mut data_path = path.clone();
                    fs::remove_file(path)?;

                    data_path.set_extension(DATA_FILE_EXT[1..].to_string());
                    warn!("Removing data block {:?}", data_path);
                    fs::remove_file(data_path)?;
                    continue;
                }};
            }

            let buf = fs::read(path.clone())?;
            let mut block = match MinimalBlock::decode(Bytes::from(buf)) {
                Ok(block) => block,
                Err(err) => {
                    remove_bad_block!(err);
                }
            };

            // Migration for old blocks without fields to speed up the restore process
            // todo: remove this data_check in the future rel 1.12
            if block.record_count == 0 {
                debug!("Record count is 0. Migrate the block");
                let mut full_block = match Block::decode(Bytes::from(fs::read(path.clone())?)) {
                    Ok(block) => block,
                    Err(err) => {
                        remove_bad_block!(err);
                    }
                };

                full_block.record_count = full_block.records.len() as u64;
                full_block.metadata_size = full_block.encoded_len() as u64;

                block.record_count = full_block.record_count;
                block.metadata_size = full_block.metadata_size;

                let mut file = fs::File::create(path.clone())?;
                file.write_all(&full_block.encode_to_vec())?;
            }

            let id = if let Some(begin_time) = block.begin_time {
                ts_to_us(&begin_time)
            } else {
                remove_bad_block!("begin time mismatch");
            };

            if block.invalid {
                remove_bad_block!("block is invalid");
            }

            // Update CRC
            crc.write(&id.to_be_bytes());
            crc.write(&block.size.to_be_bytes());
            crc.write(&block.record_count.to_be_bytes());
            crc.write(&block.metadata_size.to_be_bytes());

            // Count total numbers
            record_count += block.record_count;
            size += block.size + block.metadata_size;
            block_index.insert(id);

            // Prepare the block index
            block_index_proto.blocks.push(block_index::Block {
                block_id: id,
                size: block.size,
                record_count: block.record_count,
                metadata_size: block.metadata_size,
            });
        }

        block_index_proto.crc64 = crc.sum64();
        let mut file = fs::File::create(block_index_path)?;
        file.write_all(&block_index_proto.encode_to_vec())?;

        let name = path.file_name().unwrap().to_str().unwrap().to_string();

        Ok(Entry {
            name,
            settings: options,
            block_index,
            block_manager: Arc::new(RwLock::new(BlockManager::new(path))),
            record_count,
            size,
            queries: HashMap::new(),
        })
    }

    /// Try to restore the entry from the block index
    fn try_restore_entry_from_index(
        path: PathBuf,
        options: EntrySettings,
    ) -> Result<Entry, ReductError> {
        let mut record_count = 0;
        let mut size = 0;
        let mut block_index = BTreeSet::new();

        let block_index_path = path.join(BLOCK_INDEX_FILE);
        if !block_index_path.exists() {
            return Err(ReductError::new(
                InternalServerError,
                &format!("Block index {:?} not found", block_index_path),
            ));
        }

        let buf = fs::read(block_index_path.clone())?;
        let block_index_proto = BlockIndex::decode(Bytes::from(buf));
        if let Err(err) = block_index_proto {
            return Err(ReductError::new(
                InternalServerError,
                &format!(
                    "Failed to decode block index {:?}: {}",
                    block_index_path, err
                ),
            ));
        }

        let block_index_proto = block_index_proto.unwrap();

        let mut crc = Digest::new();
        block_index_proto.blocks.into_iter().for_each(|block| {
            // Count total numbers
            block_index.insert(block.block_id);
            record_count += block.record_count;
            size += block.size + block.metadata_size;

            // Update CRC
            crc.write(&block.block_id.to_be_bytes());
            crc.write(&block.size.to_be_bytes());
            crc.write(&block.record_count.to_be_bytes());
            crc.write(&block.metadata_size.to_be_bytes());
        });

        if crc.sum64() != block_index_proto.crc64 {
            return Err(ReductError::new(
                InternalServerError,
                &format!("Block index {:?} is corrupted", block_index_path),
            ));
        }

        let name = path.file_name().unwrap().to_str().unwrap().to_string();
        Ok(Entry {
            name,
            settings: options,
            block_index,
            block_manager: Arc::new(RwLock::new(BlockManager::new(path))),
            record_count,
            size,
            queries: HashMap::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::block_manager::ManageBlock;
    use crate::storage::entry::tests::{entry, entry_settings, path, write_stub_record};
    use crate::storage::proto::{record, us_to_ts, Record};
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_restore(entry_settings: EntrySettings, path: PathBuf) {
        let mut entry = entry(entry_settings.clone(), path.clone());
        write_stub_record(&mut entry, 1).await.unwrap();
        write_stub_record(&mut entry, 2000010).await.unwrap();

        let bm = entry.block_manager.read().await;
        let records = bm.load(1).await.unwrap().records.clone();
        assert_eq!(records.len(), 2);
        assert_eq!(
            records[0],
            Record {
                timestamp: Some(us_to_ts(&1)),
                begin: 0,
                end: 10,
                content_type: "text/plain".to_string(),
                state: record::State::Finished as i32,
                labels: vec![],
            }
        );

        assert_eq!(
            records[1],
            Record {
                timestamp: Some(us_to_ts(&2000010)),
                begin: 10,
                end: 20,
                content_type: "text/plain".to_string(),
                state: record::State::Finished as i32,
                labels: vec![],
            }
        );

        let entry = EntryLoader::restore_entry(path.join(entry.name), entry_settings).unwrap();

        assert_eq!(entry.name(), "entry");
        assert_eq!(entry.record_count, 2);
        assert_eq!(entry.size, 84);

        let rec = entry.begin_read(1).await.unwrap();
        assert_eq!(rec.timestamp(), 1);
        assert_eq!(rec.content_length(), 10);
        assert_eq!(rec.content_type(), "text/plain");

        let mut rx = rec.into_rx();
        assert_eq!(
            rx.recv().await.unwrap().unwrap(),
            Bytes::from_static(b"0123456789")
        );

        let rec = entry.begin_read(2000010).await.unwrap();
        assert_eq!(rec.timestamp(), 2000010);
        assert_eq!(rec.content_length(), 10);
        assert_eq!(rec.content_type(), "text/plain");

        let mut rx = rec.into_rx();
        assert_eq!(
            rx.recv().await.unwrap().unwrap(),
            Bytes::from_static(b"0123456789")
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_restore_bad_block(entry_settings: EntrySettings, path: PathBuf) {
        let mut entry = entry(entry_settings.clone(), path.clone());

        write_stub_record(&mut entry, 1).await.unwrap();

        let meta_path = path.join("entry/1.meta");
        fs::write(meta_path.clone(), b"bad data").unwrap();
        let data_path = path.join("entry/1.blk");
        fs::write(data_path.clone(), b"bad data").unwrap();

        let entry = EntryLoader::restore_entry(path.join(entry.name), entry_settings).unwrap();
        assert_eq!(entry.name(), "entry");
        assert_eq!(entry.record_count, 0);

        assert!(!meta_path.exists(), "should remove meta block");
        assert!(!data_path.exists(), "should remove data block");
    }

    #[rstest]
    #[tokio::test]
    async fn test_migration_v18_v19(entry_settings: EntrySettings, path: PathBuf) {
        let path = path.join("entry");
        fs::create_dir_all(path.clone()).unwrap();
        let mut block_manager = BlockManager::new(path.clone());
        let mut block_v18 = block_manager.start(1, 100).await.unwrap();
        block_v18.records.push(Record {
            timestamp: Some(us_to_ts(&1)),
            begin: 0,
            end: 10,
            content_type: "text/plain".to_string(),
            state: record::State::Finished as i32,
            labels: vec![],
        });
        block_v18.records.push(Record {
            timestamp: Some(us_to_ts(&2)),
            begin: 0,
            end: 10,
            content_type: "text/plain".to_string(),
            state: record::State::Finished as i32,
            labels: vec![],
        });
        block_v18.size = 10;
        block_v18.begin_time = Some(us_to_ts(&1));
        block_manager.save(block_v18).await.unwrap();

        // repack the block
        let entry = EntryLoader::restore_entry(path.clone(), entry_settings).unwrap();
        let info = entry.info().await.unwrap();

        assert_eq!(info.size, 65);
        assert_eq!(info.record_count, 2);
        assert_eq!(info.block_count, 1);
        assert_eq!(info.oldest_record, 1);
        assert_eq!(info.latest_record, 2);

        let block_manager = BlockManager::new(path); // reload the block manager
        let block_v19 = block_manager.load(1).await.unwrap();
        assert_eq!(block_v19.record_count, 2);
        assert_eq!(block_v19.size, 10);
        assert_eq!(block_v19.metadata_size, 55);
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_block_index(path: PathBuf, entry_settings: EntrySettings) {
        let mut entry = entry(entry_settings.clone(), path.clone());
        write_stub_record(&mut entry, 1).await.unwrap();
        write_stub_record(&mut entry, 2000010).await.unwrap();
        let _ = entry.block_manager.read().await; // let finish the block

        EntryLoader::restore_entry(path.join(entry.name), entry_settings).unwrap();

        let block_index_path = path.join("entry").join(BLOCK_INDEX_FILE);
        assert_eq!(block_index_path.exists(), true, "should create block index");
        let block_index =
            BlockIndex::decode(Bytes::from(fs::read(block_index_path).unwrap())).unwrap();

        assert_eq!(block_index.blocks.len(), 1);
        assert_eq!(block_index.crc64, 1353523511124718486);
        assert_eq!(block_index.blocks[0].block_id, 1);
        assert_eq!(block_index.blocks[0].size, 20);
        assert_eq!(block_index.blocks[0].record_count, 2);
    }

    #[rstest]
    #[tokio::test]
    async fn test_check_integrity_block_index(path: PathBuf, entry_settings: EntrySettings) {
        let mut entry = entry(entry_settings.clone(), path.clone());
        write_stub_record(&mut entry, 1).await.unwrap();
        write_stub_record(&mut entry, 2000010).await.unwrap();
        let _ = entry.block_manager.read().await; // let finish the block

        EntryLoader::restore_entry(path.join(entry.name.clone()), entry_settings.clone()).unwrap();

        let block_index_path = path.join("entry").join(BLOCK_INDEX_FILE);
        assert_eq!(block_index_path.exists(), true, "should create block index");
        let mut block_index =
            BlockIndex::decode(Bytes::from(fs::read(block_index_path.clone()).unwrap())).unwrap();

        assert_eq!(block_index.blocks[0].size, 20);

        block_index.blocks[0].size = 30;
        fs::write(block_index_path.clone(), block_index.encode_to_vec()).unwrap();

        EntryLoader::restore_entry(path.join(entry.name), entry_settings).unwrap();

        let block_index =
            BlockIndex::decode(Bytes::from(fs::read(block_index_path).unwrap())).unwrap();
        assert_eq!(
            block_index.blocks[0].size, 20,
            "should restore the block index from the blocks"
        );
    }
}
