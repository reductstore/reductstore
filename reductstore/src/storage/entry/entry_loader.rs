// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use bytesize::ByteSize;
use log::{debug, error, trace, warn};
use prost::Message;
use tokio::sync::RwLock;

use reduct_base::error::ReductError;

use crate::storage::block_manager::block_index::BlockIndex;
use crate::storage::block_manager::wal::{create_wal, WalEntry};
use crate::storage::block_manager::{
    BlockManager, ManageBlock, BLOCK_INDEX_FILE, DATA_FILE_EXT, DESCRIPTOR_FILE_EXT,
};
use crate::storage::entry::{Entry, EntrySettings};
use crate::storage::proto::{ts_to_us, Block, MinimalBlock};

pub(super) struct EntryLoader {}

impl EntryLoader {
    // Restore the entry from the given path
    pub async fn restore_entry(
        path: PathBuf,
        options: EntrySettings,
    ) -> Result<Entry, ReductError> {
        let start_time = Instant::now();

        let mut entry =
            match Self::try_restore_entry_from_index(path.clone(), options.clone()).await {
                Ok(entry) => Ok(entry),
                Err(err) => {
                    error!("{:}", err);
                    Self::restore_entry_from_blocks(path.clone(), options).await
                }
            }?;

        Self::restore_uncommitted_changes(path, &mut entry).await?;

        {
            let bm = entry.block_manager.read().await;
            debug!(
                "Restored entry `{}` in {}ms: size={}, records={}",
                entry.name,
                start_time.elapsed().as_millis(),
                ByteSize::b(bm.index().size()),
                bm.index().record_count()
            );
        }
        Ok(entry)
    }

    /// Restore the entry from blocks and create a new block index
    async fn restore_entry_from_blocks(
        path: PathBuf,
        options: EntrySettings,
    ) -> Result<Entry, ReductError> {
        warn!("Failed to restore from block index. Trying to rebuild the entry from blocks");

        let mut block_index = BlockIndex::new(path.join(BLOCK_INDEX_FILE));
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

            if let Some(begin_time) = block.begin_time {
                ts_to_us(&begin_time)
            } else {
                remove_bad_block!("begin time mismatch");
            };

            block_index.insert_or_update(block);
        }

        block_index.save().await?;
        let name = path.file_name().unwrap().to_str().unwrap().to_string();

        Ok(Entry {
            name,
            settings: options,
            block_manager: Arc::new(RwLock::new(BlockManager::new(path, block_index))),
            queries: HashMap::new(),
        })
    }

    /// Try to restore the entry from the block index
    async fn try_restore_entry_from_index(
        path: PathBuf,
        options: EntrySettings,
    ) -> Result<Entry, ReductError> {
        let block_index = BlockIndex::try_load(path.join(BLOCK_INDEX_FILE)).await?;

        let name = path.file_name().unwrap().to_str().unwrap().to_string();
        Ok(Entry {
            name,
            settings: options,
            block_manager: Arc::new(RwLock::new(BlockManager::new(path, block_index))),
            queries: HashMap::new(),
        })
    }

    async fn restore_uncommitted_changes(
        entry_path: PathBuf,
        entry: &mut Entry,
    ) -> Result<(), ReductError> {
        let wal = create_wal(entry_path.clone());
        // There are uncommitted changes in the WALs
        let wal_blocks = wal.list().await?;
        if !wal_blocks.is_empty() {
            warn!(
                "Recovering uncommitted changes from WALs for entry: {:?}",
                entry_path
            );

            let mut block_manager = entry.block_manager.write().await;
            for block_id in wal_blocks {
                let wal_entries = wal.read(block_id).await;
                if let Err(err) = wal_entries {
                    error!("Failed to read WAL for block {}: {}", block_id, err);
                    wal.remove(block_id).await?;
                    continue;
                }

                let block_ref = if block_manager.exist(block_id).await? {
                    debug!(
                        "Loading block {}/{} from block manager",
                        entry.name, block_id
                    );
                    block_manager.load(block_id).await?
                } else {
                    debug!("Creating block {}/{} from WAL", entry.name, block_id);
                    Arc::new(RwLock::new(
                        crate::storage::block_manager::block::Block::new(block_id),
                    ))
                };

                let mut block_removed = false;
                {
                    let mut block = block_ref.write().await;
                    for wal_entry in wal_entries? {
                        match wal_entry {
                            WalEntry::WriteRecord(record) => {
                                trace!(
                                    "Write record to block {}/{}: {:?}",
                                    entry.name,
                                    block_id,
                                    record
                                );
                                block.insert_or_update_record(record);
                            }
                            WalEntry::UpdateRecord(record) => {
                                trace!(
                                    "Update record to block {}/{}: {:?}",
                                    entry.name,
                                    block_id,
                                    record
                                );
                                block.insert_or_update_record(record);
                            }
                            WalEntry::RemoveBlock => {
                                debug!("Remove block {}/{}", entry.name, block_id);
                                block_removed = true;
                                break;
                            }
                        }
                    }
                }

                if block_removed {
                    block_manager.remove(block_id).await?;
                } else {
                    block_manager.save(block_ref.clone()).await?;
                    block_manager.finish(block_ref).await?;
                }
            }

            block_manager.save_cache_on_disk().await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::block_manager::wal::WalEntry;
    use crate::storage::block_manager::ManageBlock;
    use crate::storage::entry::tests::{entry, entry_settings, path, write_stub_record};
    use crate::storage::proto::{record, us_to_ts, BlockIndex as BlockIndexProto, Record};

    use rstest::{fixture, rstest};

    use super::*;

    #[rstest]
    #[tokio::test]
    async fn test_restore(entry_settings: EntrySettings, path: PathBuf) {
        let mut entry = entry(entry_settings.clone(), path.clone());
        write_stub_record(&mut entry, 1).await.unwrap();
        write_stub_record(&mut entry, 2000010).await.unwrap();

        let mut bm = entry.block_manager.write().await;
        let records = bm
            .load(1)
            .await
            .unwrap()
            .read()
            .await
            .record_index()
            .clone();

        assert_eq!(records.len(), 2);
        assert_eq!(
            *records.get(&1).unwrap(),
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
            *records.get(&2000010).unwrap(),
            Record {
                timestamp: Some(us_to_ts(&2000010)),
                begin: 10,
                end: 20,
                content_type: "text/plain".to_string(),
                state: record::State::Finished as i32,
                labels: vec![],
            }
        );

        bm.save_cache_on_disk().await.unwrap();
        let entry = EntryLoader::restore_entry(path.join(entry.name), entry_settings)
            .await
            .unwrap();

        let info = entry.info().await.unwrap();
        assert_eq!(entry.name, "entry");
        assert_eq!(info.record_count, 2);
        assert_eq!(info.size, 88);

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
        fs::create_dir_all(path.join("entry")).unwrap();

        let meta_path = path.join("entry/1.meta");
        fs::write(meta_path.clone(), b"bad data").unwrap();
        let data_path = path.join("entry/1.blk");
        fs::write(data_path.clone(), b"bad data").unwrap();

        let entry = EntryLoader::restore_entry(path.join("entry"), entry_settings)
            .await
            .unwrap();
        let info = entry.info().await.unwrap();
        assert_eq!(info.name, "entry");
        assert_eq!(info.record_count, 0);

        assert!(!meta_path.exists(), "should remove meta block");
        assert!(!data_path.exists(), "should remove data block");
    }

    #[rstest]
    #[tokio::test]
    async fn test_migration_v18_v19(entry_settings: EntrySettings, path: PathBuf) {
        let path = path.join("entry");
        fs::create_dir_all(path.clone()).unwrap();
        let mut block_manager = BlockManager::new(
            path.clone(),
            BlockIndex::new(path.clone().join(BLOCK_INDEX_FILE)),
        );
        {
            let block_v18_ref = block_manager.start(1, 100).await.unwrap();
            let mut block_v18 = block_v18_ref.write().await;
            block_v18.insert_or_update_record(Record {
                timestamp: Some(us_to_ts(&1)),
                begin: 0,
                end: 10,
                content_type: "text/plain".to_string(),
                state: record::State::Finished as i32,
                labels: vec![],
            });
            block_v18.insert_or_update_record(Record {
                timestamp: Some(us_to_ts(&2000010)),
                begin: 10,
                end: 20,
                content_type: "text/plain".to_string(),
                state: record::State::Finished as i32,
                labels: vec![],
            });
        }
        block_manager.save_cache_on_disk().await.unwrap();

        // repack the block
        let entry = EntryLoader::restore_entry(path.clone(), entry_settings)
            .await
            .unwrap();
        let info = entry.info().await.unwrap();

        assert_eq!(info.size, 88);
        assert_eq!(info.record_count, 2);
        assert_eq!(info.block_count, 1);
        assert_eq!(info.oldest_record, 1);
        assert_eq!(info.latest_record, 2000010);

        let mut block_manager =
            BlockManager::new(path.clone(), BlockIndex::new(path.join(BLOCK_INDEX_FILE))); // reload the block manager
        let block_v19 = block_manager.load(1).await.unwrap().read().await.clone();
        assert_eq!(block_v19.record_count(), 2);
        assert_eq!(block_v19.size(), 20);
        assert_eq!(block_v19.metadata_size(), 46);
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_block_index(path: PathBuf, entry_settings: EntrySettings) {
        let mut entry = entry(entry_settings.clone(), path.clone());
        write_stub_record(&mut entry, 1).await.unwrap();
        write_stub_record(&mut entry, 2000010).await.unwrap();
        entry
            .block_manager
            .write()
            .await
            .save_cache_on_disk()
            .await
            .unwrap();

        EntryLoader::restore_entry(path.join(entry.name), entry_settings)
            .await
            .unwrap();

        let block_index_path = path.join("entry").join(BLOCK_INDEX_FILE);
        assert_eq!(block_index_path.exists(), true, "should create block index");
        let block_index =
            BlockIndexProto::decode(Bytes::from(fs::read(block_index_path).unwrap())).unwrap();

        assert_eq!(block_index.blocks.len(), 1);
        assert_eq!(block_index.crc64, 5634777224230458447);
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
        let _ = entry.block_manager.write().await.save_cache_on_disk().await;

        EntryLoader::restore_entry(path.join(entry.name.clone()), entry_settings.clone())
            .await
            .unwrap();

        let block_index_path = path.join("entry").join(BLOCK_INDEX_FILE);
        assert_eq!(block_index_path.exists(), true, "should create block index");
        let mut block_index =
            BlockIndexProto::decode(Bytes::from(fs::read(block_index_path.clone()).unwrap()))
                .unwrap();

        assert_eq!(block_index.blocks[0].size, 20);

        block_index.blocks[0].size = 30;
        let mut file = fs::File::create(block_index_path.clone()).unwrap();
        file.write_all(&block_index.encode_to_vec()).unwrap();
        file.sync_all().unwrap();

        EntryLoader::restore_entry(path.join(entry.name), entry_settings)
            .await
            .unwrap();

        let buf = fs::read(block_index_path).unwrap();
        let block_index = BlockIndexProto::decode(Bytes::from(buf)).unwrap();
        assert_eq!(
            block_index.blocks[0].size, 20,
            "should restore the block index from the blocks"
        );
    }

    mod wal_recovery {
        use crate::storage::proto::Record;
        use reduct_base::error::ErrorCode::InternalServerError;
        use tokio::fs::File;

        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_new_block(#[future] entry_fix: (Entry, PathBuf), record1: Record) {
            let (entry, path) = entry_fix.await;
            let mut wal = create_wal(path.clone());
            // Block #3 was created
            wal.append(3, WalEntry::WriteRecord(record1.clone()))
                .await
                .unwrap();

            let mut record2 = record1.clone();
            record2.timestamp = Some(us_to_ts(&2));
            wal.append(3, WalEntry::WriteRecord(record2.clone()))
                .await
                .unwrap();

            let entry = EntryLoader::restore_entry(path.clone(), entry.settings.clone())
                .await
                .unwrap();

            let block_ref = entry
                .block_manager
                .write()
                .await
                .load(3)
                .await
                .unwrap()
                .clone();
            let block = block_ref.read().await;
            assert_eq!(block.get_record(1), Some(&record1));
            assert_eq!(block.get_record(2), Some(&record2));

            let file = File::open(path.join("3.blk")).await.unwrap();
            assert_eq!(
                file.metadata().await.unwrap().len(),
                block.size(),
                "should save and truncate the block"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_update_block(#[future] entry_fix: (Entry, PathBuf), mut record1: Record) {
            let (entry, path) = entry_fix.await;
            let mut wal = create_wal(path.clone());

            // Block #1 was updated
            wal.append(1, WalEntry::WriteRecord(record1.clone()))
                .await
                .unwrap();
            record1.end = 20; //size 20
            wal.append(1, WalEntry::UpdateRecord(record1.clone()))
                .await
                .unwrap();

            let entry = EntryLoader::restore_entry(path.clone(), entry.settings.clone())
                .await
                .unwrap();

            let block_ref = entry.block_manager.write().await.load(1).await.unwrap();

            let block = block_ref.read().await;
            assert_eq!(block.get_record(1), Some(&record1));

            let file = File::open(path.join("1.blk")).await.unwrap();
            assert_eq!(
                file.metadata().await.unwrap().len(),
                block.size(),
                "should save and truncate the block"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_corrupted_wal(#[future] entry_fix: (Entry, PathBuf)) {
            let (entry, path) = entry_fix.await;

            fs::write(path.join("wal/1.wal"), b"bad data").unwrap();
            let entry = EntryLoader::restore_entry(path.clone(), entry.settings.clone()).await;
            assert!(entry.is_ok());
            assert!(
                !path.join("wal/1.wal").exists(),
                "should remove corrupted wal"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_remove_block(#[future] entry_fix: (Entry, PathBuf)) {
            let (entry, path) = entry_fix.await;
            let mut wal = create_wal(path.clone());

            // Block #1 was removed
            wal.append(1, WalEntry::RemoveBlock).await.unwrap();

            let entry = EntryLoader::restore_entry(path, entry.settings.clone())
                .await
                .unwrap();

            let block = entry.block_manager.write().await.load(1).await.clone();
            assert_eq!(block.err().unwrap().status, InternalServerError,);
        }

        #[fixture]
        fn record1() -> Record {
            Record {
                timestamp: Some(us_to_ts(&1)),
                begin: 0,
                end: 10,
                content_type: "text/plain".to_string(),
                state: record::State::Finished as i32,
                labels: vec![],
            }
        }

        #[fixture]
        async fn entry_fix(path: PathBuf, entry_settings: EntrySettings) -> (Entry, PathBuf) {
            let entry = entry(entry_settings.clone(), path.clone());
            let name = entry.name.clone();
            {
                let mut block_manager = entry.block_manager.write().await;

                block_manager.start(1, 10).await.unwrap();

                block_manager.start(2, 10).await.unwrap();
                block_manager.save_cache_on_disk().await.unwrap();
            }

            (entry, path.join(name))
        }
    }
}
