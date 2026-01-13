// Copyright 2024-2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

use std::collections::{HashMap, HashSet};
use std::io::{Read, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use crc64fast::Digest;
use log::{debug, error, info, trace, warn};
use prost::Message;

use crate::cfg::Cfg;
use crate::cfg::InstanceRole::Replica;
use crate::core::file_cache::FILE_CACHE;
use crate::core::sync::AsyncRwLock;
use crate::storage::block_manager::block_index::BlockIndex;
use crate::storage::block_manager::wal::{create_wal, WalEntry};
use crate::storage::block_manager::{
    BlockManager, BLOCK_INDEX_FILE, DATA_FILE_EXT, DESCRIPTOR_FILE_EXT,
};
use crate::storage::entry::{Entry, EntrySettings};
use crate::storage::proto::{ts_to_us, Block, MinimalBlock};
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use reduct_base::msg::status::ResourceStatus;

pub(super) struct EntryLoader {}

impl EntryLoader {
    // Restore the entry from the given path
    pub async fn restore_entry(
        path: PathBuf,
        options: EntrySettings,
        cfg: Arc<Cfg>,
    ) -> Result<Option<Entry>, ReductError> {
        let start_time = Instant::now();

        let mut entry =
            match Self::try_restore_entry_from_index(path.clone(), options.clone(), cfg.clone())
                .await
            {
                Ok(entry) => entry,
                Err(err) => {
                    if cfg.role == Replica {
                        return Ok(None);
                    }

                    warn!(
                        "Failed to restore from block index {:?}: {}",
                        path, err.message
                    );
                    info!("Rebuilding the block index {:?} from blocks", path);
                    Self::restore_entry_from_blocks(path.clone(), options.clone(), cfg.clone())
                        .await?
                }
            };

        Self::restore_uncommitted_changes(path.clone(), &mut entry).await?;

        if cfg.engine_config.enable_integrity_checks {
            let needs_rebuild = {
                let bm = entry.block_manager.read().await?;
                let file_list = FILE_CACHE
                    .read_dir(&path)
                    .await?
                    .into_iter()
                    .collect::<HashSet<PathBuf>>();

                Self::check_if_block_files_exist(&path, &file_list, &bm.index())
                    .await
                    .is_err()
                    || Self::check_descriptor_count(&path, &file_list, &bm.index()).is_err()
            };

            if needs_rebuild {
                warn!("Block index is inconsistent. Rebuilding the block index from blocks");
                entry = Self::restore_entry_from_blocks(path.clone(), options, cfg.clone()).await?;
            }
        }

        {
            let bm = entry.block_manager.read().await?;
            debug!(
                "Restored entry `{}` in {}ms: size={}, records={}",
                entry.name,
                start_time.elapsed().as_millis(),
                bm.index().size(),
                bm.index().record_count()
            );
        }

        entry.cfg = cfg;
        Ok(Some(entry))
    }

    /// Restore the entry from blocks and create a new block index
    async fn restore_entry_from_blocks(
        path: PathBuf,
        options: EntrySettings,
        cfg: Arc<Cfg>,
    ) -> Result<Entry, ReductError> {
        async fn remove_block_files(path: &PathBuf) -> Result<(), ReductError> {
            warn!("Removing meta block {:?}", path);
            FILE_CACHE.remove(path).await?;

            let mut data_path = path.clone();
            data_path.set_extension(DATA_FILE_EXT[1..].to_string());
            warn!("Removing data block {:?}", data_path);
            FILE_CACHE.remove(&data_path).await?;
            Ok(())
        }

        let mut block_index = BlockIndex::new(path.join(BLOCK_INDEX_FILE));
        for path in FILE_CACHE.read_dir(&path).await? {
            if path.is_dir() {
                continue;
            }

            let name = path.file_name().unwrap().to_str().unwrap();
            if !name.ends_with(DESCRIPTOR_FILE_EXT) {
                continue;
            }

            let buf = {
                let mut file = FILE_CACHE.read(&path, SeekFrom::Start(0)).await?;
                let mut buf = vec![];
                file.read_to_end(&mut buf)?;
                buf
            };

            let mut crc = Digest::new();
            crc.write(&buf);

            let descriptor_content = Bytes::from(buf);
            let mut block = match MinimalBlock::decode(descriptor_content.clone()) {
                Ok(block) => block,
                Err(err) => {
                    error!("Failed to decode block {:?}: {}", path, err);
                    remove_block_files(&path).await?;
                    continue;
                }
            };

            if block.record_count == 0 {
                debug!("Record count is 0. Migrate the block");
                let mut full_block = match Block::decode(descriptor_content.clone()) {
                    Ok(block) => block,
                    Err(err) => {
                        error!("Failed to decode block {:?}: {}", path, err);
                        remove_block_files(&path).await?;
                        continue;
                    }
                };

                full_block.record_count = full_block.records.len() as u64;
                full_block.metadata_size = full_block.encoded_len() as u64;

                block.record_count = full_block.record_count;
                block.metadata_size = full_block.metadata_size;

                let mut file = FILE_CACHE
                    .write_or_create(&path, SeekFrom::Start(0))
                    .await?;
                file.set_len(0)?;

                let buf = full_block.encode_to_vec();
                crc = Digest::new();
                crc.write(&buf);
                file.write_all(&buf)?;
            }

            if let Some(begin_time) = block.begin_time {
                ts_to_us(&begin_time)
            } else {
                warn!("Block {:?} has no begin time", path);
                remove_block_files(&path).await?;
                continue;
            };

            block_index.insert_or_update_with_crc(block, crc.sum64());
        }

        block_index.save().await?;
        let name = path.file_name().unwrap().to_str().unwrap().to_string();
        let bucket_name = path
            .parent()
            .unwrap()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        Ok(Entry {
            name,
            bucket_name,
            settings: AsyncRwLock::new(options),
            block_manager: Arc::new(AsyncRwLock::new(
                BlockManager::build(path.clone(), block_index, cfg.clone()).await,
            )),
            queries: Arc::new(AsyncRwLock::new(HashMap::new())),
            status: AsyncRwLock::new(ResourceStatus::Ready),
            path,
            cfg,
        })
    }

    /// Try to restore the entry from the block index
    async fn try_restore_entry_from_index(
        path: PathBuf,
        options: EntrySettings,
        cfg: Arc<Cfg>,
    ) -> Result<Entry, ReductError> {
        let block_index = BlockIndex::try_load(path.join(BLOCK_INDEX_FILE)).await?;
        let name = path.file_name().unwrap().to_str().unwrap().to_string();

        let bucket_name = path
            .parent()
            .unwrap()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        Ok(Entry {
            name,
            bucket_name,
            settings: AsyncRwLock::new(options),
            block_manager: Arc::new(AsyncRwLock::new(
                BlockManager::build(path.clone(), block_index, cfg.clone()).await,
            )),
            queries: Arc::new(AsyncRwLock::new(HashMap::new())),
            status: AsyncRwLock::new(ResourceStatus::Ready),
            path,
            cfg,
        })
    }

    fn check_descriptor_count(
        path: &PathBuf,
        file_list: &HashSet<PathBuf>,
        block_index: &BlockIndex,
    ) -> Result<(), ReductError> {
        let number_of_descriptors = file_list
            .iter()
            .filter(|entry|
                // path maybe a virtual from remote storage
                entry.to_str().unwrap_or("").ends_with(DESCRIPTOR_FILE_EXT))
            .count();

        if number_of_descriptors != block_index.tree().len() {
            warn!(
                "Number of descriptors {} does not match block index {} in entry {:?}",
                number_of_descriptors,
                block_index.tree().len(),
                path
            );

            Err(internal_server_error!(""))
        } else {
            Ok(())
        }
    }

    async fn check_if_block_files_exist(
        path: &PathBuf,
        file_list: &HashSet<PathBuf>,
        block_index: &BlockIndex,
    ) -> Result<(), ReductError> {
        let mut inconsistent_data = false;
        for block_id in block_index.tree().iter() {
            let desc_path = path.join(format!("{}{}", block_id, DESCRIPTOR_FILE_EXT));
            if file_list.contains(&desc_path) {
                let data_path = path.join(format!("{}{}", block_id, DATA_FILE_EXT));
                if !file_list.contains(&data_path) {
                    warn!(
                        "Data block {:?} not found. Removing its descriptor",
                        data_path
                    );
                    FILE_CACHE.remove(&desc_path).await?;
                    inconsistent_data = true;
                }
            } else {
                warn!("Block descriptor {:?} not found", desc_path);
                inconsistent_data = true;
            }
        }

        if inconsistent_data {
            Err(internal_server_error!(""))
        } else {
            Ok(())
        }
    }

    async fn restore_uncommitted_changes(
        entry_path: PathBuf,
        entry: &mut Entry,
    ) -> Result<(), ReductError> {
        let wal = create_wal(entry_path.clone()).await;
        let wal_blocks = wal.list().await?;
        if !wal_blocks.is_empty() {
            warn!(
                "Recovering uncommitted changes from WALs for entry: {:?}",
                entry_path
            );

            let mut block_manager = entry.block_manager.write().await?;
            for block_id in wal_blocks {
                let wal_entries = match wal.read(block_id).await {
                    Ok(entries) => entries,
                    Err(err) => {
                        error!("Failed to read WAL for block {}: {}", block_id, err);
                        wal.remove(block_id).await?;
                        continue;
                    }
                };

                let block_ref = if block_manager.exist(block_id).await? {
                    debug!(
                        "Loading block {}/{} from block manager",
                        entry.name, block_id
                    );
                    match block_manager.load_block(block_id).await {
                        Ok(block_ref) => block_ref,
                        Err(err) => {
                            warn!("Failed to load block {}/{}: {}", entry.name, block_id, err);
                            info!("Creating block {}/{} from WAL", entry.name, block_id);
                            Arc::new(AsyncRwLock::new(
                                crate::storage::block_manager::block::Block::new(block_id),
                            ))
                        }
                    }
                } else {
                    debug!("Creating block {}/{} from WAL", entry.name, block_id);
                    Arc::new(AsyncRwLock::new(
                        crate::storage::block_manager::block::Block::new(block_id),
                    ))
                };

                let mut block_removed = false;
                {
                    let mut block = block_ref.write().await?;
                    for wal_entry in wal_entries {
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
                            WalEntry::RemoveRecord(timestamp) => {
                                trace!(
                                    "Remove record from block {}/{}: {}",
                                    entry.name,
                                    block_id,
                                    timestamp
                                );
                                block.remove_record(timestamp);
                            }
                        }
                    }
                }

                if block_removed {
                    block_manager.remove_block(block_id).await?;
                } else {
                    block_manager.save_block(block_ref.clone()).await?;
                    block_manager.finish_block(block_ref).await?;
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
    use crate::storage::entry::tests::{entry, entry_settings, path, write_stub_record};
    use crate::storage::proto::{record, us_to_ts, BlockIndex as BlockIndexProto, Record};
    use std::fs;
    use std::io::SeekFrom;

    use super::*;
    use crate::backend::Backend;
    use crate::core::file_cache::FILE_CACHE;
    use reduct_base::io::ReadRecord;
    use rstest::{fixture, rstest};

    #[rstest]
    #[tokio::test]
    async fn test_restore(entry_settings: EntrySettings, path: PathBuf) {
        let entry = entry(entry_settings.clone(), path.clone());
        write_stub_record(&entry, 1).await;
        write_stub_record(&entry, 2000010).await;

        let mut bm = entry.block_manager.write().await.unwrap();
        let records = bm
            .load_block(1)
            .await
            .unwrap()
            .read()
            .await
            .unwrap()
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
        let entry = EntryLoader::restore_entry(
            path.join(entry.name()),
            entry_settings,
            Cfg::default().into(),
        )
        .await
        .unwrap()
        .unwrap();
        let info = entry.info().await.unwrap();
        assert_eq!(entry.name, "entry");
        assert_eq!(info.record_count, 2);
        assert_eq!(info.size, 88);

        let mut rec = entry.begin_read(1).await.unwrap();
        assert_eq!(rec.meta().timestamp(), 1);
        assert_eq!(rec.meta().content_length(), 10);
        assert_eq!(rec.meta().content_type(), "text/plain");

        assert_eq!(
            rec.read_chunk().unwrap().unwrap(),
            Bytes::from_static(b"0123456789")
        );

        let mut rec = entry.begin_read(2000010).await.unwrap();
        assert_eq!(rec.meta().timestamp(), 2000010);
        assert_eq!(rec.meta().content_length(), 10);
        assert_eq!(rec.meta().content_type(), "text/plain");

        assert_eq!(
            rec.read_chunk().unwrap().unwrap(),
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

        let entry =
            EntryLoader::restore_entry(path.join("entry"), entry_settings, Cfg::default().into())
                .await
                .unwrap()
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
        FILE_CACHE
            .set_storage_backend(
                Backend::builder()
                    .local_data_path(path.clone())
                    .try_build()
                    .await
                    .unwrap(),
            )
            .await;

        let path = path.join("entry");
        FILE_CACHE.create_dir_all(&path).await.unwrap();

        let mut block_manager = BlockManager::build(
            path.clone(),
            BlockIndex::new(path.clone().join(BLOCK_INDEX_FILE)),
            Cfg::default().into(),
        )
        .await;
        {
            let block_v1_8_ref = block_manager.start_new_block(1, 100).await.unwrap();
            let mut block_v1_8 = block_v1_8_ref.write().await.unwrap();
            block_v1_8.insert_or_update_record(Record {
                timestamp: Some(us_to_ts(&1)),
                begin: 0,
                end: 10,
                content_type: "text/plain".to_string(),
                state: record::State::Finished as i32,
                labels: vec![],
            });
            block_v1_8.insert_or_update_record(Record {
                timestamp: Some(us_to_ts(&2000010)),
                begin: 10,
                end: 20,
                content_type: "text/plain".to_string(),
                state: record::State::Finished as i32,
                labels: vec![],
            });
        }

        let mut block_proto: Block = block_manager
            .load_block(1)
            .await
            .unwrap()
            .read()
            .await
            .unwrap()
            .clone()
            .into();
        block_proto.record_count = 0;

        let mut lock = FILE_CACHE
            .write_or_create(&path.join("1.meta"), SeekFrom::Start(0))
            .await
            .unwrap();

        lock.write_all(&block_proto.encode_to_vec()).unwrap();

        // repack the block
        let entry = EntryLoader::restore_entry(path.clone(), entry_settings, Cfg::default().into())
            .await
            .unwrap()
            .unwrap();
        let info = entry.info().await.unwrap();

        assert_eq!(info.size, 88);
        assert_eq!(info.record_count, 2);
        assert_eq!(info.block_count, 1);
        assert_eq!(info.oldest_record, 1);
        assert_eq!(info.latest_record, 2000010);

        let block_index = BlockIndex::try_load(path.join(BLOCK_INDEX_FILE))
            .await
            .unwrap();
        let mut block_manager =
            BlockManager::build(path.clone(), block_index, Cfg::default().into()).await;
        let block_v1_9 = block_manager
            .load_block(1)
            .await
            .unwrap()
            .read()
            .await
            .unwrap()
            .clone();
        assert_eq!(block_v1_9.record_count(), 2);
        assert_eq!(block_v1_9.size(), 20);
        assert_eq!(block_v1_9.metadata_size(), 68);
    }

    #[rstest]
    #[tokio::test]
    async fn test_empty_block_index(path: PathBuf, entry_settings: EntrySettings) {
        let entry = entry(entry_settings.clone(), path.clone());
        write_stub_record(&entry, 1).await;
        write_stub_record(&entry, 2000010).await;
        entry.compact().await.unwrap(); // sync WALs

        {
            let block_file_index = path.join(&entry.name).join(BLOCK_INDEX_FILE);
            let mut file = FILE_CACHE
                .write_or_create(&block_file_index, SeekFrom::Current(0))
                .await
                .unwrap();
            file.set_len(0).unwrap();
            file.sync_all().await.unwrap();
        }

        let entry = EntryLoader::restore_entry(
            path.join(entry.name()),
            entry_settings,
            Cfg::default().into(),
        )
        .await
        .unwrap()
        .unwrap();
        let info = entry.info().await.unwrap();
        assert_eq!(info.record_count, 2);
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_block_index(path: PathBuf, entry_settings: EntrySettings) {
        let entry = entry(entry_settings.clone(), path.clone());
        write_stub_record(&entry, 1).await;
        write_stub_record(&entry, 2000010).await;
        entry
            .block_manager
            .write()
            .await
            .unwrap()
            .save_cache_on_disk()
            .await
            .unwrap();

        EntryLoader::restore_entry(
            path.join(entry.name()),
            entry_settings,
            Cfg::default().into(),
        )
        .await
        .unwrap()
        .unwrap();

        let block_index_path = path.join("entry").join(BLOCK_INDEX_FILE);
        assert_eq!(block_index_path.exists(), true, "should create block index");
        let block_index =
            BlockIndexProto::decode(Bytes::from(fs::read(block_index_path).unwrap())).unwrap();

        assert_eq!(block_index.blocks.len(), 1);
        assert_eq!(block_index.crc64, 4579043244124502122);
        assert_eq!(block_index.blocks[0].block_id, 1);
        assert_eq!(block_index.blocks[0].size, 20);
        assert_eq!(block_index.blocks[0].record_count, 2);
    }

    #[rstest]
    #[tokio::test]
    async fn test_check_integrity_block_index(path: PathBuf, entry_settings: EntrySettings) {
        let entry = entry(entry_settings.clone(), path.clone());
        write_stub_record(&entry, 1).await;
        write_stub_record(&entry, 2000010).await;
        let _ = entry
            .block_manager
            .write()
            .await
            .unwrap()
            .save_cache_on_disk()
            .await;

        EntryLoader::restore_entry(
            path.join(entry.name.clone()),
            entry_settings.clone(),
            Cfg::default().into(),
        )
        .await
        .unwrap()
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

        EntryLoader::restore_entry(
            path.join(entry.name()),
            entry_settings,
            Cfg::default().into(),
        )
        .await
        .unwrap();

        let buf = fs::read(block_index_path).unwrap();
        let block_index = BlockIndexProto::decode(Bytes::from(buf)).unwrap();
        assert_eq!(
            block_index.blocks[0].size, 20,
            "should restore the block index from the blocks"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_missed_descriptor(path: PathBuf, entry_settings: EntrySettings) {
        let entry = entry(entry_settings.clone(), path.clone());
        write_stub_record(&entry, 1).await;
        let _ = entry
            .block_manager
            .write()
            .await
            .unwrap()
            .save_cache_on_disk()
            .await;

        let entry = EntryLoader::restore_entry(
            path.join(entry.name.clone()),
            entry_settings.clone(),
            Cfg::default().into(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            entry
                .block_manager
                .write()
                .await
                .unwrap()
                .load_block(1)
                .await
                .is_ok(),
            "should restore the block index from the blocks"
        );

        fs::remove_file(path.join("entry/1.meta")).unwrap();
        fs::remove_file(path.join("entry/1.blk")).unwrap();

        EntryLoader::restore_entry(
            path.join(entry.name()),
            entry_settings,
            Cfg::default().into(),
        )
        .await
        .unwrap();

        let block_index_path = path.join("entry").join(BLOCK_INDEX_FILE);
        let buf = fs::read(block_index_path).unwrap();
        let block_index = BlockIndexProto::decode(Bytes::from(buf)).unwrap();
        assert!(
            block_index.blocks.is_empty(),
            "should restore the block index from the blocks"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_recovery_with_orphan_block(path: PathBuf, entry_settings: EntrySettings) {
        let entry = entry(entry_settings.clone(), path.clone());
        write_stub_record(&entry, 1).await;
        entry.compact().await.unwrap();

        // Create a new block but don't add it to the index
        let mut bm = entry.block_manager.write().await.unwrap();
        bm.start_new_block(2, 100).await.unwrap();
        bm.save_cache_on_disk().await.unwrap();
        bm.index_mut().remove_block(2);
        bm.index_mut().save().await.unwrap();

        // Restore the entry
        let entry = EntryLoader::restore_entry(
            path.join(entry.name.clone()),
            entry.settings().await.unwrap(),
            Cfg::default().into(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            entry
                .block_manager
                .read()
                .await
                .unwrap()
                .index()
                .tree()
                .len(),
            2,
            "should rebuild index and add the block"
        );
    }

    mod wal_recovery {
        use crate::storage::proto::Record;
        use reduct_base::error::ErrorCode::InternalServerError;
        use std::fs;
        use std::fs::File;

        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_new_block(#[future] entry_fix: (Arc<Entry>, PathBuf), record2: Record) {
            let (entry, path) = entry_fix.await;
            let mut wal = create_wal(path.clone()).await;
            // Block #3 was created
            wal.append(3, WalEntry::WriteRecord(record2.clone()))
                .await
                .unwrap();

            let mut record3 = record2.clone();
            record3.timestamp = Some(us_to_ts(&3));
            wal.append(3, WalEntry::WriteRecord(record3.clone()))
                .await
                .unwrap();

            let entry = EntryLoader::restore_entry(
                path.clone(),
                entry.settings().await.unwrap(),
                Cfg::default().into(),
            )
            .await
            .unwrap()
            .unwrap();

            let block_ref = entry
                .block_manager
                .write()
                .await
                .unwrap()
                .load_block(3)
                .await
                .unwrap()
                .clone();
            let block = block_ref.read().await.unwrap();
            assert_eq!(block.get_record(2), Some(&record2));
            assert_eq!(block.get_record(3), Some(&record3));

            let file = File::open(path.join("3.blk")).unwrap();
            assert_eq!(
                file.metadata().unwrap().len(),
                block.size(),
                "should save and truncate the block"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_update_block(
            #[future] entry_fix: (Arc<Entry>, PathBuf),
            mut record2: Record,
        ) {
            let (entry, path) = entry_fix.await;
            let mut wal = create_wal(path.clone()).await;

            // Block #1 was updated
            wal.append(1, WalEntry::WriteRecord(record2.clone()))
                .await
                .unwrap();
            record2.end = 20; //size 20
            wal.append(1, WalEntry::UpdateRecord(record2.clone()))
                .await
                .unwrap();

            let entry = EntryLoader::restore_entry(
                path.clone(),
                entry.settings().await.unwrap(),
                Cfg::default().into(),
            )
            .await
            .unwrap()
            .unwrap();

            let block_ref = entry
                .block_manager
                .write()
                .await
                .unwrap()
                .load_block(1)
                .await
                .unwrap();

            let block = block_ref.read().await.unwrap();
            assert_eq!(block.get_record(2), Some(&record2));

            let file = File::open(path.join("1.blk")).unwrap();
            assert_eq!(
                file.metadata().unwrap().len(),
                block.size(),
                "should save and truncate the block"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_remove_record(#[future] entry_fix: (Arc<Entry>, PathBuf)) {
            let (entry, path) = entry_fix.await;
            let mut wal = create_wal(path.clone()).await;

            // Record #1 was removed
            wal.append(1, WalEntry::RemoveRecord(0)).await.unwrap();

            let entry = EntryLoader::restore_entry(
                path,
                entry.settings().await.unwrap(),
                Cfg::default().into(),
            )
            .await
            .unwrap()
            .unwrap();

            let block = entry
                .block_manager
                .write()
                .await
                .unwrap()
                .load_block(1)
                .await
                .unwrap();
            let block = block.read().await.unwrap();
            assert_eq!(block.record_count(), 1);
            assert!(block.get_record(0).is_none());
            assert!(block.get_record(1).is_some());
        }

        #[rstest]
        #[tokio::test]
        async fn test_remove_block(#[future] entry_fix: (Arc<Entry>, PathBuf)) {
            let (entry, path) = entry_fix.await;
            let mut wal = create_wal(path.clone()).await;

            // Block #1 was removed
            wal.append(1, WalEntry::RemoveBlock).await.unwrap();
            let entry = EntryLoader::restore_entry(
                path,
                entry.settings().await.unwrap(),
                Cfg::default().into(),
            )
            .await
            .unwrap()
            .unwrap();

            let block = entry
                .block_manager
                .write()
                .await
                .unwrap()
                .load_block(1)
                .await
                .clone();
            assert_eq!(block.err().unwrap().status, InternalServerError,);
        }

        #[rstest]
        #[tokio::test]
        async fn test_corrupted_wal(#[future] entry_fix: (Arc<Entry>, PathBuf)) {
            let (entry, path) = entry_fix.await;

            fs::write(path.join("wal/1.wal"), b"bad data").unwrap();
            let entry = EntryLoader::restore_entry(
                path.clone(),
                entry.settings().await.unwrap(),
                Cfg::default().into(),
            )
            .await;
            assert!(entry.is_ok());
            assert!(
                !path.join("wal/1.wal").exists(),
                "should remove corrupted wal"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_recovery_without_index(#[future] entry_fix: (Arc<Entry>, PathBuf)) {
            let (entry, path) = entry_fix.await;
            let mut wal = create_wal(path.clone()).await;

            // Block #1 was appended to the WAL
            wal.append(
                1,
                WalEntry::WriteRecord(Record {
                    timestamp: Some(us_to_ts(&1)),
                    begin: 0,
                    end: 10,
                    content_type: "text/plain".to_string(),
                    state: record::State::Finished as i32,
                    labels: vec![],
                }),
            )
            .await
            .unwrap();

            // Create a new block but don't add it to the index
            let mut bm = entry.block_manager.write().await.unwrap();
            bm.start_new_block(1, 100).await.unwrap();
            bm.index_mut().remove_block(1);
            bm.index_mut().save().await.unwrap();

            // Restore the entry
            let entry = EntryLoader::restore_entry(
                path.clone(),
                entry.settings().await.unwrap(),
                Cfg::default().into(),
            )
            .await
            .unwrap()
            .unwrap();
            let block = entry
                .block_manager
                .write()
                .await
                .unwrap()
                .load_block(1)
                .await
                .unwrap();
            let block = block.read().await.unwrap();

            assert_eq!(block.record_count(), 1);
        }

        #[fixture]
        fn record2() -> Record {
            Record {
                timestamp: Some(us_to_ts(&2)),
                begin: 0,
                end: 10,
                content_type: "text/plain".to_string(),
                state: record::State::Finished as i32,
                labels: vec![],
            }
        }

        #[fixture]
        async fn entry_fix(path: PathBuf, entry_settings: EntrySettings) -> (Arc<Entry>, PathBuf) {
            let entry = entry(entry_settings.clone(), path.clone());
            let name = entry.name().to_string();
            {
                let mut block_manager = entry.block_manager.write().await.unwrap();

                {
                    let block_ref = block_manager.start_new_block(1, 10).await.unwrap();
                    let mut block = block_ref.write().await.unwrap();
                    block.insert_or_update_record(Record {
                        timestamp: Some(us_to_ts(&0)),
                        begin: 0,
                        end: 10,
                        content_type: "text/plain".to_string(),
                        state: record::State::Finished as i32,
                        labels: vec![],
                    });

                    block.insert_or_update_record(Record {
                        timestamp: Some(us_to_ts(&1)),
                        begin: 0,
                        end: 10,
                        content_type: "text/plain".to_string(),
                        state: record::State::Finished as i32,
                        labels: vec![],
                    });
                }

                block_manager.start_new_block(2, 10).await.unwrap();
                block_manager.save_cache_on_disk().await.unwrap();
            }

            (entry, path.join(name))
        }
    }
}
