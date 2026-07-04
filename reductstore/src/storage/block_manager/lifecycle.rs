// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use super::*;

impl BlockManager {
    pub async fn start_new_block(
        &mut self,
        block_id: u64,
        max_block_size: u64,
    ) -> Result<BlockRef, ReductError> {
        let block = Block::new(block_id);

        // create a block with data
        {
            let mut file = FILE_CACHE
                .write_or_create(&self.path_to_data(block_id), SeekFrom::Start(0))
                .await?;

            if self.cfg.backend_config.backend_type == BackendType::Filesystem {
                // Pre-allocation for remote storage is inefficient
                // because we synchronize full size even if block is empty
                file.set_len(max_block_size)?;
            }
        }

        self.block_index.insert_or_update(block.clone());

        let block_ref = Arc::new(AsyncRwLock::new(block));
        self.save_block(block_ref.clone()).await?;
        Ok(block_ref)
    }

    /// Finish writing a record to a block.
    ///
    /// This method will shrink the block data file to the record size and sync the descriptor and data files.
    ///
    /// # Arguments
    ///
    /// * `block` - Block to finish writing to.
    ///
    /// # Errors
    ///
    /// * `ReductError` - If file system operation failed.
    pub async fn finish_block(&mut self, block: BlockRef) -> Result<(), ReductError> {
        let (block_id, block_size) = {
            let block = block.read().await?;
            (block.block_id(), block.size())
        };

        let data_path = self.path_to_data(block_id);
        let desc_path = self.path_to_desc(block_id);
        let index_path = self.path.join(BLOCK_INDEX_FILE);

        {
            // resize imminently for better testing.
            let mut data_block = FILE_CACHE
                .write_or_create(&data_path, SeekFrom::Current(0))
                .await?;
            data_block.set_len(block_size)?;
        }

        self.save_meta_on_disk(block.clone()).await?;

        let sync_block = async move {
            /* sync descriptor and data */
            {
                let mut data_block = FILE_CACHE
                    .write_or_create(&data_path, SeekFrom::Current(0))
                    .await?;
                data_block.sync_all().await?;
            }

            {
                let mut descr_block = FILE_CACHE
                    .write_or_create(&desc_path, SeekFrom::Current(0))
                    .await?;
                descr_block.sync_all().await?;
            }

            {
                let mut descr_block = FILE_CACHE
                    .write_or_create(&desc_path, SeekFrom::Current(0))
                    .await?;
                descr_block.sync_all().await?;
            }

            {
                let mut index_file = FILE_CACHE
                    .write_or_create(&index_path, SeekFrom::Current(0))
                    .await?;
                index_file.sync_all().await?;
            }

            Ok::<(), ReductError>(())
        };

        tokio::spawn(async move {
            // spawn to avoid blocking entry
            if let Err(err) = sync_block.await {
                error!("{}", err)
            }
        });

        Ok(())
    }

    /// Remove a block from file system.
    ///
    /// This method will sync the block descriptor and data files, remove the block from the cache and index.
    ///
    /// # Arguments
    ///
    /// * `block_id` - ID of the block to remove.
    ///
    /// # Errors
    ///
    /// * `ReductError` - If the block is still in use or file system operation failed.
    pub async fn remove_block(&mut self, block_id: u64) -> Result<(), ReductError> {
        self.wal.append(block_id, WalEntry::RemoveBlock).await?;

        let data_block_path = self.path_to_data(block_id);
        if FILE_CACHE.try_exists(&data_block_path).await? {
            // it can be still in WAL only
            FILE_CACHE.remove(&data_block_path).await?;
        }

        let desc_block_path = self.path_to_desc(block_id);
        if FILE_CACHE.try_exists(&desc_block_path).await? {
            // it can be still in WAL only
            FILE_CACHE.remove(&desc_block_path).await?;
        }

        self.block_index.remove_block(block_id);
        self.block_index.save().await?;

        self.block_cache.remove(&block_id);

        self.wal.remove(block_id).await?;
        Ok(())
    }

    pub async fn mark_block_corrupted(&mut self, block_id: u64) -> Result<(), ReductError> {
        if self.cfg.role == InstanceRole::Replica {
            return Ok(());
        }

        self.block_index.mark_corrupted(block_id);
        self.block_cache.remove(&block_id);

        let path = self.path_to_desc(block_id);
        let descriptor = if let Ok(mut file) = FILE_CACHE.read(&path, SeekFrom::Start(0)).await {
            let mut buf = Vec::new();
            if file.read_to_end(&mut buf).is_ok() {
                BlockProto::decode(Bytes::from(buf)).ok()
            } else {
                None
            }
        } else {
            None
        };

        if let Some(mut proto) = descriptor {
            proto.corrupted = Some(true);
            let new_buf = proto.encode_to_vec();
            if let Ok(mut writer) = FILE_CACHE.write_or_create(&path, SeekFrom::Start(0)).await {
                let _ = writer.set_len(new_buf.len() as u64);
                let _ = writer.write_all(&new_buf);
                let _ = writer.flush_local().await;
            }
        }

        self.block_index.save().await?;

        error!(
            "Marked block {}/{}/{} as corrupted",
            self.bucket, self.entry, block_id
        );
        Ok(())
    }

    #[cfg(test)]
    pub fn is_block_corrupted(&self, block_id: u64) -> bool {
        self.block_index.is_corrupted(block_id)
    }

    /// Check if a block exists on disk.
    pub async fn exist(&self, block_id: u64) -> Result<bool, ReductError> {
        let path = self.path_to_desc(block_id);
        Ok(FILE_CACHE.try_exists(&path).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::block_manager::block::Block;
    use crate::storage::block_manager::block_index::BlockIndex;
    use crate::storage::block_manager::test_utils::{block_id, block_manager};
    use crate::storage::block_manager::{
        BlockManager, BLOCK_INDEX_FILE, DATA_FILE_EXT, DESCRIPTOR_FILE_EXT,
    };
    use crate::storage::proto::Block as BlockProto;
    use prost::bytes::Bytes;
    use prost::Message;
    use rstest::rstest;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::tempdir;

    #[rstest]
    #[tokio::test]
    async fn test_starting_block(#[future] block_manager: BlockManager) {
        let mut block_manager = block_manager.await;
        let block_id = 1_000_005;

        let block_ref = block_manager.start_new_block(block_id, 1024).await.unwrap();
        assert_eq!(block_ref.read().await.unwrap().block_id(), block_id,);

        // Create an empty block
        block_manager.save_cache_on_disk().await.unwrap();
        let file = std::fs::File::open(
            block_manager
                .path
                .join(format!("{}{}", block_id, DATA_FILE_EXT)),
        )
        .unwrap();
        assert_eq!(file.metadata().unwrap().len(), 1024);

        // Create a block descriptor
        let buf = std::fs::read(
            block_manager
                .path
                .join(format!("{}{}", block_id, DESCRIPTOR_FILE_EXT)),
        )
        .unwrap();

        let block_from_file: Block = BlockProto::decode(Bytes::from(buf)).unwrap().into();
        assert_eq!(block_from_file, block_ref.read().await.unwrap().to_owned());
    }

    #[rstest]
    #[tokio::test]
    async fn test_starting_block_no_preallocation_for_remote_backend() {
        let path = tempdir().unwrap().keep().join("bucket").join("entry");
        let mut cfg = Cfg::default();
        cfg.backend_config.backend_type = BackendType::Remote;

        let mut block_manager = BlockManager::build(
            path.clone(),
            BlockIndex::new(path.join(BLOCK_INDEX_FILE)),
            "bucket".to_string(),
            "entry".to_string(),
            Arc::new(cfg),
            Default::default(),
        )
        .await
        .unwrap();

        let block_id = 2_000_005;
        block_manager.start_new_block(block_id, 1024).await.unwrap();
        block_manager.save_cache_on_disk().await.unwrap();

        let file = std::fs::File::open(
            block_manager
                .path
                .join(format!("{}{}", block_id, DATA_FILE_EXT)),
        )
        .unwrap();
        assert_eq!(file.metadata().unwrap().len(), 0);
    }

    #[rstest]
    #[tokio::test]
    async fn test_mark_block_corrupted_removes_cache(
        #[future] block_manager: BlockManager,
        block_id: u64,
    ) {
        let mut block_manager = block_manager.await;
        block_manager.load_block(block_id).await.unwrap();
        assert!(block_manager.block_cache.get_read(&block_id).is_some());

        block_manager.mark_block_corrupted(block_id).await.unwrap();

        assert!(block_manager.is_block_corrupted(block_id));
        assert!(block_manager.block_cache.get_read(&block_id).is_none());
    }

    #[rstest]
    #[tokio::test]
    async fn test_mark_block_corrupted_persists_in_descriptor(
        #[future] block_manager: BlockManager,
        block_id: u64,
    ) {
        let mut block_manager = block_manager.await;

        block_manager.mark_block_corrupted(block_id).await.unwrap();

        let block_proto = BlockProto::decode(
            std::fs::read(block_manager.path_to_desc(block_id))
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        assert_eq!(block_proto.corrupted, Some(true));
    }

    #[rstest]
    #[tokio::test]
    async fn test_finish_block(#[future] block_manager: BlockManager, block_id: u64) {
        let mut block_manager = block_manager.await;
        let block = block_manager
            .start_new_block(block_id + 1, 1024)
            .await
            .unwrap();
        let block_id = block.read().await.unwrap().block_id();
        let loaded_block = block_manager.load_block(block_id).await.unwrap();
        assert_eq!(loaded_block.read().await.unwrap().block_id(), block_id);

        block_manager.finish_block(loaded_block).await.unwrap();

        let path = block_manager
            .path
            .join(format!("{}{}", block_id, DATA_FILE_EXT));
        for _ in 0..100 {
            if std::fs::metadata(&path).unwrap().len() == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert_eq!(std::fs::metadata(path).unwrap().len(), 0);
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_non_existing_block(#[future] block_manager: BlockManager) {
        let mut block_manager = block_manager.await;
        block_manager.remove_block(999999).await.expect("No error");
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_index_when_remove_block(
        #[future] block_manager: BlockManager,
        block_id: u64,
    ) {
        let mut bm = block_manager.await;
        bm.remove_block(block_id).await.unwrap();

        let index = BlockIndex::try_load(bm.path.join(BLOCK_INDEX_FILE))
            .await
            .unwrap();
        assert!(index.get_block(block_id).is_none(), "index updated");
    }
}
