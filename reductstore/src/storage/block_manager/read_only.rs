// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::cfg::InstanceRole;
use crate::core::file_cache::FILE_CACHE;
use crate::storage::block_manager::block_index::BlockIndex;
use crate::storage::block_manager::{
    BlockManager, BLOCK_INDEX_FILE, DATA_FILE_EXT, DESCRIPTOR_FILE_EXT,
};
use crate::storage::proto::block_index::Block as BlockEntry;
use reduct_base::error::ReductError;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

pub(in crate::storage) struct ReplicaIndexReload {
    entry_path: PathBuf,
    index_path: PathBuf,
    previous_state: HashMap<u64, BlockEntry>,
}

impl ReplicaIndexReload {
    pub(in crate::storage) async fn load_updated_index(&self) -> Result<BlockIndex, ReductError> {
        FILE_CACHE
            .invalidate_local_cache_file(&self.index_path)
            .await?;

        let updated_index = BlockIndex::try_load(self.index_path.clone()).await?;

        for (block_id, new_block_info) in updated_index.info().iter() {
            if let Some(previous_block_info) = self.previous_state.get(block_id) {
                if previous_block_info.crc64 != new_block_info.crc64 {
                    FILE_CACHE
                        .invalidate_local_cache_file(
                            &self
                                .entry_path
                                .join(format!("{}{}", block_id, DESCRIPTOR_FILE_EXT)),
                        )
                        .await?;
                    FILE_CACHE
                        .invalidate_local_cache_file(
                            &self
                                .entry_path
                                .join(format!("{}{}", block_id, DATA_FILE_EXT)),
                        )
                        .await?;
                }
            }
        }

        Ok(updated_index)
    }
}

impl BlockManager {
    #[cfg(test)]
    pub(super) async fn reload_if_readonly(&mut self) -> Result<(), ReductError> {
        // Replica index refresh is driven by the launcher background task.
        Ok(())
    }

    pub(in crate::storage) fn prepare_replica_index_reload(&self) -> Option<ReplicaIndexReload> {
        if self.cfg.role != InstanceRole::Replica {
            return None;
        }

        Some(ReplicaIndexReload {
            entry_path: self.path.clone(),
            index_path: self.path.join(BLOCK_INDEX_FILE),
            previous_state: self.block_index.info().clone(),
        })
    }

    pub(in crate::storage) fn apply_replica_index_reload(&mut self, updated_index: BlockIndex) {
        self.block_index = updated_index;
        self.block_cache.clear();
        self.last_replica_sync = Instant::now();
    }
}

#[cfg(test)]
mod tests {
    // test reloading in read-only mode

    use super::*;
    use crate::cfg::storage_engine::StorageEngineConfig;
    use crate::cfg::Cfg;
    use crate::storage::block_manager::block::Block;
    use crate::storage::block_manager::block_index::BlockIndex;
    use crate::storage::block_manager::{BlockManager, BLOCK_INDEX_FILE};
    use crate::storage::proto::Block as BlockProto;
    use prost::Message;
    use reduct_base::error::ErrorCode;
    use rstest::{fixture, rstest};
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::tempdir;

    #[rstest]
    #[tokio::test]
    async fn test_reload_if_readonly_is_noop(#[future] path: PathBuf) {
        let path = path.await;
        let cfg = Cfg {
            role: InstanceRole::Replica,
            data_path: path.clone(),
            engine_config: StorageEngineConfig {
                replica_update_interval: Duration::from_millis(100),
                ..Default::default()
            },
            ..Default::default()
        };

        let index = BlockIndex::new(path.join(BLOCK_INDEX_FILE));
        index.save().await.unwrap();
        let mut block_manager = BlockManager::build(
            path.clone(),
            index,
            "bucket".to_string(),
            "entry".to_string(),
            Arc::new(cfg.clone()),
        )
        .await
        .unwrap();

        // change index on disc
        let mut new_index = BlockIndex::try_load(path.join(BLOCK_INDEX_FILE))
            .await
            .unwrap();

        let block = Block::new(1);
        new_index.insert_or_update(block);
        new_index.save().await.unwrap();

        // wait for the replica update interval to pass
        tokio::time::sleep(Duration::from_millis(150)).await;
        block_manager.reload_if_readonly().await.unwrap();

        assert!(block_manager.block_index.info().get(&1).is_none());
    }

    #[rstest]
    #[tokio::test]
    async fn test_background_replica_index_reload(#[future] path: PathBuf) {
        let path = path.await;
        let cfg = Cfg {
            role: InstanceRole::Replica,
            data_path: path.clone(),
            engine_config: StorageEngineConfig {
                replica_update_interval: Duration::from_millis(100),
                ..Default::default()
            },
            ..Default::default()
        };

        let index = BlockIndex::new(path.join(BLOCK_INDEX_FILE));
        index.save().await.unwrap();
        let mut block_manager = BlockManager::build(
            path.clone(),
            index,
            "bucket".to_string(),
            "entry".to_string(),
            Arc::new(cfg.clone()),
        )
        .await
        .unwrap();

        let mut new_index = BlockIndex::try_load(path.join(BLOCK_INDEX_FILE))
            .await
            .unwrap();
        new_index.insert_or_update(Block::new(1));
        new_index.save().await.unwrap();

        let reload = block_manager.prepare_replica_index_reload().unwrap();
        let updated_index = reload.load_updated_index().await.unwrap();
        block_manager.apply_replica_index_reload(updated_index);

        assert!(block_manager.block_index.info().get(&1).is_some());
    }

    #[rstest]
    #[tokio::test(flavor = "current_thread")]
    async fn test_reload_if_readonly_discards_cache_on_crc_change(#[future] path: PathBuf) {
        let path = path.await;
        let entry_path = path.join("bucket").join("entry");

        let cfg = Cfg {
            role: InstanceRole::Replica,
            data_path: path.clone(),
            engine_config: StorageEngineConfig {
                replica_update_interval: Duration::from_millis(50),
                ..Default::default()
            },
            ..Default::default()
        };

        let index_path = entry_path.join(BLOCK_INDEX_FILE);
        let mut index = BlockIndex::new(index_path.clone());
        index.insert_or_update_with_crc(Block::new(1), 1);
        index.save().await.unwrap();

        let mut block_manager = BlockManager::build(
            entry_path.clone(),
            index,
            "bucket".to_string(),
            "entry".to_string(),
            Arc::new(cfg.clone()),
        )
        .await
        .unwrap();

        let mut updated_index = BlockIndex::new(index_path.clone());
        updated_index.insert_or_update_with_crc(Block::new(1), 2);
        updated_index.save().await.unwrap();

        let reload = block_manager.prepare_replica_index_reload().unwrap();
        let updated_index = reload.load_updated_index().await.unwrap();
        block_manager.apply_replica_index_reload(updated_index);

        assert_eq!(
            block_manager.block_index.info().get(&1).unwrap().crc64,
            Some(2)
        );
    }

    #[rstest]
    #[tokio::test(flavor = "current_thread")]
    async fn test_load_block_missing_descriptor_on_replica_returns_too_early(
        #[future] path: PathBuf,
    ) {
        let path = path.await;
        let entry_path = path.join("bucket").join("entry");

        let cfg = Cfg {
            role: InstanceRole::Replica,
            data_path: path.clone(),
            ..Default::default()
        };

        let index_path = entry_path.join(BLOCK_INDEX_FILE);
        let mut index = BlockIndex::new(index_path);
        index.insert_or_update(Block::new(1));
        index.save().await.unwrap();

        let mut block_manager = BlockManager::build(
            entry_path,
            index,
            "bucket".to_string(),
            "entry".to_string(),
            Arc::new(cfg),
        )
        .await
        .unwrap();

        let err = block_manager.load_block(1).await.err().unwrap();
        assert_eq!(err.status(), ErrorCode::TooEarly);
        assert!(block_manager.index().get_block(1).is_none());
    }

    #[rstest]
    #[tokio::test(flavor = "current_thread")]
    async fn test_load_block_crc_mismatch_on_replica_returns_too_early(#[future] path: PathBuf) {
        let path = path.await;
        let entry_path = path.join("bucket").join("entry");

        let cfg = Cfg {
            role: InstanceRole::Replica,
            data_path: path.clone(),
            ..Default::default()
        };

        let index_path = entry_path.join(BLOCK_INDEX_FILE);
        let mut index = BlockIndex::new(index_path);
        index.insert_or_update_with_crc(Block::new(1), 1);
        index.save().await.unwrap();

        let descriptor = BlockProto::from(Block::new(1)).encode_to_vec();
        std::fs::write(entry_path.join("1.meta"), descriptor).unwrap();

        let mut block_manager = BlockManager::build(
            entry_path,
            index,
            "bucket".to_string(),
            "entry".to_string(),
            Arc::new(cfg),
        )
        .await
        .unwrap();

        let err = block_manager.load_block(1).await.err().unwrap();
        assert_eq!(err.status(), ErrorCode::TooEarly);
        assert!(block_manager.index().get_block(1).is_none());
    }

    #[fixture]
    async fn path() -> PathBuf {
        let dir = tempdir().unwrap().keep();
        tokio::fs::create_dir_all(dir.join("bucket").join("entry"))
            .await
            .unwrap();

        dir
    }
}
