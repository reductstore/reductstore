// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::cfg::InstanceRole;
use crate::core::file_cache::FILE_CACHE;
use crate::storage::block_manager::block_index::BlockIndex;
use crate::storage::block_manager::{
    all_block_file_paths, BlockManager, ReplicaPublication, BLOCK_INDEX_FILE,
};
use crate::storage::proto::block_index::Block as BlockEntry;
use reduct_base::error::ReductError;
use reduct_base::too_early;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

pub(in crate::storage) struct ReplicaIndexReload {
    entry_path: PathBuf,
    index_path: PathBuf,
    previous_state: HashMap<u64, BlockEntry>,
    base_publication: ReplicaPublication,
}

impl ReplicaIndexReload {
    pub(in crate::storage) async fn load_candidate(&self) -> Result<BlockIndex, ReductError> {
        FILE_CACHE
            .invalidate_local_cache_file(&self.index_path)
            .await?;

        BlockIndex::try_load(self.index_path.clone()).await
    }

    pub(in crate::storage) fn accepted_publication(&self) -> &ReplicaPublication {
        &self.base_publication
    }

    pub(in crate::storage) fn validate_successor(
        &self,
        candidate: &ReplicaPublication,
    ) -> Result<(), ReductError> {
        match (&self.base_publication, candidate) {
            (ReplicaPublication::Published(_), ReplicaPublication::Legacy) => Err(too_early!(
                "Entry publication disappeared after a published replica state was accepted"
            )),
            (ReplicaPublication::Published(current), ReplicaPublication::Published(next))
                if current.incarnation == next.incarnation
                    && next.generation < current.generation =>
            {
                Err(too_early!("Entry publication generation regressed"))
            }
            _ => Ok(()),
        }
    }
}

impl BlockManager {
    #[cfg(test)]
    pub(in crate::storage) fn has_mutation_batch(&self) -> bool {
        self.mutation_batch.is_some()
    }

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
            base_publication: self.accepted_publication.clone(),
        })
    }

    pub(in crate::storage) async fn apply_replica_index_reload(
        &mut self,
        reload: ReplicaIndexReload,
        updated_index: BlockIndex,
        publication: ReplicaPublication,
    ) -> Result<(), ReductError> {
        if self.accepted_publication != reload.base_publication {
            return Err(too_early!(
                "Replica index changed while candidate was loading"
            ));
        }

        reload.validate_successor(&publication)?;
        let invalidate_all = match (&reload.base_publication, &publication) {
            (ReplicaPublication::Legacy, ReplicaPublication::Published(_)) => true,
            (ReplicaPublication::Published(previous), ReplicaPublication::Published(next)) => {
                previous.incarnation != next.incarnation
            }
            _ => false,
        };
        let ids = affected_block_ids(&reload.previous_state, updated_index.info(), invalidate_all);
        let mut first_err = None;
        for block_id in ids {
            for path in all_block_file_paths(&reload.entry_path, block_id) {
                if let Err(err) = FILE_CACHE.invalidate_local_cache_file(&path).await {
                    if first_err.is_none() {
                        first_err = Some(err);
                    }
                }
            }
            self.decompress_cache.invalidate(&self.path, block_id).await;
        }
        if let Some(err) = first_err {
            return Err(err);
        }

        self.block_index = updated_index;
        self.accepted_publication = publication;
        self.block_cache.clear();
        self.last_replica_sync = Instant::now();
        Ok(())
    }

    pub(in crate::storage) async fn initialize_replica_publication(
        &mut self,
        publication: ReplicaPublication,
    ) -> Result<(), ReductError> {
        if matches!(publication, ReplicaPublication::Published(_)) {
            let ids = self.block_index.info().keys().copied().collect::<Vec<_>>();
            let mut first_err = None;
            for block_id in ids {
                for path in all_block_file_paths(&self.path, block_id) {
                    if let Err(err) = FILE_CACHE.invalidate_local_cache_file(&path).await {
                        if first_err.is_none() {
                            first_err = Some(err);
                        }
                    }
                }
                self.decompress_cache.invalidate(&self.path, block_id).await;
            }
            if let Some(err) = first_err {
                return Err(err);
            }
        }
        self.accepted_publication = publication;
        Ok(())
    }
}

fn affected_block_ids(
    previous: &HashMap<u64, BlockEntry>,
    candidate: &HashMap<u64, BlockEntry>,
    invalidate_all: bool,
) -> BTreeSet<u64> {
    previous
        .keys()
        .chain(candidate.keys())
        .filter(|id| invalidate_all || previous.get(id) != candidate.get(id))
        .copied()
        .collect()
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
            Default::default(),
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
            Default::default(),
        )
        .await
        .unwrap();

        let mut new_index = BlockIndex::try_load(path.join(BLOCK_INDEX_FILE))
            .await
            .unwrap();
        new_index.insert_or_update(Block::new(1));
        new_index.save().await.unwrap();

        let reload = block_manager.prepare_replica_index_reload().unwrap();
        let updated_index = reload.load_candidate().await.unwrap();
        block_manager
            .apply_replica_index_reload(reload, updated_index, ReplicaPublication::Legacy)
            .await
            .unwrap();

        assert!(block_manager.block_index.info().get(&1).is_some());
    }

    #[rstest]
    #[tokio::test]
    async fn test_replica_index_reload_accepts_published_transition(#[future] path: PathBuf) {
        let path = path.await;
        let cfg = Cfg {
            role: InstanceRole::Replica,
            data_path: path.clone(),
            ..Default::default()
        };
        let index_path = path.join(BLOCK_INDEX_FILE);
        let index = BlockIndex::new(index_path.clone());
        index.save().await.unwrap();
        let mut block_manager = BlockManager::build(
            path,
            index,
            "bucket".to_string(),
            "entry".to_string(),
            Arc::new(cfg),
            Default::default(),
        )
        .await
        .unwrap();

        let mut updated_index = BlockIndex::new(index_path);
        updated_index.insert_or_update(Block::new(1));
        updated_index.save().await.unwrap();

        let reload = block_manager.prepare_replica_index_reload().unwrap();
        let updated_index = reload.load_candidate().await.unwrap();
        let publication =
            ReplicaPublication::Published(crate::storage::entry::publication::PublicationId {
                incarnation: "test-incarnation".to_string(),
                generation: 2,
            });
        block_manager
            .apply_replica_index_reload(reload, updated_index, publication.clone())
            .await
            .unwrap();

        assert_eq!(block_manager.accepted_publication, publication);
        assert!(block_manager.block_index.info().contains_key(&1));
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
            Default::default(),
        )
        .await
        .unwrap();

        let mut updated_index = BlockIndex::new(index_path.clone());
        updated_index.insert_or_update_with_crc(Block::new(1), 2);
        updated_index.save().await.unwrap();

        let reload = block_manager.prepare_replica_index_reload().unwrap();
        let updated_index = reload.load_candidate().await.unwrap();
        block_manager
            .apply_replica_index_reload(reload, updated_index, ReplicaPublication::Legacy)
            .await
            .unwrap();

        assert_eq!(
            block_manager.block_index.info().get(&1).unwrap().crc64,
            Some(2)
        );
    }

    #[rstest]
    #[tokio::test(flavor = "current_thread")]
    async fn test_reload_if_readonly_discards_compressed_cache_on_crc_change(
        #[future] path: PathBuf,
    ) {
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

        std::fs::write(entry_path.join("1.meta.zst"), b"old-meta").unwrap();
        std::fs::write(entry_path.join("1.blk.zst"), b"old-data").unwrap();

        FILE_CACHE
            .read(&entry_path.join("1.meta.zst"), std::io::SeekFrom::Start(0))
            .await
            .unwrap();
        FILE_CACHE
            .read(&entry_path.join("1.blk.zst"), std::io::SeekFrom::Start(0))
            .await
            .unwrap();

        let mut block_manager = BlockManager::build(
            entry_path.clone(),
            index,
            "bucket".to_string(),
            "entry".to_string(),
            Arc::new(cfg),
            Default::default(),
        )
        .await
        .unwrap();

        let mut updated_index = BlockIndex::new(index_path.clone());
        updated_index.insert_or_update_with_crc(Block::new(1), 2);
        updated_index.save().await.unwrap();

        std::fs::write(entry_path.join("1.meta.zst"), b"new-meta").unwrap();
        std::fs::write(entry_path.join("1.blk.zst"), b"new-data").unwrap();

        let reload = block_manager.prepare_replica_index_reload().unwrap();
        let updated_index = reload.load_candidate().await.unwrap();
        block_manager
            .apply_replica_index_reload(reload, updated_index, ReplicaPublication::Legacy)
            .await
            .unwrap();

        let mut meta = FILE_CACHE
            .read(&entry_path.join("1.meta.zst"), std::io::SeekFrom::Start(0))
            .await
            .unwrap();
        let mut meta_content = vec![];
        use std::io::Read;
        meta.read_to_end(&mut meta_content).unwrap();

        let mut data = FILE_CACHE
            .read(&entry_path.join("1.blk.zst"), std::io::SeekFrom::Start(0))
            .await
            .unwrap();
        let mut data_content = vec![];
        data.read_to_end(&mut data_content).unwrap();

        assert_eq!(meta_content, b"new-meta");
        assert_eq!(data_content, b"new-data");
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
            Default::default(),
        )
        .await
        .unwrap();

        let err = block_manager.load_block(1).await.err().unwrap();
        assert_eq!(err.status(), ErrorCode::TooEarly);
        assert!(block_manager.index().get_block(1).is_some());
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
            Default::default(),
        )
        .await
        .unwrap();

        let err = block_manager.load_block(1).await.err().unwrap();
        assert_eq!(err.status(), ErrorCode::TooEarly);
        assert!(block_manager.index().get_block(1).is_some());
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
