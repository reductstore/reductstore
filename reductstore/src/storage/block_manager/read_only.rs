// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::cfg::InstanceRole;
use crate::core::file_cache::FILE_CACHE;
use crate::storage::block_manager::BlockManager;
use std::time::Instant;

impl BlockManager {
    pub(super) async fn reload_if_readonly(
        &mut self,
    ) -> Result<(), reduct_base::error::ReductError> {
        if self.cfg.role == InstanceRole::Replica
            && self.last_replica_sync.elapsed() > self.cfg.engine_config.replica_update_interval
        {
            // we need to update the index from disk and chaned blocks for read-only instances
            let previous_state = self.block_index.info().clone();
            self.block_index.update_from_disc().await?;

            for (block_id, new_block_info) in self.block_index.info().iter() {
                if let Some(previous_block_info) = previous_state.get(block_id) {
                    if previous_block_info.crc64 != new_block_info.crc64 {
                        // block changed, we need to reload it
                        FILE_CACHE
                            .discard_recursive(&self.path_to_desc(*block_id))
                            .await?;
                        FILE_CACHE
                            .discard_recursive(&self.path_to_data(*block_id))
                            .await?;
                    }
                }
            }

            self.block_cache.clear();
            self.last_replica_sync = Instant::now();
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // test reloading in read-only mode

    use super::*;
    use crate::backend::Backend;
    use crate::cfg::storage_engine::StorageEngineConfig;
    use crate::cfg::Cfg;
    use crate::storage::block_manager::block::Block;
    use crate::storage::block_manager::block_index::BlockIndex;
    use crate::storage::block_manager::{BlockManager, BLOCK_INDEX_FILE};
    use rstest::{fixture, rstest};
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;
    use tempfile::tempdir;

    #[rstest]
    #[tokio::test]
    async fn test_reload_if_readonly(path: PathBuf) {
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
        let mut block_manager =
            BlockManager::build(path.clone(), index, Arc::new(cfg.clone())).await;

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

        assert!(block_manager.block_index.info().get(&1).is_some());
    }

    #[fixture]
    fn path() -> PathBuf {
        let dir = tempdir().unwrap().keep();
        let rt = tokio::runtime::Handle::current();
        rt.block_on(async {
            tokio::fs::create_dir_all(dir.join("bucket").join("entry"))
                .await
                .unwrap();
        });

        dir
    }
}
