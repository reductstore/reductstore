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
                            .invalidate_local_cache_file(&self.path_to_desc(*block_id))
                            .await?;
                        FILE_CACHE
                            .discard_recursive(&self.path_to_data(*block_id))
                            .await?;
                        FILE_CACHE
                            .invalidate_local_cache_file(&self.path_to_data(*block_id))
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
    use crate::backend::file::AccessMode;
    use crate::backend::{Backend, ObjectMetadata};
    use crate::cfg::storage_engine::StorageEngineConfig;
    use crate::cfg::Cfg;
    use crate::storage::block_manager::block::Block;
    use crate::storage::block_manager::block_index::BlockIndex;
    use crate::storage::block_manager::{BlockManager, BLOCK_INDEX_FILE};
    use mockall::mock;
    use rstest::{fixture, rstest};
    use serial_test::serial;
    use std::io::SeekFrom;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::tempdir;

    mock! {
        StorageBackend {}

        #[async_trait::async_trait]
        impl crate::backend::StorageBackend for StorageBackend {
            fn path(&self) -> &PathBuf;
            async fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()>;
            async fn remove(&self, path: &std::path::Path) -> std::io::Result<()>;
            async fn remove_dir_all(&self, path: &std::path::Path) -> std::io::Result<()>;
            async fn create_dir_all(&self, path: &std::path::Path) -> std::io::Result<()>;
            async fn read_dir(&self, path: &std::path::Path) -> std::io::Result<Vec<PathBuf>>;
            async fn try_exists(&self, path: &std::path::Path) -> std::io::Result<bool>;
            async fn upload(&self, path: &std::path::Path) -> std::io::Result<()>;
            async fn download(&self, path: &std::path::Path) -> std::io::Result<()>;
            async fn update_local_cache(&self, path: &std::path::Path, mode: &AccessMode) -> std::io::Result<()>;
            async fn invalidate_locally_cached_files(&self) -> Vec<PathBuf>;
            async fn get_stats(&self, path: &std::path::Path) -> std::io::Result<Option<ObjectMetadata>>;
            async fn remove_from_local_cache(&self, path: &std::path::Path) -> std::io::Result<()>;
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_reload_if_readonly(#[future] path: PathBuf) {
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

    #[rstest]
    #[tokio::test(flavor = "current_thread")]
    #[serial]
    async fn test_reload_if_readonly_discards_cache_on_crc_change(#[future] path: PathBuf) {
        let path = path.await;
        let entry_path = path.join("bucket").join("entry");
        let desc_path = entry_path.join("1.meta");
        let data_path = entry_path.join("1.blk");

        tokio::fs::write(&desc_path, b"desc").await.unwrap();
        tokio::fs::write(&data_path, b"data").await.unwrap();

        let index_path = entry_path.join(BLOCK_INDEX_FILE);

        let mut mock = MockStorageBackend::new();
        mock.expect_path().return_const(path.clone()).times(0..);
        mock.expect_try_exists().returning(|_| Ok(false)).times(0..);
        mock.expect_download().returning(|_| Ok(())).times(0..);
        mock.expect_upload().returning(|_| Ok(())).times(0..);
        mock.expect_update_local_cache()
            .returning(|_, _| Ok(()))
            .times(0..);
        mock.expect_read_dir()
            .returning(|_| Ok(Vec::new()))
            .times(0..);
        mock.expect_create_dir_all()
            .returning(|_| Ok(()))
            .times(0..);
        mock.expect_rename().returning(|_, _| Ok(())).times(0..);
        mock.expect_remove().returning(|_| Ok(())).times(0..);
        mock.expect_remove_dir_all()
            .returning(|_| Ok(()))
            .times(0..);
        mock.expect_invalidate_locally_cached_files()
            .returning(Vec::new)
            .times(0..);
        mock.expect_get_stats().returning(|_| Ok(None)).times(0..);
        let desc_path_ref = desc_path.clone();
        mock.expect_remove_from_local_cache()
            .withf(move |path| path == desc_path_ref.as_path())
            .returning(|_| Ok(()))
            .times(2);
        let data_path_ref = data_path.clone();
        mock.expect_remove_from_local_cache()
            .withf(move |path| path == data_path_ref.as_path())
            .returning(|_| Ok(()))
            .times(2);
        let index_path_ref = index_path.clone();
        mock.expect_remove_from_local_cache()
            .withf(move |path| path == index_path_ref.as_path())
            .returning(|_| Ok(()))
            .times(2);

        let backend = Backend::from_backend(Box::new(mock));
        FILE_CACHE.set_storage_backend(backend).await;

        let cfg = Cfg {
            role: InstanceRole::Replica,
            data_path: path.clone(),
            engine_config: StorageEngineConfig {
                replica_update_interval: Duration::from_millis(50),
                ..Default::default()
            },
            ..Default::default()
        };

        let mut index = BlockIndex::new(index_path.clone());
        index.insert_or_update_with_crc(Block::new(1), 1);
        index.save().await.unwrap();

        let mut block_manager =
            BlockManager::build(entry_path.clone(), index, Arc::new(cfg.clone())).await;

        {
            let _desc_guard = FILE_CACHE
                .read(&desc_path, SeekFrom::Start(0))
                .await
                .unwrap();
            let _data_guard = FILE_CACHE
                .read(&data_path, SeekFrom::Start(0))
                .await
                .unwrap();
        }

        let mut updated_index = BlockIndex::new(index_path.clone());
        updated_index.insert_or_update_with_crc(Block::new(1), 2);
        updated_index.save().await.unwrap();

        tokio::time::sleep(Duration::from_millis(75)).await;
        block_manager.reload_if_readonly().await.unwrap();
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
