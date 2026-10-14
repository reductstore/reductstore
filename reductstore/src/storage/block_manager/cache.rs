// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use super::*;

impl BlockManager {
    pub(super) async fn invalidate_replica_block_cache(
        &self,
        block_id: u64,
    ) -> Result<(), ReductError> {
        let mut first_err = None;
        for path in all_block_file_paths(&self.path, block_id) {
            if let Err(err) = self.file_cache.invalidate_local_cache_file(&path).await {
                if first_err.is_none() {
                    first_err = Some(err);
                }
            }
        }
        self.decompress_cache.invalidate(&self.path, block_id).await;
        if let Some(err) = first_err {
            return Err(err);
        }
        Ok(())
    }

    pub(in crate::storage) fn is_block_in_write_cache(&self, block_id: u64) -> bool {
        self.block_cache.get_write(&block_id).is_some()
    }

    #[cfg(test)]
    pub(crate) fn clear_cache_for_test(&self) {
        self.block_cache.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::sync::{reset_rwlock_config, set_rwlock_timeout};
    use crate::storage::block_manager::test_utils::{block_id, block_manager};
    use serial_test::serial;

    struct RwLockConfigGuard;

    impl Drop for RwLockConfigGuard {
        fn drop(&mut self) {
            reset_rwlock_config();
        }
    }

    #[rstest::rstest]
    #[tokio::test]
    #[serial]
    async fn test_invalidate_replica_block_cache_returns_first_error(
        #[future] block_manager: BlockManager,
        block_id: u64,
    ) {
        let block_manager = block_manager.await;
        let _reset = RwLockConfigGuard;
        set_rwlock_timeout(Duration::from_millis(10));

        let data_path = block_manager.path_to_data(block_id);
        let data_guard = block_manager
            .file_cache()
            .read(&data_path, SeekFrom::Start(0))
            .await
            .unwrap();

        let err = block_manager
            .invalidate_replica_block_cache(block_id)
            .await
            .unwrap_err();

        assert_eq!(
            err.status(),
            reduct_base::error::ErrorCode::InternalServerError
        );
        assert!(err
            .message
            .contains("Failed to acquire async owned write lock"));
        drop(data_guard);
    }
}
