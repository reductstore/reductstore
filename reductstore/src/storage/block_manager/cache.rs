// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use super::*;

impl BlockManager {
    pub(super) async fn invalidate_replica_block_cache(
        &self,
        block_id: u64,
    ) -> Result<(), ReductError> {
        FILE_CACHE
            .invalidate_local_cache_file(&self.path_to_desc(block_id))
            .await?;
        FILE_CACHE
            .invalidate_local_cache_file(&self.path_to_data(block_id))
            .await?;
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
