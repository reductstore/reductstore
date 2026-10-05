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
            if let Err(err) = FILE_CACHE.invalidate_local_cache_file(&path).await {
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
