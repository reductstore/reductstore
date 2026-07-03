// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use super::*;

impl BlockManager {
    pub(super) async fn resolve_desc_path(&self, block_id: u64) -> Result<PathBuf, ReductError> {
        if self.block_is_compressed(block_id) {
            let compressed_desc_path = self.path_to_compressed_desc(block_id);
            if FILE_CACHE.try_exists(&compressed_desc_path).await? {
                self.decompress_cache
                    .get_or_decompress(
                        &self.path,
                        block_id,
                        DecompressedFileType::Descriptor,
                        &compressed_desc_path,
                    )
                    .await
            } else {
                Ok(self.path_to_desc(block_id))
            }
        } else {
            Ok(self.path_to_desc(block_id))
        }
    }

    pub(super) async fn resolve_data_path(&self, block_id: u64) -> Result<PathBuf, ReductError> {
        if self.block_is_compressed(block_id) {
            self.decompress_cache
                .get_or_decompress(
                    &self.path,
                    block_id,
                    DecompressedFileType::Data,
                    &self.path_to_compressed_data(block_id),
                )
                .await
        } else {
            Ok(self.path_to_data(block_id))
        }
    }

    pub(in crate::storage) fn block_is_compressed(&self, block_id: u64) -> bool {
        self.block_index
            .get_block(block_id)
            .and_then(|block| block.compression)
            .unwrap_or(i32::from(CompressionAlgorithm::None))
            != i32::from(CompressionAlgorithm::None)
    }

    pub fn index_mut(&mut self) -> &mut BlockIndex {
        &mut self.block_index
    }

    pub fn index(&self) -> &BlockIndex {
        &self.block_index
    }

    pub(in crate::storage) fn usage_counters(&self) -> &UsageCounters {
        &self.usage_counters
    }

    pub(in crate::storage) fn usage_counters_arc(&self) -> Arc<UsageCounters> {
        Arc::clone(&self.usage_counters)
    }

    pub(in crate::storage) fn is_replica(&self) -> bool {
        self.cfg.role == InstanceRole::Replica
    }

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

    pub fn bucket_name(&self) -> &String {
        &self.bucket
    }

    pub fn entry_name(&self) -> &String {
        &self.entry
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    #[cfg(test)]
    pub(crate) fn clear_cache_for_test(&self) {
        self.block_cache.clear();
    }

    pub(super) fn path_to_desc(&self, block_id: u64) -> PathBuf {
        self.path
            .join(format!("{}{}", block_id, DESCRIPTOR_FILE_EXT))
    }

    pub(super) fn path_to_data(&self, block_id: u64) -> PathBuf {
        self.path.join(format!("{}{}", block_id, DATA_FILE_EXT))
    }

    pub(super) fn path_to_compressed_desc(&self, block_id: u64) -> PathBuf {
        self.path
            .join(format!("{}{}", block_id, COMPRESSED_DESCRIPTOR_FILE_EXT))
    }

    pub(super) fn path_to_compressed_data(&self, block_id: u64) -> PathBuf {
        self.path
            .join(format!("{}{}", block_id, COMPRESSED_DATA_FILE_EXT))
    }
}
