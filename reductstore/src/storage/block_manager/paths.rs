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
