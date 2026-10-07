// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use super::*;
use crate::storage::block_manager::decompress_cache::DecompressedFile;

/// Path to a block file to read.
///
/// For a compressed block it points to the decompressed copy, which stays on disk while this value is alive.
pub(in crate::storage) struct BlockFilePath {
    path: PathBuf,
    _decompressed: Option<DecompressedFile>,
}

impl BlockFilePath {
    pub(in crate::storage) fn path(&self) -> &PathBuf {
        &self.path
    }
}

impl From<PathBuf> for BlockFilePath {
    fn from(path: PathBuf) -> Self {
        Self {
            path,
            _decompressed: None,
        }
    }
}

impl From<DecompressedFile> for BlockFilePath {
    fn from(file: DecompressedFile) -> Self {
        Self {
            path: file.path().clone(),
            _decompressed: Some(file),
        }
    }
}

impl BlockManager {
    pub(super) async fn resolve_desc_path(
        &self,
        block_id: u64,
    ) -> Result<BlockFilePath, ReductError> {
        if self.block_is_compressed(block_id) {
            let compressed_desc_path = self.path_to_compressed_desc(block_id);
            if FILE_CACHE.try_exists(&compressed_desc_path).await? {
                let file = self
                    .decompress_cache
                    .get_or_decompress(
                        &self.path,
                        block_id,
                        DecompressedFileType::Descriptor,
                        &compressed_desc_path,
                    )
                    .await?;
                Ok(file.into())
            } else {
                Ok(self.path_to_desc(block_id).into())
            }
        } else {
            Ok(self.path_to_desc(block_id).into())
        }
    }

    pub(super) async fn resolve_data_path(
        &self,
        block_id: u64,
    ) -> Result<BlockFilePath, ReductError> {
        if self.block_is_compressed(block_id) {
            let file = self
                .decompress_cache
                .get_or_decompress(
                    &self.path,
                    block_id,
                    DecompressedFileType::Data,
                    &self.path_to_compressed_data(block_id),
                )
                .await?;
            Ok(file.into())
        } else {
            Ok(self.path_to_data(block_id).into())
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
