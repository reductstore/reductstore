// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

#![allow(dead_code)]

use crate::core::file_cache::FILE_CACHE;
use crate::storage::block_manager::{
    BlockManager, COMPRESSED_DATA_FILE_EXT, COMPRESSED_DESCRIPTOR_FILE_EXT,
};
use crate::storage::engine::MAX_IO_BUFFER_SIZE;
use crate::storage::proto::block_index::CompressionAlgorithm as ProtoCompressionAlgorithm;
use crc64fast::Digest;
use reduct_base::error::ReductError;
use reduct_base::{conflict, internal_server_error, not_found};
use std::cmp::min;
use std::fs::OpenOptions;
use std::io::{Read, SeekFrom, Write};
use std::path::{Path, PathBuf};
use zstd::stream::write::Encoder as ZstdEncoder;

const ZSTD_COMPRESSION_LEVEL: i32 = 3;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompressionAlgorithm {
    None,
    Zstd,
}

impl From<CompressionAlgorithm> for ProtoCompressionAlgorithm {
    fn from(value: CompressionAlgorithm) -> Self {
        match value {
            CompressionAlgorithm::None => ProtoCompressionAlgorithm::None,
            CompressionAlgorithm::Zstd => ProtoCompressionAlgorithm::Zstd,
        }
    }
}

impl From<ProtoCompressionAlgorithm> for CompressionAlgorithm {
    fn from(value: ProtoCompressionAlgorithm) -> Self {
        match value {
            ProtoCompressionAlgorithm::None => CompressionAlgorithm::None,
            ProtoCompressionAlgorithm::Zstd => CompressionAlgorithm::Zstd,
        }
    }
}

impl From<CompressionAlgorithm> for i32 {
    fn from(value: CompressionAlgorithm) -> Self {
        i32::from(ProtoCompressionAlgorithm::from(value))
    }
}

impl BlockManager {
    pub(crate) async fn compress_block(
        &mut self,
        block_id: u64,
        algorithm: CompressionAlgorithm,
    ) -> Result<(), ReductError> {
        {
            let block = self.block_index.get_block(block_id).ok_or_else(|| {
                not_found!(
                    "Block {} not found in entry {}/{}",
                    block_id,
                    self.bucket,
                    self.entry
                )
            })?;

            if block
                .compression
                .unwrap_or(i32::from(CompressionAlgorithm::None))
                != i32::from(CompressionAlgorithm::None)
            {
                return Err(conflict!(
                    "Block {}/{}/{} is already compressed",
                    self.bucket,
                    self.entry,
                    block_id
                ));
            }
        }

        match algorithm {
            CompressionAlgorithm::None => return Ok(()),
            CompressionAlgorithm::Zstd => {
                let (data_size, desc_size) = self.compress_block_zstd(block_id).await?;
                let block = self.block_index.get_block_mut(block_id).ok_or_else(|| {
                    not_found!(
                        "Block {} not found in entry {}/{}",
                        block_id,
                        self.bucket,
                        self.entry
                    )
                })?;
                block.size = data_size;
                block.metadata_size = desc_size;
            }
        }

        let block = self.block_index.get_block_mut(block_id).ok_or_else(|| {
            not_found!(
                "Block {} not found in entry {}/{}",
                block_id,
                self.bucket,
                self.entry
            )
        })?;
        block.compression = Some(i32::from(algorithm));
        self.block_index.save().await?;
        self.block_index.sync_all().await?;

        let data_path = self.path_to_data(block_id);
        let desc_path = self.path_to_desc(block_id);
        FILE_CACHE.remove(&data_path).await?;
        FILE_CACHE.remove(&desc_path).await?;
        FILE_CACHE.discard_recursive(&data_path).await?;
        FILE_CACHE.discard_recursive(&desc_path).await?;

        Ok(())
    }

    pub(crate) async fn decompress_block(&mut self, block_id: u64) -> Result<(), ReductError> {
        if !self.block_is_compressed(block_id) {
            return Ok(());
        }

        let compressed_data_path = self.path_to_compressed_data(block_id);
        let compressed_desc_path = self.path_to_compressed_desc(block_id);
        let data_path = self.path_to_data(block_id);
        let desc_path = self.path_to_desc(block_id);

        if let Err(err) = decompress_file_zstd(&compressed_data_path, &data_path).await {
            if let Err(mark_err) = self.mark_block_corrupted(block_id).await {
                log::error!(
                    "Failed to mark block {}/{}/{} as corrupted after decompression error: {}",
                    self.bucket_name(),
                    self.entry_name(),
                    block_id,
                    mark_err
                );
            }
            return Err(err);
        }
        if let Err(err) = decompress_file_zstd(&compressed_desc_path, &desc_path).await {
            if let Err(mark_err) = self.mark_block_corrupted(block_id).await {
                log::error!(
                    "Failed to mark block {}/{}/{} as corrupted after decompression error: {}",
                    self.bucket_name(),
                    self.entry_name(),
                    block_id,
                    mark_err
                );
            }
            return Err(err);
        }

        FILE_CACHE.remove(&compressed_data_path).await?;
        FILE_CACHE.remove(&compressed_desc_path).await?;
        FILE_CACHE.discard_recursive(&compressed_data_path).await?;
        FILE_CACHE.discard_recursive(&compressed_desc_path).await?;

        let data_size = tokio::fs::metadata(&data_path)
            .await
            .map_err(|err| {
                internal_server_error!("Failed to get data file size {:?}: {}", data_path, err)
            })?
            .len();
        let desc = std::fs::read(&desc_path).map_err(|err| {
            internal_server_error!("Failed to read descriptor file {:?}: {}", desc_path, err)
        })?;
        let desc_size = desc.len() as u64;
        let mut crc = Digest::new();
        crc.write(&desc);

        let block = self.block_index.get_block_mut(block_id).ok_or_else(|| {
            not_found!(
                "Block {} not found in entry {}/{}",
                block_id,
                self.bucket,
                self.entry
            )
        })?;
        block.compression = None;
        block.size = data_size;
        block.metadata_size = desc_size;
        block.crc64 = Some(crc.sum64());
        self.block_index.save().await?;

        self.decompress_cache.invalidate(&self.path, block_id).await;

        Ok(())
    }

    async fn compress_block_zstd(&self, block_id: u64) -> Result<(u64, u64), ReductError> {
        let data_path = self.path_to_data(block_id);
        let data_size = FILE_CACHE
            .read(&data_path, SeekFrom::Start(0))
            .await?
            .metadata()?
            .len();

        let compressed_data_path = self.path_to_compressed_data(block_id);
        let compressed_data_tmp_path = self
            .path
            .join(format!("{}{}.tmp", block_id, COMPRESSED_DATA_FILE_EXT));

        let desc_path = self.path_to_desc(block_id);
        let compressed_desc_path = self.path_to_compressed_desc(block_id);
        let compressed_desc_tmp_path = self.path.join(format!(
            "{}{}.tmp",
            block_id, COMPRESSED_DESCRIPTOR_FILE_EXT
        ));

        if let Err(err) = compress_file_zstd(&data_path, &compressed_data_tmp_path, data_size).await
        {
            cleanup_tmp(&compressed_data_tmp_path);
            return Err(err);
        }

        let desc_size = FILE_CACHE
            .read(&desc_path, SeekFrom::Start(0))
            .await?
            .metadata()?
            .len();
        if let Err(err) = compress_file_zstd(&desc_path, &compressed_desc_tmp_path, desc_size).await
        {
            cleanup_tmp(&compressed_data_tmp_path);
            cleanup_tmp(&compressed_desc_tmp_path);
            return Err(err);
        }

        if let Err(err) = tokio::fs::rename(&compressed_data_tmp_path, &compressed_data_path).await
        {
            cleanup_tmp(&compressed_data_tmp_path);
            cleanup_tmp(&compressed_desc_tmp_path);
            return Err(internal_server_error!(
                "Failed to rename compressed data file {:?} to {:?}: {}",
                compressed_data_tmp_path,
                compressed_data_path,
                err
            ));
        }

        if let Err(err) = tokio::fs::rename(&compressed_desc_tmp_path, &compressed_desc_path).await
        {
            cleanup_tmp(&compressed_desc_tmp_path);
            let _ = std::fs::remove_file(&compressed_data_path);
            return Err(internal_server_error!(
                "Failed to rename compressed descriptor file {:?} to {:?}: {}",
                compressed_desc_tmp_path,
                compressed_desc_path,
                err
            ));
        }
        {
            // we need to sync files after rename,
            // which doesn't work for S3 storage
            let mut file = FILE_CACHE
                .write_or_create(&compressed_data_path, SeekFrom::Start(0))
                .await?;
            file.flush_local().await?;
            file.sync_all().await?;
            let mut file = FILE_CACHE
                .write_or_create(&compressed_desc_path, SeekFrom::Start(0))
                .await?;
            file.flush_local().await?;
            file.sync_all().await?;
        }

        let compressed_data_size = tokio::fs::metadata(&compressed_data_path)
            .await
            .map_err(|err| {
                internal_server_error!(
                    "Failed to get compressed data file size {:?}: {}",
                    compressed_data_path,
                    err
                )
            })?
            .len();
        let compressed_desc_size = tokio::fs::metadata(&compressed_desc_path)
            .await
            .map_err(|err| {
                internal_server_error!(
                    "Failed to get compressed descriptor file size {:?}: {}",
                    compressed_desc_path,
                    err
                )
            })?
            .len();

        Ok((compressed_data_size, compressed_desc_size))
    }
}

async fn compress_file_zstd(
    source_path: &PathBuf,
    temp_path: &PathBuf,
    source_size: u64,
) -> Result<(), ReductError> {
    let temp_file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .read(true)
        .open(temp_path)
        .map_err(|err| {
            internal_server_error!(
                "Failed to create temporary compressed file {:?}: {}",
                temp_path,
                err
            )
        })?;

    let mut encoder = ZstdEncoder::new(temp_file, ZSTD_COMPRESSION_LEVEL).map_err(|err| {
        internal_server_error!("Failed to create zstd encoder for {:?}: {}", temp_path, err)
    })?;

    let mut read_bytes = 0u64;
    while read_bytes < source_size {
        let bytes_to_read = min(MAX_IO_BUFFER_SIZE as u64, source_size - read_bytes) as usize;
        let mut buf = vec![0; bytes_to_read];
        {
            let mut source = FILE_CACHE
                .read(source_path, SeekFrom::Start(read_bytes))
                .await?;
            source.read_exact(&mut buf).map_err(|err| {
                internal_server_error!(
                    "Failed to read file {:?} at offset {}: {}",
                    source_path,
                    read_bytes,
                    err
                )
            })?;
        }

        encoder.write_all(&buf).map_err(|err| {
            internal_server_error!("Failed to compress file {:?}: {}", source_path, err)
        })?;
        read_bytes += bytes_to_read as u64;
    }

    let compressed_file = encoder.finish().map_err(|err| {
        internal_server_error!("Failed to finish zstd stream {:?}: {}", temp_path, err)
    })?;
    compressed_file.sync_all().map_err(|err| {
        internal_server_error!("Failed to sync compressed file {:?}: {}", temp_path, err)
    })?;

    Ok(())
}

async fn decompress_file_zstd(
    compressed_path: &PathBuf,
    output_path: &PathBuf,
) -> Result<(), ReductError> {
    let mut compressed = vec![];
    {
        let mut file = FILE_CACHE.read(compressed_path, SeekFrom::Start(0)).await?;
        file.read_to_end(&mut compressed).map_err(|err| {
            internal_server_error!(
                "Failed to read compressed file {:?}: {}",
                compressed_path,
                err
            )
        })?;
    }

    let decompressed = zstd::decode_all(compressed.as_slice()).map_err(|err| {
        internal_server_error!("Failed to decompress file {:?}: {}", compressed_path, err)
    })?;

    let mut out = FILE_CACHE
        .write_or_create(output_path, SeekFrom::Start(0))
        .await?;
    out.write_all(&decompressed)?;
    out.sync_all().await?;
    Ok(())
}

fn cleanup_tmp(path: &Path) {
    let _ = std::fs::remove_file(path);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::Cfg;
    use crate::storage::block_manager::block_index::BlockIndex;
    use crate::storage::block_manager::decompress_cache::DecompressedFileType;
    use crate::storage::block_manager::{BLOCK_INDEX_FILE, DATA_FILE_EXT, DESCRIPTOR_FILE_EXT};
    use crate::storage::entry::io::record_reader::read_in_chunks;
    use crate::storage::proto::{record, Record};
    use prost_wkt_types::Timestamp;
    use reduct_base::error::ErrorCode;
    use rstest::rstest;
    use serial_test::serial;
    use tempfile::tempdir;

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_compress_block_zstd() {
        let data = b"compress me".to_vec();
        let (mut block_manager, block_id, original_data, original_descriptor) =
            block_manager_with_data(data).await;

        block_manager
            .compress_block(block_id, CompressionAlgorithm::Zstd)
            .await
            .unwrap();

        let compressed_data_path = block_manager.path_to_compressed_data(block_id);
        let compressed_descriptor_path = block_manager.path_to_compressed_desc(block_id);
        assert!(compressed_data_path.exists());
        assert!(compressed_descriptor_path.exists());

        let decompressed_data =
            zstd::decode_all(std::fs::File::open(&compressed_data_path).unwrap()).unwrap();
        assert_eq!(decompressed_data, original_data);

        let decompressed_descriptor =
            zstd::decode_all(std::fs::File::open(&compressed_descriptor_path).unwrap()).unwrap();
        assert_eq!(decompressed_descriptor, original_descriptor);

        assert!(!block_manager.path_to_data(block_id).exists());
        assert!(!block_manager.path_to_desc(block_id).exists());
        assert_eq!(
            block_manager
                .index()
                .get_block(block_id)
                .unwrap()
                .compression,
            Some(i32::from(CompressionAlgorithm::Zstd))
        );

        let compressed_data_size = std::fs::metadata(&compressed_data_path).unwrap().len();
        let compressed_descriptor_size = std::fs::metadata(&compressed_descriptor_path)
            .unwrap()
            .len();
        let block = block_manager.index().get_block(block_id).unwrap();
        assert_eq!(block.size, compressed_data_size);
        assert_eq!(block.metadata_size, compressed_descriptor_size);
        assert_eq!(
            block_manager.index().size(),
            compressed_data_size + compressed_descriptor_size
        );
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_compress_block_already_compressed() {
        let (mut block_manager, block_id, _, _) =
            block_manager_with_data(b"already compressed".to_vec()).await;
        block_manager
            .index_mut()
            .get_block_mut(block_id)
            .unwrap()
            .compression = Some(i32::from(CompressionAlgorithm::Zstd));

        let err = block_manager
            .compress_block(block_id, CompressionAlgorithm::Zstd)
            .await
            .err()
            .unwrap();

        assert_eq!(err.status(), ErrorCode::Conflict);
    }

    #[rstest]
    fn test_compression_algorithm_from_proto() {
        assert_eq!(
            CompressionAlgorithm::from(ProtoCompressionAlgorithm::None),
            CompressionAlgorithm::None
        );
        assert_eq!(
            CompressionAlgorithm::from(ProtoCompressionAlgorithm::Zstd),
            CompressionAlgorithm::Zstd
        );
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_compress_block_noop_with_none_algorithm() {
        let (mut block_manager, block_id, original_data, original_descriptor) =
            block_manager_with_data(b"no compression".to_vec()).await;

        block_manager
            .compress_block(block_id, CompressionAlgorithm::None)
            .await
            .unwrap();

        assert!(block_manager.path_to_data(block_id).exists());
        assert!(block_manager.path_to_desc(block_id).exists());
        assert!(!block_manager.path_to_compressed_data(block_id).exists());
        assert!(!block_manager.path_to_compressed_desc(block_id).exists());
        assert_eq!(
            std::fs::read(block_manager.path_to_data(block_id)).unwrap(),
            original_data
        );
        assert_eq!(
            std::fs::read(block_manager.path_to_desc(block_id)).unwrap(),
            original_descriptor
        );
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_compress_block_not_found() {
        let path = tempdir().unwrap().keep().join("bucket").join("entry");
        let mut block_manager = BlockManager::build(
            path.clone(),
            BlockIndex::new(path.join(BLOCK_INDEX_FILE)),
            "bucket".to_string(),
            "entry".to_string(),
            Cfg::default().into(),
            Default::default(),
        )
        .await
        .unwrap();

        let err = block_manager
            .compress_block(42, CompressionAlgorithm::Zstd)
            .await
            .err()
            .unwrap();

        assert_eq!(err.status(), ErrorCode::NotFound);
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_compress_block_cleans_temp_when_data_compression_fails() {
        let (mut block_manager, block_id, _, _) =
            block_manager_with_data(b"missing source".to_vec()).await;
        FILE_CACHE
            .remove(&block_manager.path_to_data(block_id))
            .await
            .unwrap();

        let err = block_manager
            .compress_block(block_id, CompressionAlgorithm::Zstd)
            .await
            .err()
            .unwrap();

        assert_eq!(err.status(), ErrorCode::InternalServerError);
        assert!(!block_manager
            .path()
            .join(format!("{}{}.tmp", block_id, COMPRESSED_DATA_FILE_EXT))
            .exists());
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_compress_block_uses_data_file_size() {
        let (mut block_manager, block_id, original_data, _) =
            block_manager_with_data(b"short data".to_vec()).await;
        block_manager
            .index_mut()
            .get_block_mut(block_id)
            .unwrap()
            .size = original_data.len() as u64 + 1;

        block_manager
            .compress_block(block_id, CompressionAlgorithm::Zstd)
            .await
            .unwrap();

        let decompressed_data = zstd::decode_all(
            std::fs::File::open(block_manager.path_to_compressed_data(block_id)).unwrap(),
        )
        .unwrap();

        assert_eq!(decompressed_data, original_data);
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_decompress_block_restores_files() {
        let data = b"decompress me".to_vec();
        let (mut block_manager, block_id, original_data, original_descriptor) =
            block_manager_with_data(data).await;

        block_manager
            .compress_block(block_id, CompressionAlgorithm::Zstd)
            .await
            .unwrap();
        let compressed_data_path = block_manager.path_to_compressed_data(block_id);
        let cached_data_path = block_manager
            .decompress_cache
            .get_or_decompress(
                block_manager.path(),
                block_id,
                DecompressedFileType::Data,
                &compressed_data_path,
            )
            .await
            .unwrap();

        block_manager.decompress_block(block_id).await.unwrap();

        assert!(block_manager.path_to_data(block_id).exists());
        assert!(block_manager.path_to_desc(block_id).exists());
        assert!(!block_manager.path_to_compressed_data(block_id).exists());
        assert!(!block_manager.path_to_compressed_desc(block_id).exists());
        assert!(!cached_data_path.exists());
        assert_eq!(
            std::fs::read(block_manager.path_to_data(block_id)).unwrap(),
            original_data
        );
        assert_eq!(
            std::fs::read(block_manager.path_to_desc(block_id)).unwrap(),
            original_descriptor
        );

        let block = block_manager.index().get_block(block_id).unwrap();
        assert_eq!(block.compression, None);
        assert_eq!(block.size, original_data.len() as u64);
        assert_eq!(block.metadata_size, original_descriptor.len() as u64);
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_decompress_block_noop_if_not_compressed() {
        let (mut block_manager, block_id, original_data, original_descriptor) =
            block_manager_with_data(b"not compressed".to_vec()).await;

        block_manager.decompress_block(block_id).await.unwrap();

        assert!(block_manager.path_to_data(block_id).exists());
        assert!(block_manager.path_to_desc(block_id).exists());
        assert!(!block_manager.path_to_compressed_data(block_id).exists());
        assert!(!block_manager.path_to_compressed_desc(block_id).exists());
        assert_eq!(
            std::fs::read(block_manager.path_to_data(block_id)).unwrap(),
            original_data
        );
        assert_eq!(
            std::fs::read(block_manager.path_to_desc(block_id)).unwrap(),
            original_descriptor
        );
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_decompress_block_with_corrupted_data() {
        let (mut block_manager, block_id, _, _) =
            block_manager_with_data(b"corrupt me".to_vec()).await;

        block_manager
            .compress_block(block_id, CompressionAlgorithm::Zstd)
            .await
            .unwrap();

        std::fs::write(
            block_manager.path_to_compressed_data(block_id),
            b"not valid zstd data",
        )
        .unwrap();

        let err = block_manager
            .decompress_block(block_id)
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), ErrorCode::InternalServerError);
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_decompress_block_with_corrupted_descriptor() {
        let (mut block_manager, block_id, _, _) =
            block_manager_with_data(b"corrupt desc".to_vec()).await;

        block_manager
            .compress_block(block_id, CompressionAlgorithm::Zstd)
            .await
            .unwrap();

        std::fs::write(block_manager.path_to_compressed_desc(block_id), b"garbage").unwrap();

        let err = block_manager
            .decompress_block(block_id)
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), ErrorCode::InternalServerError);
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_decompress_block_missing_compressed_file() {
        let (mut block_manager, block_id, _, _) =
            block_manager_with_data(b"missing file".to_vec()).await;

        block_manager
            .compress_block(block_id, CompressionAlgorithm::Zstd)
            .await
            .unwrap();

        std::fs::remove_file(block_manager.path_to_compressed_data(block_id)).unwrap();

        let err = block_manager
            .decompress_block(block_id)
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), ErrorCode::InternalServerError);
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_compress_file_zstd_missing_temp_parent() {
        let dir = tempdir().unwrap().keep();
        let source_path = dir.join("source.blk");
        let temp_path = dir.join("missing").join("source.blk.zst.tmp");
        std::fs::write(&source_path, b"data").unwrap();

        let err = compress_file_zstd(&source_path, &temp_path, 4)
            .await
            .err()
            .unwrap();

        assert_eq!(err.status(), ErrorCode::InternalServerError);
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_compress_file_zstd_short_source() {
        let dir = tempdir().unwrap().keep();
        let source_path = dir.join("source.blk");
        let temp_path = dir.join("source.blk.zst.tmp");
        std::fs::write(&source_path, b"a").unwrap();

        let err = compress_file_zstd(&source_path, &temp_path, 2)
            .await
            .err()
            .unwrap();

        assert_eq!(err.status(), ErrorCode::InternalServerError);
        cleanup_tmp(&temp_path);
        assert!(!temp_path.exists());
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_decompress_file_zstd_creates_output_parent() {
        let dir = tempdir().unwrap().keep();
        let compressed_path = dir.join("source.blk.zst");
        let output_path = dir.join("missing").join("source.blk");
        std::fs::write(
            &compressed_path,
            zstd::encode_all("data".as_bytes(), 3).unwrap(),
        )
        .unwrap();

        decompress_file_zstd(&compressed_path, &output_path)
            .await
            .unwrap();

        assert_eq!(std::fs::read(output_path).unwrap(), b"data");
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_compress_block_large_data() {
        let data = (0..MAX_IO_BUFFER_SIZE + 1)
            .map(|idx| (idx % 251) as u8)
            .collect::<Vec<_>>();
        let (mut block_manager, block_id, original_data, _) = block_manager_with_data(data).await;

        block_manager
            .compress_block(block_id, CompressionAlgorithm::Zstd)
            .await
            .unwrap();

        let compressed_data_path = block_manager.path_to_compressed_data(block_id);
        let decompressed_data =
            zstd::decode_all(std::fs::File::open(&compressed_data_path).unwrap()).unwrap();

        assert_eq!(decompressed_data, original_data);
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_load_compressed_block() {
        let (mut block_manager, block_id, _, _) =
            block_manager_with_data(b"load compressed descriptor".to_vec()).await;

        block_manager
            .compress_block(block_id, CompressionAlgorithm::Zstd)
            .await
            .unwrap();
        block_manager.clear_cache_for_test();

        let block_ref = block_manager.load_block(block_id).await.unwrap();
        let block = block_ref.read().await.unwrap();

        assert_eq!(block.block_id(), block_id);
        assert!(block.get_record(0).is_some());
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_read_record_from_compressed_block() {
        let data = b"read compressed data".to_vec();
        let (mut block_manager, block_id, original_data, _) = block_manager_with_data(data).await;

        block_manager
            .compress_block(block_id, CompressionAlgorithm::Zstd)
            .await
            .unwrap();
        block_manager.clear_cache_for_test();

        let block_ref = block_manager.load_block(block_id).await.unwrap();
        let block = block_ref.read().await.unwrap();
        let (file_path, offset) = block_manager.begin_read_record(&block, 0).await.unwrap();
        let (content, read) = read_in_chunks(&file_path, offset, original_data.len() as u64, 0)
            .await
            .unwrap();

        assert_eq!(read, original_data.len());
        assert_eq!(content, original_data);
    }

    async fn block_manager_with_data(data: Vec<u8>) -> (BlockManager, u64, Vec<u8>, Vec<u8>) {
        let block_id = 1;
        let path = tempdir().unwrap().keep().join("bucket").join("entry");
        let mut block_manager = BlockManager::build(
            path.clone(),
            BlockIndex::new(path.join(BLOCK_INDEX_FILE)),
            "bucket".to_string(),
            "entry".to_string(),
            Cfg::default().into(),
            Default::default(),
        )
        .await
        .unwrap();
        let block_ref = block_manager
            .start_new_block(block_id, data.len() as u64)
            .await
            .unwrap();

        let mut block = block_ref.write().await.unwrap();
        block.insert_or_update_record(Record {
            timestamp: Some(Timestamp {
                seconds: 0,
                nanos: 0,
            }),
            begin: 0,
            end: data.len() as u64,
            state: 0,
            labels: vec![],
            content_type: "".to_string(),
        });

        let (file, offset) = block_manager.begin_write_record(&block, 0).unwrap();
        drop(block);

        let mut data_file = FILE_CACHE
            .write_or_create(&file, SeekFrom::Start(offset))
            .await
            .unwrap();
        data_file.write_all(&data).unwrap();
        data_file.sync_all().await.unwrap();
        drop(data_file);

        block_manager
            .finish_write_record(block_id, record::State::Finished, 0)
            .await
            .unwrap();
        block_manager
            .save_meta_on_disk(block_ref.clone())
            .await
            .unwrap();

        FILE_CACHE.force_sync_all().await.unwrap();

        let original_descriptor = std::fs::read(block_manager.path_to_desc(block_id)).unwrap();
        assert!(block_manager
            .path()
            .join(format!("{}{}", block_id, DATA_FILE_EXT))
            .exists());
        assert!(block_manager
            .path()
            .join(format!("{}{}", block_id, DESCRIPTOR_FILE_EXT))
            .exists());

        (block_manager, block_id, data, original_descriptor)
    }
}
