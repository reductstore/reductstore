// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::core::cache::Cache;
use crate::core::file_cache::FILE_CACHE;
use crate::core::sync::AsyncRwLock;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::collections::hash_map::DefaultHasher;
use std::fs::OpenOptions;
use std::hash::{Hash, Hasher};
use std::io::{Read, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DECOMPRESS_CACHE_MAX_SIZE: usize = 64;
const DECOMPRESS_CACHE_TTL: Duration = Duration::from_secs(30);

pub(crate) static DECOMPRESS_CACHE: LazyLock<DecompressCache> =
    LazyLock::new(|| DecompressCache::new(DECOMPRESS_CACHE_MAX_SIZE, DECOMPRESS_CACHE_TTL));

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DecompressedFileType {
    Data,
    Descriptor,
}

impl DecompressedFileType {
    fn as_key_part(&self) -> &'static str {
        match self {
            DecompressedFileType::Data => "data",
            DecompressedFileType::Descriptor => "desc",
        }
    }

    fn as_extension(&self) -> &'static str {
        match self {
            DecompressedFileType::Data => "blk",
            DecompressedFileType::Descriptor => "meta",
        }
    }
}

pub(crate) struct DecompressCache {
    cache: Arc<AsyncRwLock<Cache<String, PathBuf>>>,
}

impl DecompressCache {
    fn new(max_size: usize, ttl: Duration) -> Self {
        Self {
            cache: Arc::new(AsyncRwLock::new(Cache::new(max_size, ttl))),
        }
    }

    pub(crate) async fn get_or_decompress(
        &self,
        entry_path: &Path,
        block_id: u64,
        file_type: DecompressedFileType,
        compressed_path: &PathBuf,
    ) -> Result<PathBuf, ReductError> {
        let key = Self::key(entry_path, block_id, file_type);
        let mut cache = self.cache.write().await?;
        let expired = cache.discard_expired();
        for (_, path) in expired {
            cleanup_tmp(&path);
        }

        if let Some(path) = cache.get(&key) {
            return Ok(path.clone());
        }

        let path = self
            .decompress_to_temp(entry_path, block_id, file_type, compressed_path)
            .await?;
        let evicted = cache.insert(key, path.clone());
        drop(cache);

        for (_, path) in evicted {
            cleanup_tmp(&path);
        }

        Ok(path)
    }

    fn key(entry_path: &Path, block_id: u64, file_type: DecompressedFileType) -> String {
        format!(
            "{}::{}::{}",
            entry_path.display(),
            block_id,
            file_type.as_key_part()
        )
    }

    async fn decompress_to_temp(
        &self,
        entry_path: &Path,
        block_id: u64,
        file_type: DecompressedFileType,
        compressed_path: &PathBuf,
    ) -> Result<PathBuf, ReductError> {
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

        let temp_path = Self::temp_path(entry_path, block_id, file_type);
        let mut temp_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&temp_path)
            .map_err(|err| {
                internal_server_error!(
                    "Failed to create decompressed temporary file {:?}: {}",
                    temp_path,
                    err
                )
            })?;
        temp_file.write_all(&decompressed).map_err(|err| {
            internal_server_error!(
                "Failed to write decompressed temporary file {:?}: {}",
                temp_path,
                err
            )
        })?;
        temp_file.sync_all().map_err(|err| {
            internal_server_error!(
                "Failed to sync decompressed temporary file {:?}: {}",
                temp_path,
                err
            )
        })?;

        Ok(temp_path)
    }

    fn temp_path(entry_path: &Path, block_id: u64, file_type: DecompressedFileType) -> PathBuf {
        let mut hasher = DefaultHasher::new();
        entry_path.hash(&mut hasher);
        block_id.hash(&mut hasher);
        file_type.as_key_part().hash(&mut hasher);
        let entry_hash = hasher.finish();
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();

        std::env::temp_dir().join(format!(
            "reductstore_{}_{}_{}.{}",
            entry_hash,
            block_id,
            unique,
            file_type.as_extension()
        ))
    }
}

fn cleanup_tmp(path: &Path) {
    let _ = std::fs::remove_file(path);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use tempfile::tempdir;

    #[tokio::test]
    #[serial]
    async fn test_get_or_decompress_caches_decompressed_file() {
        let dir = tempdir().unwrap().keep();
        let compressed_path = dir.join("1.blk.zst");
        std::fs::write(
            &compressed_path,
            zstd::encode_all("content".as_bytes(), 3).unwrap(),
        )
        .unwrap();
        let cache = DecompressCache::new(64, Duration::from_secs(30));

        let path = cache
            .get_or_decompress(&dir, 1, DecompressedFileType::Data, &compressed_path)
            .await
            .unwrap();
        let cached_path = cache
            .get_or_decompress(&dir, 1, DecompressedFileType::Data, &compressed_path)
            .await
            .unwrap();

        assert_eq!(path, cached_path);
        assert_eq!(std::fs::read(path).unwrap(), b"content");
    }

    #[tokio::test]
    #[serial]
    async fn test_cache_eviction_removes_temp_file() {
        let dir = tempdir().unwrap().keep();
        let compressed_path_1 = dir.join("1.blk.zst");
        let compressed_path_2 = dir.join("2.blk.zst");
        std::fs::write(
            &compressed_path_1,
            zstd::encode_all("one".as_bytes(), 3).unwrap(),
        )
        .unwrap();
        std::fs::write(
            &compressed_path_2,
            zstd::encode_all("two".as_bytes(), 3).unwrap(),
        )
        .unwrap();
        let cache = DecompressCache::new(1, Duration::from_secs(30));

        let first_path = cache
            .get_or_decompress(&dir, 1, DecompressedFileType::Data, &compressed_path_1)
            .await
            .unwrap();
        assert!(first_path.exists());

        cache
            .get_or_decompress(&dir, 2, DecompressedFileType::Data, &compressed_path_2)
            .await
            .unwrap();

        assert!(!first_path.exists());
    }

    #[tokio::test]
    #[serial]
    async fn test_ttl_expiration_removes_temp_file() {
        let dir = tempdir().unwrap().keep();
        let compressed_path_1 = dir.join("1.blk.zst");
        std::fs::write(
            &compressed_path_1,
            zstd::encode_all("one".as_bytes(), 3).unwrap(),
        )
        .unwrap();
        let cache = DecompressCache::new(64, Duration::from_millis(1));

        let first_path = cache
            .get_or_decompress(&dir, 1, DecompressedFileType::Data, &compressed_path_1)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        let second_path = cache
            .get_or_decompress(&dir, 1, DecompressedFileType::Data, &compressed_path_1)
            .await
            .unwrap();

        assert!(!first_path.exists());
        assert!(second_path.exists());
        assert_ne!(first_path, second_path);
    }
}
