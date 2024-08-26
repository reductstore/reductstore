// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use tokio::fs::File;
use tokio::io::AsyncSeekExt;
use tokio::sync::RwLock;
use tokio::time::Instant;

use crate::core::cache::Cache;
use reduct_base::error::ReductError;

pub(super) type FileRef = Arc<RwLock<File>>;

const FILE_CACHE_MAX_SIZE: usize = 1024;
const FILE_CACHE_TIME_TO_LIVE: Duration = Duration::from_secs(60);

pub(super) static FILE_CACHE: LazyLock<FileCache> =
    LazyLock::new(|| FileCache::new(FILE_CACHE_MAX_SIZE, FILE_CACHE_TIME_TO_LIVE));

#[derive(PartialEq)]
enum AccessMode {
    Read,
    ReadWrite,
}

struct FileDescriptor {
    file_ref: FileRef,
    mode: AccessMode,
}

/// A cache to keep file descriptors open
///
/// This optimization is needed for network file systems because opening
/// and closing files for writing causes synchronization overhead.
#[derive(Clone)]
pub(in crate::storage) struct FileCache {
    cache: Arc<RwLock<Cache<PathBuf, FileDescriptor>>>,
}

impl FileCache {
    /// Create a new file cache
    ///
    /// # Arguments
    ///
    /// * `max_size` - The maximum number of file descriptors to keep open
    /// * `ttl` - The time to live for a file descriptor
    pub fn new(max_size: usize, ttl: Duration) -> Self {
        FileCache {
            cache: Arc::new(RwLock::new(Cache::new(max_size, ttl))),
        }
    }

    /// Get a file descriptor for reading
    ///
    /// If the file is not in the cache, it will be opened and added to the cache.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the file
    /// * `pos` - The position to read from
    ///
    /// # Returns
    ///
    /// A file reference
    pub async fn read(&self, path: &PathBuf, pos: SeekFrom) -> Result<FileRef, ReductError> {
        let mut cache = self.cache.write().await;
        let file = if let Some(desc) = cache.get_mut(path) {
            Arc::clone(&desc.file_ref)
        } else {
            let file = File::options().read(true).open(path).await?;
            let file = Arc::new(RwLock::new(file));
            cache.insert(
                path.clone(),
                FileDescriptor {
                    file_ref: Arc::clone(&file),
                    mode: AccessMode::Read,
                },
            );
            file
        };

        if pos != SeekFrom::Current(0) {
            file.write().await.seek(pos).await?;
        }
        Ok(file)
    }

    /// Get a file descriptor for writing
    ///
    /// If the file is not in the cache, it will be opened or created and added to the cache.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the file
    /// * `pos` - The position to write to
    ///
    ///
    /// # Returns
    ///
    /// A file reference
    pub async fn write_or_create(
        &self,
        path: &PathBuf,
        pos: SeekFrom,
    ) -> Result<FileRef, ReductError> {
        let mut cache = self.cache.write().await;

        let file = if let Some(desc) = cache.get_mut(path) {
            if desc.mode == AccessMode::ReadWrite {
                Arc::clone(&desc.file_ref)
            } else {
                let rw_file = File::options().write(true).read(true).open(path).await?;
                desc.file_ref = Arc::new(RwLock::new(rw_file));
                desc.mode = AccessMode::ReadWrite;

                Arc::clone(&desc.file_ref)
            }
        } else {
            let file = File::options()
                .create(true)
                .write(true)
                .read(true)
                .open(path)
                .await?;
            let file = Arc::new(RwLock::new(file));
            cache.insert(
                path.clone(),
                FileDescriptor {
                    file_ref: Arc::clone(&file),
                    mode: AccessMode::ReadWrite,
                },
            );
            file
        };

        if pos != SeekFrom::Current(0) {
            file.write().await.seek(pos).await?;
        }
        Ok(file)
    }

    /// Removes a file from the file system and the cache.
    ///
    /// This function attempts to remove a file at the specified path from the file system.
    /// If the file exists and is successfully removed, it also removes the file descriptor
    /// from the cache.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the file to be removed.
    ///
    /// # Returns
    ///
    /// A `Result` which is `Ok` if the file was successfully removed, or an `Err` containing
    /// a `ReductError` if an error occurred.
    ///
    /// # Errors
    ///
    /// This function will return an error if the file does not exist or if there is an issue
    /// removing the file from the file system.
    pub async fn remove(&self, path: &PathBuf) -> Result<(), ReductError> {
        if path.try_exists()? {
            tokio::fs::remove_file(path).await?;
        }
        let mut cache = self.cache.write().await;
        cache.remove(path);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write;

    use rstest::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::time::sleep;

    use super::*;

    #[rstest]
    #[tokio::test]
    async fn test_read(cache: FileCache, tmp_dir: tempfile::TempDir) {
        let file_path = tmp_dir.path().join("test_read.txt");
        let mut file = fs::File::create(&file_path).unwrap();
        file.write_all(b"test").unwrap();
        file.sync_all().unwrap();
        drop(file);

        let file_ref = cache.read(&file_path, SeekFrom::Start(0)).await.unwrap();
        let mut data = String::new();
        file_ref
            .write()
            .await
            .read_to_string(&mut data)
            .await
            .unwrap();
        assert_eq!(data, "test", "should read from beginning");

        let file_ref = cache.read(&file_path, SeekFrom::End(-2)).await.unwrap();
        let mut data = String::new();
        file_ref
            .write()
            .await
            .read_to_string(&mut data)
            .await
            .unwrap();
        assert_eq!(data, "st", "should read last 2 bytes");
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_or_create(cache: FileCache, tmp_dir: tempfile::TempDir) {
        let file_path = tmp_dir.path().join("test_write_or_create.txt");

        let file_ref = cache
            .write_or_create(&file_path, SeekFrom::Start(0))
            .await
            .unwrap();
        {
            let mut file = file_ref.write().await;
            file.write_all(b"test").await.unwrap();
            file.sync_all().await.unwrap();
        };

        assert_eq!(
            fs::read(&file_path).unwrap(),
            b"test",
            "should write to file"
        );

        let file_ref = cache
            .write_or_create(&file_path, SeekFrom::End(-2))
            .await
            .unwrap();
        {
            let mut file = file_ref.write().await;
            file.write_all(b"xx").await.unwrap();
            file.sync_all().await.unwrap();
        }

        assert_eq!(
            fs::read(&file_path).unwrap(),
            b"texx",
            "should override last 2 bytes"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove(cache: FileCache, tmp_dir: tempfile::TempDir) {
        let file_path = tmp_dir.path().join("test_remove.txt");
        let mut file = fs::File::create(&file_path).unwrap();
        file.write_all(b"test").unwrap();
        file.sync_all().unwrap();
        drop(file);

        cache.remove(&file_path).await.unwrap();
        assert_eq!(file_path.exists(), false);
    }

    #[rstest]
    #[tokio::test]
    async fn test_cache_max_size(cache: FileCache, tmp_dir: tempfile::TempDir) {
        let file_path1 = tmp_dir.path().join("test_cache_max_size1.txt");
        let file_path2 = tmp_dir.path().join("test_cache_max_size2.txt");
        let file_path3 = tmp_dir.path().join("test_cache_max_size3.txt");

        cache
            .write_or_create(&file_path1, SeekFrom::Start(0))
            .await
            .unwrap();
        cache
            .write_or_create(&file_path2, SeekFrom::Start(0))
            .await
            .unwrap();
        cache
            .write_or_create(&file_path3, SeekFrom::Start(0))
            .await
            .unwrap();

        let inner_cache = cache.cache.read().await;
        assert_eq!(inner_cache.len(), 2);
        assert_eq!(inner_cache.contains_key(&file_path1), false);
    }

    #[rstest]
    #[tokio::test]
    async fn test_cache_ttl(cache: FileCache, tmp_dir: tempfile::TempDir) {
        let file_path1 = tmp_dir.path().join("test_cache_max_size1.txt");
        let file_path2 = tmp_dir.path().join("test_cache_max_size2.txt");
        let file_path3 = tmp_dir.path().join("test_cache_max_size3.txt");

        cache
            .write_or_create(&file_path1, SeekFrom::Start(0))
            .await
            .unwrap();
        cache
            .write_or_create(&file_path2, SeekFrom::Start(0))
            .await
            .unwrap();

        sleep(Duration::from_millis(200)).await;

        cache
            .write_or_create(&file_path3, SeekFrom::Start(0))
            .await
            .unwrap(); // should remove the file_path1 descriptor

        let inner_cache = cache.cache.read().await;
        assert_eq!(inner_cache.len(), 1);
        assert_eq!(inner_cache.contains_key(&file_path1), false);
    }

    #[fixture]
    fn cache() -> FileCache {
        FileCache::new(2, Duration::from_millis(100))
    }

    #[fixture]
    fn tmp_dir() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }
}
