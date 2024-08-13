// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::fs::File;
use tokio::sync::RwLock;
use tokio::time::Instant;

use reduct_base::error::ReductError;

pub(super) type FileRef = Arc<RwLock<File>>;

const FILE_CACHE_MAX_SIZE: usize = 1024;
const FILE_CACHE_TIME_TO_LIVE: Duration = Duration::from_secs(60);

pub(super) fn get_global_file_cache() -> &'static FileCache {
    static mut FILE_CACHE: OnceLock<FileCache> = OnceLock::new();
    unsafe {
        FILE_CACHE.get_or_init(|| FileCache::new(FILE_CACHE_MAX_SIZE, FILE_CACHE_TIME_TO_LIVE))
    }
}

#[derive(PartialEq)]
enum AccessMode {
    Read,
    ReadWrite,
}

struct FileDescriptor {
    file_ref: FileRef,
    mode: AccessMode,
    used: Instant,
}

/// A cache to keep file descriptors open
#[derive(Clone)]
pub(in crate::storage) struct FileCache {
    cache: Arc<RwLock<HashMap<PathBuf, FileDescriptor>>>,
    max_size: usize,
    ttl: Duration,
}

impl FileCache {
    pub fn new(max_size: usize, ttl: Duration) -> Self {
        FileCache {
            cache: Arc::new(RwLock::new(HashMap::new())),
            max_size,
            ttl,
        }
    }

    pub async fn read(&self, path: &PathBuf) -> Result<FileRef, ReductError> {
        let mut cache = self.cache.write().await;
        let file = if let Some(desc) = cache.get_mut(path) {
            desc.used = Instant::now();
            Arc::clone(&desc.file_ref)
        } else {
            let file = File::options().read(true).open(path).await?;
            let file = Arc::new(RwLock::new(file));
            cache.insert(
                path.clone(),
                FileDescriptor {
                    file_ref: Arc::clone(&file),
                    mode: AccessMode::Read,
                    used: Instant::now(),
                },
            );
            file
        };

        Self::discard_old_descriptors(self.ttl, self.max_size, &mut cache);
        Ok(file)
    }

    pub async fn write_or_create(&self, path: &PathBuf) -> Result<FileRef, ReductError> {
        let mut cache = self.cache.write().await;

        let file = if let Some(desc) = cache.get_mut(path) {
            desc.used = Instant::now();
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
                    used: Instant::now(),
                },
            );
            file
        };

        Self::discard_old_descriptors(self.ttl, self.max_size, &mut cache);
        Ok(file)
    }

    pub async fn remove(&self, path: &PathBuf) -> Result<(), ReductError> {
        tokio::fs::remove_file(path).await?;
        let mut cache = self.cache.write().await;
        cache.remove(path);
        Ok(())
    }

    fn discard_old_descriptors(
        ttl: Duration,
        max_size: usize,
        cache: &mut HashMap<PathBuf, FileDescriptor>,
    ) {
        // remove old descriptors
        cache.retain(|_, desc| desc.used.elapsed() < ttl);

        // check if the cache is full and remove old
        if cache.len() > max_size {
            let mut oldest: Option<(&PathBuf, &FileDescriptor)> = None;

            for (path, desc) in cache.iter() {
                if let Some(oldest_desc) = oldest {
                    if desc.used < oldest_desc.1.used {
                        oldest = Some((path, desc));
                    }
                } else {
                    oldest = Some((path, desc));
                }
            }

            let path = oldest.unwrap().0.clone();
            cache.remove(&path);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write;

    use rstest::*;
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

        let file_ref = cache.read(&file_path).await.unwrap();
        let file = file_ref.read().await;
        assert_eq!(file.metadata().await.unwrap().len(), 4);
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_or_create(cache: FileCache, tmp_dir: tempfile::TempDir) {
        let file_path = tmp_dir.path().join("test_write_or_create.txt");

        let file_ref = cache.write_or_create(&file_path).await.unwrap();
        let file = file_ref.read().await;
        assert_eq!(file.metadata().await.unwrap().len(), 0);
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

        cache.write_or_create(&file_path1).await.unwrap();
        cache.write_or_create(&file_path2).await.unwrap();
        cache.write_or_create(&file_path3).await.unwrap();

        let inner_cache = cache.cache.read().await;
        assert_eq!(inner_cache.len(), 2);
        assert_eq!(inner_cache.contains_key(&file_path1), false);
    }

    #[rstest]
    #[tokio::test]
    async fn test_cache_ttl(cache: FileCache, tmp_dir: tempfile::TempDir) {
        let file_path1 = tmp_dir.path().join("test_cache_max_size1.txt");
        let file_path2 = tmp_dir.path().join("test_cache_max_size2.txt");
        cache.write_or_create(&file_path1).await.unwrap();
        cache.write_or_create(&file_path2).await.unwrap();

        sleep(Duration::from_millis(200)).await;

        cache.read(&file_path2).await.unwrap(); // should remove the file_path1 descriptor

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
