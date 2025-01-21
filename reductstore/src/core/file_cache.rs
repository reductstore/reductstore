// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::core::cache::Cache;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::fs::{read_dir, remove_dir, remove_file, rename, File};
use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, RwLock, Weak};
use std::time::Duration;

const FILE_CACHE_MAX_SIZE: usize = 1024;
const FILE_CACHE_TIME_TO_LIVE: Duration = Duration::from_secs(60);

pub(crate) static FILE_CACHE: LazyLock<FileCache> =
    LazyLock::new(|| FileCache::new(FILE_CACHE_MAX_SIZE, FILE_CACHE_TIME_TO_LIVE));

pub(crate) struct FileWeak {
    file: Weak<RwLock<File>>,
    path: PathBuf,
}

impl FileWeak {
    fn new(file: FileRc, path: PathBuf) -> Self {
        FileWeak {
            file: Arc::downgrade(&file),
            path,
        }
    }

    pub fn upgrade(&self) -> Result<FileRc, ReductError> {
        self.file.upgrade().ok_or(internal_server_error!(
            "File descriptor for {:?} is no longer available",
            self.path
        ))
    }
}

pub(crate) type FileRc = Arc<RwLock<File>>;

#[derive(PartialEq)]
enum AccessMode {
    Read,
    ReadWrite,
}

struct FileDescriptor {
    file_ref: FileRc,
    mode: AccessMode,
}

/// A cache to keep file descriptors open
///
/// This optimization is needed for network file systems because opening
/// and closing files for writing causes synchronization overhead.
#[derive(Clone)]
pub(crate) struct FileCache {
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
    pub fn read(&self, path: &PathBuf, pos: SeekFrom) -> Result<FileWeak, ReductError> {
        let mut cache = self.cache.write()?;
        let file = if let Some(desc) = cache.get_mut(path) {
            Arc::clone(&desc.file_ref)
        } else {
            let file = File::options().read(true).open(path)?;
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
            file.write()?.seek(pos)?;
        }
        Ok(FileWeak::new(file, path.clone()))
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
    pub fn write_or_create(&self, path: &PathBuf, pos: SeekFrom) -> Result<FileWeak, ReductError> {
        let mut cache = self.cache.write()?;

        let file = if let Some(desc) = cache.get_mut(path) {
            if desc.mode == AccessMode::ReadWrite {
                Arc::clone(&desc.file_ref)
            } else {
                let rw_file = File::options().write(true).read(true).open(path)?;
                desc.file_ref = Arc::new(RwLock::new(rw_file));
                desc.mode = AccessMode::ReadWrite;

                Arc::clone(&desc.file_ref)
            }
        } else {
            let file = File::options()
                .create(true)
                .write(true)
                .read(true)
                .open(path)?;
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
            file.write()?.seek(pos)?;
        }
        Ok(FileWeak::new(file, path.clone()))
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
    pub fn remove(&self, path: &PathBuf) -> Result<(), ReductError> {
        let mut cache = self.cache.write()?;
        cache.remove(path);

        if path.try_exists()? {
            remove_file(path)?;
        }

        Ok(())
    }

    pub fn remove_dir(&self, path: &PathBuf) -> Result<(), ReductError> {
        if path.try_exists()? {
            // Remove all files in the directory recursively
            // due to some file systems not supporting removing non-empty directories
            Self::remove_dir_contents(path)?;
            remove_dir(path)?;
        }
        self.discard_recursive(path)
    }

    pub fn discard_recursive(&self, path: &PathBuf) -> Result<(), ReductError> {
        let mut cache = self.cache.write()?;
        let files_to_remove = cache
            .keys()
            .iter()
            .filter(|file_path| file_path.starts_with(path))
            .map(|file_path| (*file_path).clone())
            .collect::<Vec<PathBuf>>();
        for file_path in files_to_remove {
            cache.remove(&file_path);
        }
        Ok(())
    }

    /// Renames a file in the file system and updates the cache.
    ///
    /// This function attempts to rename a file at the specified old path to the new path.
    /// If the file exists and is successfully renamed, it removes the old path from the cache.
    ///
    /// # Arguments
    ///
    /// * `old_path` - The old path to the file to be renamed.
    /// * `new_path` - The new path to the file.
    ///
    /// # Returns
    ///
    /// A `Result` which is `Ok` if the file was successfully renamed, or an `Err` containing
    pub fn rename(&self, old_path: &PathBuf, new_path: &PathBuf) -> Result<(), ReductError> {
        let mut cache = self.cache.write()?;
        cache.remove(old_path);
        rename(old_path, new_path)?;
        Ok(())
    }

    fn remove_dir_contents<P: AsRef<Path>>(path: P) -> Result<(), ReductError> {
        for entry in read_dir(path)? {
            let entry = entry?;
            let path = entry.path();

            if entry.file_type()?.is_dir() {
                Self::remove_dir_contents(&path)?;
                remove_dir(path)?;
            } else {
                remove_file(path)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs;
    use std::io::Write;

    use rstest::*;
    use std::io::Read;
    use std::thread::sleep;

    #[rstest]
    fn test_read(cache: FileCache, tmp_dir: PathBuf) {
        let file_path = tmp_dir.join("test_read.txt");
        let mut file = fs::File::create(&file_path).unwrap();
        file.write_all(b"test").unwrap();
        file.sync_all().unwrap();
        drop(file);

        let file_ref = cache.read(&file_path, SeekFrom::Start(0)).unwrap();
        let mut data = String::new();
        file_ref
            .upgrade()
            .unwrap()
            .write()
            .unwrap()
            .read_to_string(&mut data)
            .unwrap();
        assert_eq!(data, "test", "should read from beginning");

        let file_ref = cache
            .read(&file_path, SeekFrom::End(-2))
            .unwrap()
            .upgrade()
            .unwrap();
        let mut data = String::new();
        file_ref.write().unwrap().read_to_string(&mut data).unwrap();
        assert_eq!(data, "st", "should read last 2 bytes");
    }

    #[rstest]
    fn test_write_or_create(cache: FileCache, tmp_dir: PathBuf) {
        let file_path = tmp_dir.join("test_write_or_create.txt");

        let file_ref = cache
            .write_or_create(&file_path, SeekFrom::Start(0))
            .unwrap()
            .upgrade()
            .unwrap();
        {
            let mut file = file_ref.write().unwrap();
            file.write_all(b"test").unwrap();
            file.sync_all().unwrap();
        };

        assert_eq!(
            fs::read(&file_path).unwrap(),
            b"test",
            "should write to file"
        );

        let file_ref = cache
            .write_or_create(&file_path, SeekFrom::End(-2))
            .unwrap()
            .upgrade()
            .unwrap();
        {
            let mut file = file_ref.write().unwrap();
            file.write_all(b"xx").unwrap();
            file.sync_all().unwrap();
        }

        assert_eq!(
            fs::read(&file_path).unwrap(),
            b"texx",
            "should override last 2 bytes"
        );
    }

    #[rstest]
    fn test_remove(cache: FileCache, tmp_dir: PathBuf) {
        let file_path = tmp_dir.join("test_remove.txt");
        let mut file = fs::File::create(&file_path).unwrap();
        file.write_all(b"test").unwrap();
        file.sync_all().unwrap();
        drop(file);

        cache.remove(&file_path).unwrap();
        assert_eq!(file_path.exists(), false);
    }

    #[rstest]
    fn test_cache_max_size(cache: FileCache, tmp_dir: PathBuf) {
        let file_path1 = tmp_dir.join("test_cache_max_size1.txt");
        let file_path2 = tmp_dir.join("test_cache_max_size2.txt");
        let file_path3 = tmp_dir.join("test_cache_max_size3.txt");

        cache
            .write_or_create(&file_path1, SeekFrom::Start(0))
            .unwrap();
        cache
            .write_or_create(&file_path2, SeekFrom::Start(0))
            .unwrap();
        cache
            .write_or_create(&file_path3, SeekFrom::Start(0))
            .unwrap();

        let mut inner_cache = cache.cache.write().unwrap();
        assert_eq!(inner_cache.len(), 2);
        assert!(inner_cache.get(&file_path1).is_none());
    }

    #[rstest]
    fn test_cache_ttl(cache: FileCache, tmp_dir: PathBuf) {
        let file_path1 = tmp_dir.join("test_cache_max_size1.txt");
        let file_path2 = tmp_dir.join("test_cache_max_size2.txt");
        let file_path3 = tmp_dir.join("test_cache_max_size3.txt");

        cache
            .write_or_create(&file_path1, SeekFrom::Start(0))
            .unwrap();
        cache
            .write_or_create(&file_path2, SeekFrom::Start(0))
            .unwrap();

        sleep(Duration::from_millis(200));

        cache
            .write_or_create(&file_path3, SeekFrom::Start(0))
            .unwrap(); // should remove the file_path1 descriptor

        let mut inner_cache = cache.cache.write().unwrap();
        assert_eq!(inner_cache.len(), 1);
        assert!(inner_cache.get(&file_path1).is_none());
    }

    #[rstest]
    fn test_remove_dir(cache: FileCache, tmp_dir: PathBuf) {
        let file_1 = cache
            .write_or_create(&tmp_dir.join("test_remove_dir.txt"), SeekFrom::Start(0))
            .unwrap();
        let file_2 = cache
            .write_or_create(&tmp_dir.join("test_remove_dir.txt"), SeekFrom::Start(0))
            .unwrap();

        cache.remove_dir(&tmp_dir).unwrap();

        assert!(file_1.upgrade().is_err());
        assert!(file_2.upgrade().is_err());

        assert_eq!(tmp_dir.exists(), false);
    }

    #[fixture]
    fn cache() -> FileCache {
        FileCache::new(2, Duration::from_millis(100))
    }

    #[fixture]
    fn tmp_dir() -> PathBuf {
        tempfile::tempdir().unwrap().into_path()
    }
}
