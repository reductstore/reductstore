// Copyright 2023-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::core::cache::Cache;
use backpack_rs::fs::File;
use backpack_rs::Backpack;
use log::{debug, warn};
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::backtrace::Backtrace;
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, RwLock, RwLockWriteGuard, Weak};
use std::thread::spawn;
use std::time::Duration;

const FILE_CACHE_MAX_SIZE: usize = 1024;
const FILE_CACHE_TIME_TO_LIVE: Duration = Duration::from_secs(60);

const FILECACHE_SYNC_INTERVAL: Duration = Duration::from_millis(100);

pub(crate) static FILE_CACHE: LazyLock<FileCache> = LazyLock::new(|| {
    FileCache::new(
        FILE_CACHE_MAX_SIZE,
        FILE_CACHE_TIME_TO_LIVE,
        FILECACHE_SYNC_INTERVAL,
    )
});

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
    synced: bool,
}

/// A cache to keep file descriptors open
///
/// This optimization is needed for network file systems because opening
/// and closing files for writing causes synchronization overhead.
///
/// Additionally, it periodically syncs files to disk to ensure data integrity.
pub(crate) struct FileCache {
    cache: Arc<RwLock<Cache<PathBuf, FileDescriptor>>>,
    stop_sync_worker: Arc<AtomicBool>,
    backpack: RwLock<Backpack>,
}

impl FileCache {
    /// Create a new file cache
    ///
    /// # Arguments
    ///
    /// * `max_size` - The maximum number of file descriptors to keep open
    /// * `ttl` - The time to live for a file descriptor
    /// * `sync_interval` - The interval to sync files from cache to disk
    fn new(max_size: usize, ttl: Duration, sync_interval: Duration) -> Self {
        let cache = Arc::new(RwLock::new(Cache::<PathBuf, FileDescriptor>::new(
            max_size, ttl,
        )));
        let cache_clone = Arc::clone(&cache);
        let stop_sync_worker = Arc::new(AtomicBool::new(false));
        let stop_sync_worker_clone = Arc::clone(&stop_sync_worker);

        spawn(move || {
            // Periodically sync files from cache to disk
            while !stop_sync_worker.load(Ordering::Relaxed) {
                std::thread::sleep(sync_interval);
                Self::sync_rw_and_unused_files(&cache);
            }
        });

        FileCache {
            cache: cache_clone,
            stop_sync_worker: stop_sync_worker_clone,
            backpack: RwLock::new(Backpack::default()),
        }
    }

    fn sync_rw_and_unused_files(cache: &Arc<RwLock<Cache<PathBuf, FileDescriptor>>>) {
        let mut cache = cache.write().unwrap();

        for (path, file_desc) in cache.iter_mut() {
            // Sync only writeable files that are not synced yet
            // and are not used by other threads
            if file_desc.mode != AccessMode::ReadWrite
                || file_desc.synced
                || Arc::strong_count(&file_desc.file_ref) > 1
                || Arc::weak_count(&file_desc.file_ref) > 0
            {
                continue;
            }

            if let Err(err) = file_desc.file_ref.write().unwrap().sync_all() {
                warn!("Failed to sync file {}: {}", path.display(), err);
                continue;
            }

            debug!("File {} synced to disk", path.display());
            file_desc.synced = true; // Mark as synced after successful sync
        }
    }

    pub fn set_backpack(&self, backpack: Backpack) {
        *self.backpack.write().unwrap() = backpack;
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
            let file = self.backpack.read()?.open_options().read(true).open(path)?;
            let file = Arc::new(RwLock::new(file));
            Self::save_in_cache_and_sync_discarded(
                path.clone(),
                &mut cache,
                &file,
                AccessMode::Read,
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
            desc.synced = false;

            if desc.mode == AccessMode::ReadWrite {
                Arc::clone(&desc.file_ref)
            } else {
                let rw_file = self
                    .backpack
                    .read()?
                    .open_options()
                    .write(true)
                    .read(true)
                    .open(path)?;
                desc.file_ref = Arc::new(RwLock::new(rw_file));
                desc.mode = AccessMode::ReadWrite;
                Arc::clone(&desc.file_ref)
            }
        } else {
            let file = self
                .backpack
                .read()?
                .open_options()
                .create(true)
                .write(true)
                .read(true)
                .open(path)?;
            let file = Arc::new(RwLock::new(file));
            Self::save_in_cache_and_sync_discarded(
                path.clone(),
                &mut cache,
                &file,
                AccessMode::ReadWrite,
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
            self.backpack.read()?.remove(path)?;
        }

        Ok(())
    }

    pub fn remove_dir(&self, path: &PathBuf) -> Result<(), ReductError> {
        self.discard_recursive(path)?;
        if path.try_exists()? {
            self.backpack.read()?.remove_dir_all(path)?;
        }

        Ok(())
    }

    pub fn create_dir_all(&self, path: &PathBuf) -> Result<(), ReductError> {
        self.backpack.read()?.create_dir_all(path)?;
        Ok(())
    }

    pub fn read_dir(&self, path: &PathBuf) -> Result<Vec<PathBuf>, ReductError> {
        let mut entries = Vec::new();
        for entry in self.backpack.read()?.read_dir(path)? {
            let entry = entry?;
            entries.push(entry.path());
        }
        Ok(entries)
    }

    /// Discards all files in the cache that are under the specified path.
    ///
    /// This function iterates through the cache and removes all file descriptors
    /// whose paths start with the specified `path`. If a file is in read-write mode
    /// and has not been synced, it attempts to sync the file before removing it from the cache.
    ///
    pub fn discard_recursive(&self, path: &PathBuf) -> Result<(), ReductError> {
        let mut cache = self.cache.write()?;
        let files_to_remove = cache
            .keys()
            .iter()
            .filter(|file_path| file_path.starts_with(path))
            .map(|file_path| (*file_path).clone())
            .collect::<Vec<PathBuf>>();

        for file_path in files_to_remove {
            if let Some(file) = cache.remove(&file_path) {
                // If the file is in read-write mode and not synced, we need to sync it before removing from cache
                if file.mode == AccessMode::ReadWrite && !file.synced {
                    if let Err(err) = file.file_ref.write()?.sync_all() {
                        warn!("Failed to sync file {}: {}", file_path.display(), err);
                    }
                }
            }
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
        self.backpack.read()?.rename(old_path, new_path)?;
        Ok(())
    }

    /// Saves a file descriptor in the cache and syncs any discarded files.
    ///
    /// We need to make sure that we sync all files that were discarded
    fn save_in_cache_and_sync_discarded(
        path: PathBuf,
        cache: &mut RwLockWriteGuard<Cache<PathBuf, FileDescriptor>>,
        file: &Arc<RwLock<File>>,
        mode: AccessMode,
    ) -> () {
        let discarded_files = cache.insert(
            path.clone(),
            FileDescriptor {
                file_ref: Arc::clone(file),
                mode,
                synced: false,
            },
        );

        for (_, file) in discarded_files {
            if let Err(err) = file.file_ref.write().unwrap().sync_all() {
                warn!("Failed to sync file {}: {}", path.display(), err);
            }
        }
    }
}

impl Drop for FileCache {
    fn drop(&mut self) {
        self.stop_sync_worker.store(true, Ordering::Relaxed);
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

    mod sync_rw_and_unused_files {
        use super::*;

        #[rstest]
        fn test_sync_unused_files(cache: FileCache, tmp_dir: PathBuf) {
            let file_path = tmp_dir.join("test_sync_rw_and_unused_files.txt");
            {
                let _file_ref = cache
                    .write_or_create(&file_path, SeekFrom::Start(0))
                    .unwrap()
                    .upgrade()
                    .unwrap();

                assert!(
                    !cache.cache.write().unwrap().get(&file_path).unwrap().synced,
                    "File should not be synced initially"
                );
            }

            // Wait for the sync worker to run
            sleep(Duration::from_millis(150));

            assert!(
                cache.cache.write().unwrap().get(&file_path).unwrap().synced,
                "File should be synced after sync worker runs"
            );
        }

        #[rstest]
        fn test_not_sync_used_files(cache: FileCache, tmp_dir: PathBuf) {
            let file_path = tmp_dir.join("test_not_sync_unused_files.txt");
            let _file_ref = cache
                .write_or_create(&file_path, SeekFrom::Start(0))
                .unwrap()
                .upgrade()
                .unwrap();

            // Wait for the sync worker to run
            sleep(Duration::from_millis(150));

            assert!(
                !cache.cache.write().unwrap().get(&file_path).unwrap().synced,
                "File should not be synced after sync worker runs"
            );
        }
    }

    #[fixture]
    fn cache() -> FileCache {
        FileCache::init(2, Duration::from_millis(100), Duration::from_millis(100))
    }

    #[fixture]
    fn tmp_dir() -> PathBuf {
        tempfile::tempdir().unwrap().keep()
    }
}
