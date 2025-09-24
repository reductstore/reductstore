// Copyright 2023-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::backend::file::{AccessMode, File};
use crate::backend::Backend;
use crate::core::cache::Cache;
use log::{debug, warn};
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::fs;
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, RwLock, RwLockWriteGuard, Weak};
use std::thread::spawn;
use std::time::Duration;

const FILE_CACHE_MAX_SIZE: usize = 1024;
const FILE_CACHE_TIME_TO_LIVE: Duration = Duration::from_secs(60);

const FILE_CACHE_SYNC_INTERVAL: Duration = Duration::from_millis(60_000);

pub(crate) static FILE_CACHE: LazyLock<FileCache> = LazyLock::new(|| {
    #[allow(unused_mut)]
    let mut cache = FileCache::new(
        FILE_CACHE_MAX_SIZE,
        FILE_CACHE_TIME_TO_LIVE,
        FILE_CACHE_SYNC_INTERVAL,
    );

    #[cfg(test)]
    {
        // Use a temporary directory for tests without backend configuration
        cache.set_storage_backend(
            Backend::builder()
                .local_data_path(tempfile::tempdir().unwrap().keep().to_str().unwrap())
                .try_build()
                .unwrap(),
        );
    }

    cache
});

pub(crate) struct FileWeak {
    file: Weak<RwLock<File>>,
    path: PathBuf,
}

impl FileWeak {
    fn new(file: FileRc) -> Self {
        FileWeak {
            file: Arc::downgrade(&file),
            path: file.read().unwrap().path().clone(),
        }
    }

    pub fn upgrade(&self) -> Result<FileRc, ReductError> {
        let file = self.file.upgrade().ok_or(internal_server_error!(
            "File descriptor for {:?} is no longer available",
            self.path
        ))?;

        // Notify storage backend that the file was accessed
        file.read()?.access()?;
        Ok(file)
    }
}

pub(crate) type FileRc = Arc<RwLock<File>>;

/// A cache to keep file descriptors open
///
/// This optimization is needed for network file systems because opening
/// and closing files for writing causes synchronization overhead.
///
/// Additionally, it periodically syncs files to disk to ensure data integrity.
pub(crate) struct FileCache {
    cache: Arc<RwLock<Cache<PathBuf, FileRc>>>,
    stop_sync_worker: Arc<AtomicBool>,
    backpack: Arc<RwLock<Backend>>,
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
        let cache = Arc::new(RwLock::new(Cache::<PathBuf, FileRc>::new(max_size, ttl)));
        let cache_clone = Arc::clone(&cache);
        let stop_sync_worker = Arc::new(AtomicBool::new(false));
        let stop_sync_worker_clone = Arc::clone(&stop_sync_worker);
        let backpack = Arc::new(RwLock::new(Backend::default()));
        let backpack_clone = Arc::clone(&backpack);

        spawn(move || {
            // Periodically sync files from cache to disk
            while !stop_sync_worker.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(100));
                Self::sync_rw_and_unused_files(&backpack_clone, &cache, sync_interval, false);
            }
        });

        FileCache {
            cache: cache_clone,
            stop_sync_worker: stop_sync_worker_clone,
            backpack,
        }
    }

    fn sync_rw_and_unused_files(
        backend: &Arc<RwLock<Backend>>,
        cache: &Arc<RwLock<Cache<PathBuf, FileRc>>>,
        sync_interval: Duration,
        force: bool,
    ) {
        let mut cache = cache.write().unwrap();
        let invalidated_files = backend.read().unwrap().invalidate_locally_cached_files();
        for path in invalidated_files {
            if let Some(file) = cache.remove(&path) {
                if let Err(err) = file.write().unwrap().sync_all() {
                    warn!("Failed to sync invalidated file {:?}: {}", path, err);
                }
            }

            fs::remove_file(&path).ok();
            debug!("Removed invalidated file {:?} from cache and storage", path);
        }

        for (path, file) in cache.iter_mut() {
            let mut file_lock = if force {
                // force sync, we need to get a write lock and wait for it
                file.write().unwrap()
            } else {
                // if the file is used by other threads, sync it next time
                let Some(file) = file.try_write().ok() else {
                    continue;
                };
                file
            };

            // Sync only writeable files that are not synced yet
            // and are not used by other threads
            if file_lock.mode() != &AccessMode::ReadWrite
                || Arc::strong_count(&file) > 1
                || Arc::weak_count(&file) > 0
                || file_lock.is_synced()
                || file_lock.last_synced().elapsed() < sync_interval
            {
                continue;
            }

            if let Err(err) = file_lock.sync_all() {
                warn!("Failed to sync file {}: {}", path.display(), err);
                continue;
            }
        }
    }

    pub fn set_storage_backend(&self, backpack: Backend) {
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
        let open_file = |cache| -> Result<FileRc, ReductError> {
            let file = self.backpack.read()?.open_options().read(true).open(path)?;
            let file = Arc::new(RwLock::new(file));
            Self::save_in_cache_and_sync_discarded(path.clone(), cache, &file);
            Ok(file)
        };

        let file = if let Some(file) = cache.get_mut(path) {
            Arc::clone(&file)
        } else {
            open_file(&mut cache)?
        };

        if pos != SeekFrom::Current(0) {
            file.write()?.seek(pos)?;
        }
        Ok(FileWeak::new(file))
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
        let open_file = |cache, create| -> Result<FileRc, ReductError> {
            let file = self
                .backpack
                .read()?
                .open_options()
                .create(create)
                .write(true)
                .read(true)
                .open(path)?;
            let file = Arc::new(RwLock::new(file));
            Self::save_in_cache_and_sync_discarded(path.clone(), cache, &file);
            Ok(file)
        };

        let file = if let Some(file) = cache.get_mut(path).cloned() {
            let lock = file.read()?;
            if lock.mode() == &AccessMode::ReadWrite {
                Arc::clone(&file)
            } else {
                // file was opened in read-only mode, we need to reopen it in read-write mode
                open_file(&mut cache, false)?
            }
        } else {
            open_file(&mut cache, true)?
        };

        if pos != SeekFrom::Current(0) {
            file.write()?.seek(pos)?;
        }
        Ok(FileWeak::new(file))
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
        if let Some(file) = cache.get(path) {
            if Arc::strong_count(&file) > 1 || Arc::weak_count(&file) > 0 {
                return Err(internal_server_error!(
                    "Cannot remove file {} because it is in use",
                    path.display()
                ));
            }
        }

        cache.remove(path);
        self.backpack.read()?.remove(path)?;

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
        Ok(self.backpack.read()?.read_dir(path)?)
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
                let mut lock = file.write()?;
                if lock.mode() == &AccessMode::ReadWrite && !lock.is_synced() {
                    if let Err(err) = lock.sync_all() {
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

    pub fn try_exists(&self, path: &PathBuf) -> Result<bool, ReductError> {
        let backpack = self.backpack.read()?;
        Ok(backpack.try_exists(path)?)
    }

    pub fn force_sync_all(&self) {
        Self::sync_rw_and_unused_files(&self.backpack, &self.cache, Duration::from_secs(0), true);
    }

    /// Saves a file descriptor in the cache and syncs any discarded files.
    ///
    /// We need to make sure that we sync all files that were discarded
    fn save_in_cache_and_sync_discarded(
        path: PathBuf,
        cache: &mut RwLockWriteGuard<Cache<PathBuf, FileRc>>,
        file: &Arc<RwLock<File>>,
    ) -> () {
        let discarded_files = cache.insert(path.clone(), Arc::clone(file));

        for (_, file) in discarded_files {
            if let Err(err) = file.write().unwrap().sync_all() {
                warn!("Failed to sync file {:?}: {}", path, err);
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
                let file_ref = cache
                    .write_or_create(&file_path, SeekFrom::Start(0))
                    .unwrap()
                    .upgrade()
                    .unwrap();
                file_ref.write().unwrap().write_all(b"test").unwrap();

                assert!(
                    !cache
                        .cache
                        .write()
                        .unwrap()
                        .get(&file_path)
                        .unwrap()
                        .read()
                        .unwrap()
                        .is_synced(),
                    "File should not be synced initially"
                );
            }

            // Wait for the sync worker to run
            sleep(Duration::from_millis(250));

            assert!(
                cache
                    .cache
                    .write()
                    .unwrap()
                    .get(&file_path)
                    .unwrap()
                    .read()
                    .unwrap()
                    .is_synced(),
                "File should be synced after sync worker runs"
            );
        }

        #[rstest]
        fn test_not_sync_used_files(cache: FileCache, tmp_dir: PathBuf) {
            let file_path = tmp_dir.join("test_not_sync_unused_files.txt");
            let file_ref = cache
                .write_or_create(&file_path, SeekFrom::Start(0))
                .unwrap()
                .upgrade()
                .unwrap();
            file_ref.write().unwrap().write_all(b"test").unwrap();

            // Wait for the sync worker to run
            sleep(Duration::from_millis(250));

            assert!(
                !cache
                    .cache
                    .write()
                    .unwrap()
                    .get(&file_path)
                    .unwrap()
                    .read()
                    .unwrap()
                    .is_synced(),
                "File should not be synced after sync worker runs"
            );
        }
    }

    #[fixture]
    fn cache() -> FileCache {
        let cache = FileCache::new(2, Duration::from_millis(100), Duration::from_millis(100));
        cache.set_storage_backend(
            Backend::builder()
                .local_data_path(tempfile::tempdir().unwrap().keep().to_str().unwrap())
                .try_build()
                .unwrap(),
        );
        cache
    }

    #[fixture]
    fn tmp_dir() -> PathBuf {
        tempfile::tempdir().unwrap().keep()
    }
}
