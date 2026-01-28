// Copyright 2023-2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::backend::file::{AccessMode, File};
use crate::backend::{Backend, ObjectMetadata};
use crate::core::cache::Cache;
use crate::core::sync::{AsyncRwLock, RwLock};
use log::{debug, warn};
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::fs;
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::sync::{OwnedRwLockWriteGuard, RwLockWriteGuard};
use tokio::time::sleep;

const FILE_CACHE_MAX_SIZE: usize = 1024;
const FILE_CACHE_TIME_TO_LIVE: Duration = Duration::from_secs(60);

const FILE_CACHE_SYNC_INTERVAL: Duration = Duration::from_millis(10);

pub(crate) static FILE_CACHE: LazyLock<FileCache> = LazyLock::new(|| {
    #[allow(unused_mut)]
    let mut cache = FileCache::new(
        FILE_CACHE_MAX_SIZE,
        FILE_CACHE_TIME_TO_LIVE,
        FILE_CACHE_SYNC_INTERVAL,
    );

    #[cfg(test)]
    {
        use futures::executor;

        // Use an isolated filesystem backend for tests to avoid relying on
        // other tests to initialise the global cache.
        let temp_dir = tempfile::tempdir()
            .expect("Failed to create temporary directory for FILE_CACHE")
            .keep();
        executor::block_on(async {
            let mut backend = cache.backend.write().await.unwrap();
            *backend = (Backend::builder().local_data_path(temp_dir).try_build())
                .await
                .expect("Failed to initialise FILE_CACHE backend for tests");
        });
    }

    cache
});

pub(crate) type FileLock = Arc<AsyncRwLock<File>>;
pub(crate) type FileGuard = OwnedRwLockWriteGuard<File>;

/// A cache to keep file descriptors open
///
/// This optimization is needed for network file systems because opening
/// and closing files for writing causes synchronization overhead.
///
/// Additionally, it periodically syncs files to disk to ensure data integrity.
pub(crate) struct FileCache {
    cache: Arc<AsyncRwLock<Cache<PathBuf, FileLock>>>,
    stop_sync_worker: Arc<AtomicBool>,
    backend: Arc<AsyncRwLock<Backend>>,
    sync_interval: Arc<RwLock<Duration>>,
    read_only: Arc<AtomicBool>,
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
        let cache = Arc::new(AsyncRwLock::new(Cache::<PathBuf, FileLock>::new(
            max_size, ttl,
        )));
        let cache_clone = Arc::clone(&cache);
        let stop_sync_worker = Arc::new(AtomicBool::new(false));
        let stop_sync_worker_clone = Arc::clone(&stop_sync_worker);
        let backpack = Arc::new(AsyncRwLock::new(Backend::default()));
        let backpack_clone = Arc::clone(&backpack);
        let sync_interval = Arc::new(RwLock::new(sync_interval));
        let sync_interval_clone = Some(Arc::clone(&sync_interval));
        let read_only = Arc::new(AtomicBool::new(false));
        let read_only_clone = Arc::clone(&read_only);

        tokio::spawn(async move {
            // Periodically sync files from cache to disk
            while !stop_sync_worker.load(Ordering::Relaxed) {
                sleep(Duration::from_millis(100)).await;

                if let Err(err) = Self::sync_rw_and_unused_files(
                    &read_only_clone,
                    &backpack_clone,
                    &cache,
                    &sync_interval_clone,
                )
                .await
                {
                    warn!(
                        "Failed to sync files from descriptor cache to disk: {}",
                        err
                    );
                }
            }
        });

        FileCache {
            cache: cache_clone,
            stop_sync_worker: stop_sync_worker_clone,
            backend: backpack,
            sync_interval,
            read_only,
        }
    }

    async fn sync_rw_and_unused_files(
        read_only: &Arc<AtomicBool>,
        backend: &Arc<AsyncRwLock<Backend>>,
        cache: &Arc<AsyncRwLock<Cache<PathBuf, FileLock>>>,
        sync_interval: &Option<Arc<RwLock<Duration>>>,
    ) -> Result<(), ReductError> {
        if read_only.load(Ordering::Relaxed) {
            return Ok(());
        }

        let mut cache = cache.write().await?;

        let force = sync_interval.is_none();
        let sync_interval = sync_interval
            .as_ref()
            .map_or(FILE_CACHE_SYNC_INTERVAL, |si| *si.read_blocking());
        let invalidated_files = backend
            .read()
            .await?
            .invalidate_locally_cached_files()
            .await;
        for path in invalidated_files {
            if let Some(file) = cache.remove(&path) {
                if let Err(err) = file.write_owned().await?.sync_all().await {
                    warn!("Failed to sync invalidated file {:?}: {}", path, err);
                }
            }

            tokio::fs::remove_file(&path).await.ok();
            debug!("Removed invalidated file {:?} from cache and storage", path);
        }

        for (path, file) in cache.iter_mut() {
            let mut file_lock = if force {
                file.write().await?
            } else {
                let Some(file) = file.try_write() else {
                    continue;
                };
                file
            };

            // Sync only writeable files that are not synced yet
            // and are not used by other threads
            if file_lock.mode() != &AccessMode::ReadWrite
                || file_lock.is_synced()
                || (!force && file_lock.last_synced().elapsed() < sync_interval)
            {
                continue;
            }

            if let Err(err) = file_lock.sync_all().await {
                warn!("Failed to sync file {}: {}", path.display(), err);
                continue;
            }
        }

        Ok(())
    }

    async fn open_read_file(&self, path: &PathBuf) -> Result<Arc<AsyncRwLock<File>>, ReductError> {
        let file = self
            .backend
            .read()
            .await?
            .open_options()
            .read(true)
            .ignore_write(self.read_only.load(Ordering::Relaxed))
            .open(path)
            .await?;
        let arc = Arc::new(AsyncRwLock::new(file));
        Ok(arc)
    }

    async fn open_write_file(
        &self,
        path: &PathBuf,
        create: bool,
    ) -> Result<Arc<AsyncRwLock<File>>, ReductError> {
        let file = self
            .backend
            .read()
            .await?
            .open_options()
            .create(create)
            .write(true)
            .ignore_write(self.read_only.load(Ordering::Relaxed))
            .read(true)
            .open(path)
            .await?;
        let arc = Arc::new(AsyncRwLock::new(file));
        Ok(arc)
    }

    async fn insert_file_cached(
        &self,
        path: &PathBuf,
        file: Arc<AsyncRwLock<File>>,
    ) -> Result<(usize, usize), ReductError> {
        let discarded = self
            .cache
            .write()
            .await?
            .insert(path.clone(), Arc::clone(&file));

        let mut synced_count = 0usize;
        let mut discarded_count = 0usize;
        for (path, file) in discarded {
            if let Some(mut lock) = file.try_write_owned() {
                discarded_count += 1;
                if lock.mode() == &AccessMode::ReadWrite && !lock.is_synced() {
                    lock.sync_all().await.unwrap_or_else(|err| {
                        warn!("Failed to sync discarded file {:?}: {}", path, err);
                    });
                    synced_count += 1;
                }
            } else {
                // return the file to the cache if it is still in use
                self.cache.write().await?.insert(path, Arc::clone(&file));
                continue;
            }
        }

        Ok((discarded_count, synced_count))
    }

    /// Set the storage backend
    pub async fn set_storage_backend(&self, backpack: Backend) {
        let mut backend = self.backend.write().await.unwrap();
        *backend = backpack;
    }

    /// Set sync interval
    pub fn set_sync_interval(&self, interval: Duration) {
        *self.sync_interval.write_blocking() = interval;
    }

    /// Set read-only mode
    pub fn set_read_only(&self, read_only: bool) {
        self.read_only.store(read_only, Ordering::Relaxed);
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
    pub async fn read(&self, path: &PathBuf, pos: SeekFrom) -> Result<FileGuard, ReductError> {
        let file = {
            let file = self.cache.read().await?.get(path).cloned();
            if let Some(file) = file {
                Arc::clone(&file)
            } else {
                let file = self.open_read_file(path).await?;
                self.insert_file_cached(path, file.clone()).await?;
                file
            }
        };

        let mut lock = file.write_owned().await?;
        if pos != SeekFrom::Current(0) {
            lock.seek(pos)?;
        }

        lock.access().await?;
        Ok(lock)
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
    ) -> Result<FileGuard, ReductError> {
        let file = {
            let file = self.cache.read().await?.get(path).cloned();
            if let Some(file) = file {
                let Ok(lock) = file.read().await else {
                    Err(internal_server_error!(
                        "Failed to acquire read lock for file {}",
                        path.display()
                    ))?
                };
                if lock.mode() == &AccessMode::ReadWrite {
                    Arc::clone(&file)
                } else {
                    drop(lock);
                    let file = self.open_write_file(path, false).await?;
                    self.insert_file_cached(path, file.clone()).await?;
                    file
                }
            } else {
                let file = self.open_write_file(path, true).await?;
                self.insert_file_cached(path, file.clone()).await?;
                file
            }
        };

        let mut lock = file.write_owned().await?;
        if pos != SeekFrom::Current(0) {
            lock.seek(pos)?;
        }

        lock.access().await?;
        Ok(lock)
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
        if self.read_only.load(Ordering::Relaxed) {
            return Ok(());
        }

        // We hold the lock to ensure that no other operations are being performed on the file
        let _lock = {
            let mut cache = self.cache.write().await?;
            if let Some(file) = cache.remove(path) {
                if let Some(lock) = file.try_write_owned() {
                    Some(lock)
                } else {
                    cache.insert(path.clone(), file);
                    return Err(internal_server_error!(
                        "Cannot remove file {} because it is in use",
                        path.display()
                    ));
                }
            } else {
                None
            }
        };

        let backend = self.backend.read().await?.clone();
        backend.remove(path).await?;

        Ok(())
    }

    pub async fn remove_dir(&self, path: &PathBuf) -> Result<(), ReductError> {
        if self.read_only.load(Ordering::Relaxed) {
            return Ok(());
        }

        let mut cache = self.cache.write().await?;
        self.discard_recursive_with_locked_cache(path, &mut cache)
            .await?;
        if path.try_exists()? {
            let backend = self.backend.read().await?.clone();
            backend.remove_dir_all(path).await?;
        }

        Ok(())
    }

    /// Discards all files in the cache that are under the specified path.
    ///
    /// This function iterates through the cache and removes all file descriptors
    /// whose paths start with the specified `path`. If a file is in read-write mode
    /// and has not been synced, it attempts to sync the file before removing it from the cache.
    ///
    pub async fn discard_recursive(&self, path: &PathBuf) -> Result<(), ReductError> {
        let mut cache = self.cache.write().await?;
        self.discard_recursive_with_locked_cache(path, &mut cache)
            .await
    }

    /// We need the method to lock the cache only once across multiple calls so that we prevent race conditions
    async fn discard_recursive_with_locked_cache(
        &self,
        path: &PathBuf,
        cache: &mut RwLockWriteGuard<'_, Cache<PathBuf, FileLock>>,
    ) -> Result<(), ReductError> {
        let normalized_path = fs::canonicalize(path).unwrap_or_else(|_| path.clone());
        let files_to_remove = cache
            .keys()
            .iter()
            .filter(|file_path| {
                file_path.starts_with(path)
                    || fs::canonicalize(file_path)
                        .map(|p| p.starts_with(&normalized_path))
                        .unwrap_or(false)
            })
            .map(|file_path| (*file_path).clone())
            .collect::<Vec<PathBuf>>();

        for file_path in files_to_remove {
            if let Some(file) = cache.remove(&file_path) {
                let mut lock = file.write_owned().await?;
                if lock.mode() == &AccessMode::ReadWrite && !lock.is_synced() {
                    if let Err(err) = lock.sync_all().await {
                        warn!("Failed to sync file {}: {}", file_path.display(), err);
                    }
                }
            }

            self.backend
                .write()
                .await?
                .remove_from_local_cache(&file_path)
                .await?;
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
    pub async fn rename(&self, old_path: &PathBuf, new_path: &PathBuf) -> Result<(), ReductError> {
        if self.read_only.load(Ordering::Relaxed) {
            return Ok(());
        }

        // important to keep cache preventing race conditions
        let mut cache = self.cache.write().await?;
        self.discard_recursive_with_locked_cache(old_path, &mut cache)
            .await?;
        cache.remove(old_path);

        let backend = self.backend.read().await?.clone();
        backend.rename(old_path, new_path).await?;
        Ok(())
    }

    pub async fn try_exists(&self, path: &PathBuf) -> Result<bool, ReductError> {
        let backpack = self.backend.read().await?;
        Ok(backpack.try_exists(path).await?)
    }

    pub async fn get_stats(&self, path: &PathBuf) -> Result<Option<ObjectMetadata>, ReductError> {
        let backpack = self.backend.read().await?;
        Ok(backpack.get_stats(path).await?)
    }

    pub async fn force_sync_all(&self) -> Result<(), ReductError> {
        Self::sync_rw_and_unused_files(&self.read_only, &self.backend, &self.cache, &None).await
    }

    pub async fn create_dir_all(&self, path: &PathBuf) -> Result<(), ReductError> {
        if self.read_only.load(Ordering::Relaxed) {
            return Ok(());
        }

        self.backend.read().await?.create_dir_all(path).await?;
        Ok(())
    }

    pub async fn read_dir(&self, path: &PathBuf) -> Result<Vec<PathBuf>, ReductError> {
        Ok(self.backend.read().await?.read_dir(path).await?)
    }

    /// Remove a file from the backend's local cache, if any.
    pub async fn invalidate_local_cache_file(&self, path: &PathBuf) -> Result<(), ReductError> {
        self.discard_recursive(path).await?;
        self.backend
            .read()
            .await?
            .remove_from_local_cache(path)
            .await?;
        Ok(())
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

    use futures::executor;
    use mockall::mock;
    use std::fs;
    use std::io::Write;

    use rstest::*;
    use std::io::Read;

    mock! {
        pub StorageBackend {}

        #[async_trait::async_trait]
        impl crate::backend::StorageBackend for StorageBackend {
            fn path(&self) -> &PathBuf;
            async fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()>;
            async fn remove(&self, path: &std::path::Path) -> std::io::Result<()>;
            async fn remove_dir_all(&self, path: &std::path::Path) -> std::io::Result<()>;
            async fn create_dir_all(&self, path: &std::path::Path) -> std::io::Result<()>;
            async fn read_dir(&self, path: &std::path::Path) -> std::io::Result<Vec<PathBuf>>;
            async fn try_exists(&self, path: &std::path::Path) -> std::io::Result<bool>;
            async fn upload(&self, path: &std::path::Path) -> std::io::Result<()>;
            async fn download(&self, path: &std::path::Path) -> std::io::Result<()>;
            async fn update_local_cache(&self, path: &std::path::Path, mode: &AccessMode) -> std::io::Result<()>;
            async fn invalidate_locally_cached_files(&self) -> Vec<PathBuf>;
            async fn get_stats(&self, path: &std::path::Path) -> std::io::Result<Option<crate::backend::ObjectMetadata>>;
            async fn remove_from_local_cache(&self, path: &std::path::Path) -> std::io::Result<()>;
        }
    }

    fn build_backend(configure: impl FnOnce(&mut MockStorageBackend)) -> Backend {
        let mut backend = MockStorageBackend::new();
        configure(&mut backend);
        Backend::from_backend(Box::new(backend))
    }

    fn expect_path(mock: &mut MockStorageBackend, root: &PathBuf, times: usize) {
        mock.expect_path().return_const(root.clone()).times(times);
    }

    fn expect_try_exists(
        mock: &mut MockStorageBackend,
        path: &PathBuf,
        exists: bool,
        times: usize,
    ) {
        let expected = path.clone();
        mock.expect_try_exists()
            .withf(move |path| path == expected.as_path())
            .returning(move |_| Ok(exists))
            .times(times);
    }

    fn expect_upload(mock: &mut MockStorageBackend, path: &PathBuf, times: usize) {
        let expected = path.clone();
        mock.expect_upload()
            .withf(move |path| path == expected.as_path())
            .returning(|_| Ok(()))
            .times(times);
    }

    fn expect_update_local_cache(
        mock: &mut MockStorageBackend,
        path: &PathBuf,
        mode: AccessMode,
        times: usize,
    ) {
        let expected = path.clone();
        mock.expect_update_local_cache()
            .withf(move |path, mode_arg| path == expected.as_path() && mode_arg == &mode)
            .returning(|_, _| Ok(()))
            .times(times);
    }

    fn expect_remove(mock: &mut MockStorageBackend, path: &PathBuf, times: usize) {
        let expected = path.clone();
        mock.expect_remove()
            .withf(move |path| path == expected.as_path())
            .returning(|path| std::fs::remove_file(path))
            .times(times);
    }

    fn expect_remove_dir_all(mock: &mut MockStorageBackend, path: &PathBuf, times: usize) {
        let expected = path.clone();
        mock.expect_remove_dir_all()
            .withf(move |path| path == expected.as_path())
            .returning(|path| std::fs::remove_dir_all(path))
            .times(times);
    }

    fn expect_remove_from_local_cache(mock: &mut MockStorageBackend, path: &PathBuf, times: usize) {
        let expected = path.clone();
        mock.expect_remove_from_local_cache()
            .withf(move |path| path == expected.as_path())
            .returning(|_| Ok(()))
            .times(times);
    }

    fn build_cache(backend: Backend) -> FileCache {
        let cache = FileCache::new(2, Duration::from_millis(100), Duration::from_millis(100));
        executor::block_on(async {
            cache.set_storage_backend(backend).await;
        });
        cache.stop_sync_worker.store(true, Ordering::Relaxed);
        cache
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_read(tmp_dir: PathBuf) {
        let file_path = tmp_dir.join("test_read.txt");
        let backend = build_backend(|mock| {
            expect_path(mock, &tmp_dir, 1);
            expect_update_local_cache(mock, &file_path, AccessMode::Read, 2);
        });
        let cache = build_cache(backend);
        let mut file = fs::File::create(&file_path).unwrap();
        file.write_all(b"test").unwrap();
        file.sync_all().unwrap();
        drop(file);

        {
            let mut file_ref = cache.read(&file_path, SeekFrom::Start(0)).await.unwrap();
            let mut data = String::new();
            file_ref.read_to_string(&mut data).unwrap();
            assert_eq!(data, "test", "should read from beginning");
        }

        let mut file_ref = cache.read(&file_path, SeekFrom::End(-2)).await.unwrap();
        let mut data = String::new();
        file_ref.read_to_string(&mut data).unwrap();
        assert_eq!(data, "st", "should read last 2 bytes");
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_or_create(tmp_dir: PathBuf) {
        let file_path = tmp_dir.join("test_write_or_create.txt");
        let backend = build_backend(|mock| {
            expect_path(mock, &tmp_dir, 1);
            expect_try_exists(mock, &file_path, false, 1);
            expect_update_local_cache(mock, &file_path, AccessMode::ReadWrite, 2);
            expect_upload(mock, &file_path, 2);
        });
        let cache = build_cache(backend);

        {
            let mut file_ref = cache
                .write_or_create(&file_path, SeekFrom::Start(0))
                .await
                .unwrap();
            file_ref.write_all(b"test").unwrap();
            file_ref.sync_all().await.unwrap();
        }

        assert_eq!(
            fs::read(&file_path).unwrap(),
            b"test",
            "should write to file"
        );

        let mut file_ref = cache
            .write_or_create(&file_path, SeekFrom::End(-2))
            .await
            .unwrap();
        file_ref.write_all(b"xx").unwrap();
        file_ref.sync_all().await.unwrap();

        assert_eq!(
            fs::read(&file_path).unwrap(),
            b"texx",
            "should override last 2 bytes"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove(tmp_dir: PathBuf) {
        let file_path = tmp_dir.join("test_remove.txt");
        let backend = build_backend(|mock| {
            expect_remove(mock, &file_path, 1);
        });
        let cache = build_cache(backend);
        let mut file = fs::File::create(&file_path).unwrap();
        file.write_all(b"test").unwrap();
        file.sync_all().unwrap();
        drop(file);

        cache.remove(&file_path).await.unwrap();
        assert_eq!(file_path.exists(), false);
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_used(tmp_dir: PathBuf) {
        let file_path = tmp_dir.join("test_remove_used.txt");
        let backend = build_backend(|mock| {
            expect_path(mock, &tmp_dir, 1);
            expect_try_exists(mock, &file_path, false, 1);
            expect_update_local_cache(mock, &file_path, AccessMode::ReadWrite, 1);
        });
        let cache = build_cache(backend);
        let file_path = tmp_dir.join("test_remove_used.txt");
        let _file_guard = cache
            .write_or_create(&file_path, SeekFrom::Start(0))
            .await
            .unwrap();

        let err = cache.remove(&file_path).await.unwrap_err();
        assert_eq!(
            err,
            internal_server_error!(
                "Cannot remove file {} because it is in use",
                file_path.display()
            )
        );

        assert!(file_path.exists());
    }

    #[rstest]
    #[tokio::test]
    async fn test_cache_max_size(tmp_dir: PathBuf) {
        let file_path1 = tmp_dir.join("test_cache_max_size1.txt");
        let file_path2 = tmp_dir.join("test_cache_max_size2.txt");
        let file_path3 = tmp_dir.join("test_cache_max_size3.txt");
        let backend = build_backend(|mock| {
            expect_path(mock, &tmp_dir, 3);
            expect_try_exists(mock, &file_path1, false, 1);
            expect_try_exists(mock, &file_path2, false, 1);
            expect_try_exists(mock, &file_path3, false, 1);
            expect_update_local_cache(mock, &file_path1, AccessMode::ReadWrite, 1);
            expect_update_local_cache(mock, &file_path2, AccessMode::ReadWrite, 1);
            expect_update_local_cache(mock, &file_path3, AccessMode::ReadWrite, 1);
        });
        let cache = build_cache(backend);

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

        let inner_cache = cache.cache.write().await.unwrap();
        let has_file1 = inner_cache.get(&file_path1).is_some();
        drop(inner_cache);
        assert!(!has_file1);
    }

    #[rstest]
    #[tokio::test]
    async fn test_cache_keeps_entries_with_weak_refs(tmp_dir: PathBuf) {
        let cache = {
            let cache = FileCache::new(1, Duration::from_secs(60), Duration::from_millis(100));
            let backend = build_backend(|mock| {
                expect_path(mock, &tmp_dir, 2);
                expect_try_exists(mock, &tmp_dir.join("test_cache_keep_weak1.txt"), false, 1);
                expect_try_exists(mock, &tmp_dir.join("test_cache_keep_weak2.txt"), false, 1);
                expect_update_local_cache(
                    mock,
                    &tmp_dir.join("test_cache_keep_weak1.txt"),
                    AccessMode::ReadWrite,
                    1,
                );
                expect_update_local_cache(
                    mock,
                    &tmp_dir.join("test_cache_keep_weak2.txt"),
                    AccessMode::ReadWrite,
                    1,
                );
            });
            cache.set_storage_backend(backend).await;
            cache.stop_sync_worker.store(true, Ordering::Relaxed);
            cache
        };

        let file_path1 = tmp_dir.join("test_cache_keep_weak1.txt");
        let weak_ref = cache
            .write_or_create(&file_path1, SeekFrom::Start(0))
            .await
            .unwrap();

        let file_path2 = tmp_dir.join("test_cache_keep_weak2.txt");
        cache
            .write_or_create(&file_path2, SeekFrom::Start(0))
            .await
            .unwrap();

        assert_eq!(cache.cache.read().await.unwrap().len(), 1);
        drop(weak_ref);
    }

    #[rstest]
    #[tokio::test]
    async fn test_cache_ttl(tmp_dir: PathBuf) {
        let file_path1 = tmp_dir.join("test_cache_max_size1.txt");
        let file_path2 = tmp_dir.join("test_cache_max_size2.txt");
        let file_path3 = tmp_dir.join("test_cache_max_size3.txt");
        let backend = build_backend(|mock| {
            expect_path(mock, &tmp_dir, 3);
            expect_try_exists(mock, &file_path1, false, 1);
            expect_try_exists(mock, &file_path2, false, 1);
            expect_try_exists(mock, &file_path3, false, 1);
            expect_update_local_cache(mock, &file_path1, AccessMode::ReadWrite, 1);
            expect_update_local_cache(mock, &file_path2, AccessMode::ReadWrite, 1);
            expect_update_local_cache(mock, &file_path3, AccessMode::ReadWrite, 1);
        });
        let cache = build_cache(backend);

        cache
            .write_or_create(&file_path1, SeekFrom::Start(0))
            .await
            .unwrap();
        cache
            .write_or_create(&file_path2, SeekFrom::Start(0))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await;

        cache
            .write_or_create(&file_path3, SeekFrom::Start(0))
            .await
            .unwrap(); // should remove the file_path1 descriptor

        let inner_cache = cache.cache.write().await.unwrap();
        assert_eq!(inner_cache.len(), 1);
        assert!(inner_cache.get(&file_path1).is_none());
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_dir(tmp_dir: PathBuf) {
        let file_path1 = tmp_dir.join("test_remove_dir1.txt");
        let file_path2 = tmp_dir.join("test_remove_dir2.txt");
        let backend = build_backend(|mock| {
            expect_path(mock, &tmp_dir, 2);
            expect_try_exists(mock, &file_path1, false, 1);
            expect_try_exists(mock, &file_path2, false, 1);
            expect_update_local_cache(mock, &file_path1, AccessMode::ReadWrite, 1);
            expect_update_local_cache(mock, &file_path2, AccessMode::ReadWrite, 1);
            expect_remove_from_local_cache(mock, &file_path1, 1);
            expect_remove_from_local_cache(mock, &file_path2, 1);
            expect_remove_dir_all(mock, &tmp_dir, 1);
        });
        let cache = build_cache(backend);
        cache
            .write_or_create(&file_path1, SeekFrom::Start(0))
            .await
            .unwrap();
        cache
            .write_or_create(&file_path2, SeekFrom::Start(0))
            .await
            .unwrap();

        cache.remove_dir(&tmp_dir).await.unwrap();

        assert!(!tmp_dir.exists());
    }

    mod insert_file_cached {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_insert_file_cached_file_in_use(
            tmp_dir: PathBuf,
            file_path_1: PathBuf,
            file_path_2: PathBuf,
        ) {
            let backend = build_backend(|mock| {
                expect_path(mock, &tmp_dir, 2);
            });
            let small_cache = build_small_cache(backend);
            let file = small_cache
                .open_write_file(&file_path_1, false)
                .await
                .unwrap();
            let _guard = file.read().await.unwrap();
            small_cache
                .insert_file_cached(&file_path_1, Arc::clone(&file))
                .await
                .unwrap();

            let file2 = small_cache
                .open_write_file(&file_path_2, false)
                .await
                .unwrap();
            let (discarded, synced) = small_cache
                .insert_file_cached(&file_path_2, file2)
                .await
                .unwrap();

            assert_eq!((discarded, synced), (0, 0));
        }

        #[rstest]
        #[tokio::test]
        async fn test_insert_file_cached_missing_on_disk(
            tmp_dir: PathBuf,
            file_path_1: PathBuf,
            file_path_2: PathBuf,
        ) {
            let backend = build_backend(|mock| {
                expect_path(mock, &tmp_dir, 2);
            });
            let small_cache = build_small_cache(backend);
            let file = small_cache
                .open_write_file(&file_path_1, false)
                .await
                .unwrap();
            small_cache
                .insert_file_cached(&file_path_1, file)
                .await
                .unwrap();
            fs::remove_file(&file_path_1).unwrap();
            assert!(!file_path_1.exists());

            let file2 = small_cache
                .open_write_file(&file_path_2, false)
                .await
                .unwrap();
            let (discarded, synced) = small_cache
                .insert_file_cached(&file_path_2, file2)
                .await
                .unwrap();

            assert_eq!((discarded, synced), (1, 0));
        }

        #[rstest]
        #[tokio::test]
        async fn test_insert_file_cached_no_sync_needed(
            tmp_dir: PathBuf,
            file_path_1: PathBuf,
            file_path_2: PathBuf,
        ) {
            let backend = build_backend(|mock| {
                expect_path(mock, &tmp_dir, 2);
            });
            let small_cache = build_small_cache(backend);
            let file = small_cache
                .open_write_file(&file_path_1, false)
                .await
                .unwrap();
            small_cache
                .insert_file_cached(&file_path_1, file)
                .await
                .unwrap();

            let file2 = small_cache
                .open_write_file(&file_path_2, false)
                .await
                .unwrap();
            let (discarded, synced) = small_cache
                .insert_file_cached(&file_path_2, file2)
                .await
                .unwrap();

            assert_eq!((discarded, synced), (1, 0));
        }

        #[rstest]
        #[tokio::test]
        async fn test_insert_file_cached_read_mode(
            tmp_dir: PathBuf,
            file_path_1: PathBuf,
            file_path_2: PathBuf,
        ) {
            let backend = build_backend(|mock| {
                expect_path(mock, &tmp_dir, 2);
            });
            let small_cache = build_small_cache(backend);
            let file = small_cache.open_read_file(&file_path_1).await.unwrap();
            small_cache
                .insert_file_cached(&file_path_1, file)
                .await
                .unwrap();

            let file2 = small_cache
                .open_write_file(&file_path_2, false)
                .await
                .unwrap();
            let (discarded, synced) = small_cache
                .insert_file_cached(&file_path_2, file2)
                .await
                .unwrap();

            assert_eq!((discarded, synced), (1, 0));
        }

        #[rstest]
        #[tokio::test]
        async fn test_insert_file_cached_sync_unsynced_file(
            tmp_dir: PathBuf,
            file_path_1: PathBuf,
            file_path_2: PathBuf,
        ) {
            let backend = build_backend(|mock| {
                expect_path(mock, &tmp_dir, 2);
                expect_upload(mock, &file_path_1, 1);
            });
            let small_cache = build_small_cache(backend);
            let file = small_cache
                .open_write_file(&file_path_1, false)
                .await
                .unwrap();
            // Write to the file to make it unsynced
            file.write().await.unwrap().write_all(b"new data").unwrap();
            small_cache
                .insert_file_cached(&file_path_1, file)
                .await
                .unwrap();

            let file2 = small_cache
                .open_write_file(&file_path_2, false)
                .await
                .unwrap();
            let (discarded, synced) = small_cache
                .insert_file_cached(&file_path_2, file2)
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;

            assert_eq!((discarded, synced), (1, 1));
        }

        fn build_small_cache(backend: Backend) -> FileCache {
            let cache = FileCache::new(1, Duration::from_secs(60), Duration::from_secs(60));
            executor::block_on(async {
                cache.set_storage_backend(backend).await;
            });
            cache.stop_sync_worker.store(true, Ordering::Relaxed);
            cache
        }

        #[fixture]
        fn file_path_1(tmp_dir: PathBuf) -> PathBuf {
            let path = tmp_dir.join("test_file_1.txt");
            fs::write(&path, b"test").unwrap();
            path
        }

        #[fixture]
        fn file_path_2(tmp_dir: PathBuf) -> PathBuf {
            let path = tmp_dir.join("test_file_2.txt");
            fs::write(&path, b"test").unwrap();
            path
        }
    }

    mod sync_rw_and_unused_files {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_sync_unused_files(tmp_dir: PathBuf) {
            let file_path = tmp_dir.join("test_sync_rw_and_unused_files.txt");
            let backend = build_backend(|mock| {
                expect_path(mock, &tmp_dir, 1);
                expect_try_exists(mock, &file_path, false, 1);
                expect_update_local_cache(mock, &file_path, AccessMode::ReadWrite, 1);
                mock.expect_invalidate_locally_cached_files()
                    .returning(Vec::new)
                    .times(1);
                expect_upload(mock, &file_path, 1);
            });
            let cache = build_cache(backend);
            {
                let mut file_ref = cache
                    .write_or_create(&file_path, SeekFrom::Start(0))
                    .await
                    .unwrap();
                file_ref.write_all(b"test").unwrap();
            }

            cache.force_sync_all().await.unwrap();
            assert!(cache.cache.write().await.unwrap().get(&file_path).is_some());
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn test_not_sync_used_files(tmp_dir: PathBuf) {
            let file_path = tmp_dir.join("test_not_sync_unused_files.txt");
            let backend = build_backend(|mock| {
                expect_path(mock, &tmp_dir, 1);
                expect_try_exists(mock, &file_path, false, 1);
                expect_update_local_cache(mock, &file_path, AccessMode::ReadWrite, 1);
            });
            let cache = build_cache(backend);
            {
                let mut file_ref = cache
                    .write_or_create(&file_path, SeekFrom::Start(0))
                    .await
                    .unwrap();
                file_ref.write_all(b"test").unwrap();
            }

            assert!(!cache
                .cache
                .write()
                .await
                .unwrap()
                .get(&file_path)
                .unwrap()
                .read()
                .await
                .unwrap()
                .is_synced());
        }

        #[rstest]
        #[tokio::test]
        async fn test_remove_invalidated_files(tmp_dir: PathBuf) {
            let file_path = tmp_dir.join("test_invalidated_file.txt");
            let backend = build_backend(|mock| {
                let invalidated_path = file_path.clone();
                mock.expect_invalidate_locally_cached_files()
                    .returning(move || vec![invalidated_path.clone()])
                    .times(1);
            });
            let cache = build_cache(backend);
            fs::write(&file_path, b"test").unwrap();

            cache.force_sync_all().await.unwrap();
            assert!(!file_path.exists(), "invalidated file should be removed");
        }
    }

    mod test_read_only {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_write(tmp_dir: PathBuf) {
            let file_path = tmp_dir.join("test_read_only_mode.txt");
            let backend = build_backend(|mock| {
                expect_path(mock, &tmp_dir, 1);
                expect_update_local_cache(mock, &file_path, AccessMode::Read, 1);
            });
            let read_only_cache = build_cache(backend);
            read_only_cache.set_read_only(true);
            fs::write(&file_path, b"test").unwrap();

            let mut file = read_only_cache
                .read(&file_path, SeekFrom::Start(0))
                .await
                .unwrap();
            let mut data = String::new();
            file.read_to_string(&mut data).unwrap();
            assert_eq!(data, "test");

            file.write_all(b"new data").unwrap();
        }

        #[rstest]
        #[tokio::test]
        async fn test_remove(tmp_dir: PathBuf) {
            let backend = build_backend(|_mock| {});
            let read_only_cache = build_cache(backend);
            read_only_cache.set_read_only(true);
            let file_path = tmp_dir.join("test_remove_in_read_only_mode.txt");
            fs::write(&file_path, b"test").unwrap();

            read_only_cache.remove(&file_path).await.unwrap();

            assert_eq!(
                file_path.exists(),
                true,
                "file should not be removed in read-only mode"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_rename(tmp_dir: PathBuf) {
            let backend = build_backend(|_mock| {});
            let read_only_cache = build_cache(backend);
            read_only_cache.set_read_only(true);
            let old_file_path = tmp_dir.join("test_rename_in_read_only_mode_old.txt");
            let new_file_path = tmp_dir.join("test_rename_in_read_only_mode_new.txt");

            fs::write(&old_file_path, b"test").unwrap();

            read_only_cache
                .rename(&old_file_path, &new_file_path)
                .await
                .unwrap();

            assert_eq!(
                old_file_path.exists(),
                true,
                "old file should not be renamed in read-only mode"
            );
            assert_eq!(
                new_file_path.exists(),
                false,
                "new file should not be created in read-only mode"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_create_dir(tmp_dir: PathBuf) {
            let backend = build_backend(|_mock| {});
            let read_only_cache = build_cache(backend);
            read_only_cache.set_read_only(true);
            let dir_path = tmp_dir.join("test_create_dir_in_read_only_mode");

            read_only_cache.create_dir_all(&dir_path).await.unwrap();

            assert_eq!(
                dir_path.exists(),
                false,
                "directory should not be created in read-only mode"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_remove_dir(tmp_dir: PathBuf) {
            let backend = build_backend(|_mock| {});
            let read_only_cache = build_cache(backend);
            read_only_cache.set_read_only(true);
            let dir_path = tmp_dir.join("test_remove_dir_in_read_only_mode");
            fs::create_dir_all(&dir_path).unwrap();

            read_only_cache.remove_dir(&dir_path).await.unwrap();

            assert_eq!(
                dir_path.exists(),
                true,
                "directory should not be removed in read-only mode"
            );
        }
    }

    #[fixture]
    fn tmp_dir() -> PathBuf {
        tempfile::tempdir().unwrap().keep()
    }
}
