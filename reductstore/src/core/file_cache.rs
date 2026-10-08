// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::backend::file::{AccessMode, File};
use crate::backend::{Backend, ObjectMetadata};
use crate::core::cache::Cache;
use crate::core::sync::{AsyncRwLock, RwLock};
use log::{debug, warn};
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{ErrorKind, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedRwLockWriteGuard, RwLockWriteGuard};
use tokio::time::sleep;

pub(crate) const FILE_CACHE_MAX_SIZE: usize = 512;
pub(crate) const FILE_CACHE_TIME_TO_LIVE: Duration = Duration::from_secs(60);

pub(crate) const FILE_CACHE_SYNC_INTERVAL: Duration = Duration::from_millis(10);
const FILE_CACHE_SYNC_BATCH_SIZE: usize = 16;

pub(crate) type FileLock = Arc<AsyncRwLock<File>>;
pub(crate) type FileGuard = OwnedRwLockWriteGuard<File>;

type BatchId = u64;

#[derive(Default)]
struct BatchRegistry {
    next_id: BatchId,
    active: HashMap<BatchId, ActiveBatch>,
    path_owners: HashMap<PathBuf, BatchId>,
}

struct ActiveBatch {
    dirty_files: HashMap<PathBuf, u64>,
    pending_deletes: HashSet<PathBuf>,
    state: BatchState,
}

#[derive(Clone, Copy)]
enum BatchState {
    Active,
    Failed,
}

/// Identifies an active batch. A token can be cloned for cooperating writers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BatchToken {
    id: BatchId,
}

/// Owns synchronization and completion of one batch.
pub(crate) struct FileBatch {
    token: BatchToken,
    cache: Arc<AsyncRwLock<Cache<PathBuf, FileLock>>>,
    backend: Arc<AsyncRwLock<Backend>>,
    batches: Arc<AsyncRwLock<BatchRegistry>>,
}

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
    batches: Arc<AsyncRwLock<BatchRegistry>>,
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
    pub(crate) fn new(max_size: usize, ttl: Duration, sync_interval: Duration) -> Self {
        let cache = Arc::new(AsyncRwLock::new(Cache::<PathBuf, FileLock>::new(
            max_size, ttl,
        )));
        let cache_clone = Arc::clone(&cache);
        let stop_sync_worker = Arc::new(AtomicBool::new(false));
        let stop_sync_worker_clone = Arc::clone(&stop_sync_worker);
        let backpack = Arc::new(AsyncRwLock::new(Backend::default()));
        let backpack_clone = Arc::clone(&backpack);
        let batches = Arc::new(AsyncRwLock::new(BatchRegistry::default()));
        let batches_clone = Arc::clone(&batches);
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
                    &batches_clone,
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
            batches,
            sync_interval,
            read_only,
        }
    }

    async fn sync_rw_and_unused_files(
        read_only: &Arc<AtomicBool>,
        backend: &Arc<AsyncRwLock<Backend>>,
        cache: &Arc<AsyncRwLock<Cache<PathBuf, FileLock>>>,
        batches: &Arc<AsyncRwLock<BatchRegistry>>,
        sync_interval: &Option<Arc<RwLock<Duration>>>,
    ) -> Result<(), ReductError> {
        if read_only.load(Ordering::Relaxed) {
            return Ok(());
        }

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
            if Self::is_batched(batches, &path).await? {
                continue;
            }
            let mut cache = cache.write().await?;
            if let Some(file) = cache.remove(&path) {
                if let Err(err) = file.write_owned().await?.sync_all().await {
                    warn!("Failed to sync invalidated file {:?}: {}", path, err);
                }
            }

            tokio::fs::remove_file(&path).await.ok();
            debug!("Removed invalidated file {:?} from cache and storage", path);
        }

        let mut files_to_sync = vec![];
        {
            let cache = cache.read().await?;
            for (path, file) in cache.iter() {
                let file_lock = if force {
                    file.read().await?
                } else {
                    let Some(file) = file.try_read() else {
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

                if !Self::is_batched(batches, path).await? {
                    files_to_sync.push((path.clone(), file.clone(), file_lock.last_synced()));
                }
            }
        }

        // For scheduled synchronization we syn only a batch of files that are the most overdue for synchronization
        if !force {
            files_to_sync.sort_by(|a, b| a.2.cmp(&b.2));
            files_to_sync.truncate(FILE_CACHE_SYNC_BATCH_SIZE);
        }

        for (path, file, _) in files_to_sync {
            let mut file_lock = if force {
                file.write().await?
            } else {
                let Some(file) = file.try_write() else {
                    continue;
                };
                file
            };

            if let Err(err) = file_lock.sync_all().await {
                // ignore not found errors, since file can be removed while we are syncing,
                // it's better than holding the file lock for too long and preventing other operations on the file
                if err.kind() != std::io::ErrorKind::NotFound {
                    debug!("Failed to sync file {}: {}", path.display(), err);
                }
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
                    if !Self::is_batched(&self.batches, &path).await? {
                        lock.sync_all().await.unwrap_or_else(|err| {
                            debug!("Failed to sync discarded file {:?}: {}", path, err);
                        });
                        synced_count += 1;
                    }
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

    /// Starts a batch. Paths are claimed explicitly by batch-aware mutations.
    pub async fn begin_batch(&self) -> Result<FileBatch, ReductError> {
        let mut batches = self.batches.write().await?;
        let id = batches.next_id;
        batches.next_id += 1;
        batches.active.insert(
            id,
            ActiveBatch {
                dirty_files: HashMap::new(),
                pending_deletes: HashSet::new(),
                state: BatchState::Active,
            },
        );
        Ok(FileBatch {
            token: BatchToken { id },
            cache: Arc::clone(&self.cache),
            backend: Arc::clone(&self.backend),
            batches: Arc::clone(&self.batches),
        })
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
        lock.set_batch_owner(self.path_owner(path).await?);
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
        lock.set_batch_owner(self.path_owner(path).await?);
        if pos != SeekFrom::Current(0) {
            lock.seek(pos)?;
        }

        lock.access().await?;
        self.mark_owned_dirty(path).await?;
        Ok(lock)
    }

    /// Writes a file as part of `batch`, claiming its exact path if necessary.
    pub async fn write_or_create_in_batch(
        &self,
        batch: &BatchToken,
        path: &PathBuf,
        pos: SeekFrom,
    ) -> Result<FileGuard, ReductError> {
        self.claim_path(batch, path).await?;
        match self.write_or_create(path, pos).await {
            Ok(mut file) => {
                file.set_batch_owner(Some(batch.id));
                Ok(file)
            }
            Err(err) => {
                self.release_claim_if_unused(batch, path).await?;
                Err(err)
            }
        }
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

        let deferred = self.defer_owned_delete(path).await?;
        let remove_from_backend = async |path| {
            let backend = self.backend.read().await?.clone();
            backend.remove(path).await?;
            Ok::<(), ReductError>(())
        };

        // We hold the lock to ensure that no other operations are being performed on the file
        let cache = self.cache.read().await?;
        if let Some(file) = cache.get(path).cloned() {
            drop(cache);
            match file.write().await {
                Ok(_) => {
                    self.cache.write().await?.remove(path);
                    if deferred {
                        tokio::fs::remove_file(path).await?;
                    } else {
                        remove_from_backend(path).await?;
                    }
                }
                Err(_) => {
                    return Err(internal_server_error!(
                        "Cannot remove file {} because it is in use",
                        path.display()
                    ))
                }
            }
        } else {
            if deferred {
                tokio::fs::remove_file(path).await?;
            } else {
                remove_from_backend(path).await?;
            }
        }

        Ok(())
    }

    /// Removes a file as part of `batch`, deferring its remote deletion.
    pub async fn remove_in_batch(
        &self,
        batch: &BatchToken,
        path: &PathBuf,
    ) -> Result<(), ReductError> {
        self.claim_path(batch, path).await?;
        self.remove(path).await
    }

    pub async fn remove_dir(&self, path: &PathBuf) -> Result<(), ReductError> {
        if self.read_only.load(Ordering::Relaxed) {
            return Ok(());
        }
        self.reject_batched_operation(path).await?;

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
            // A batch owns publication of this exact path, including its cached descriptor.
            if Self::is_batched(&self.batches, &file_path).await? {
                continue;
            }
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
        self.reject_batched_operation(old_path).await?;
        self.reject_batched_operation(new_path).await?;

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
        Self::sync_rw_and_unused_files(
            &self.read_only,
            &self.backend,
            &self.cache,
            &self.batches,
            &None,
        )
        .await
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

    pub fn stop_sync_worker(&self) {
        self.stop_sync_worker.store(true, Ordering::Relaxed);
    }

    async fn is_batched(
        batches: &Arc<AsyncRwLock<BatchRegistry>>,
        path: &Path,
    ) -> Result<bool, ReductError> {
        Ok(batches.read().await?.path_owners.contains_key(path))
    }

    async fn mark_owned_dirty(&self, path: &Path) -> Result<(), ReductError> {
        let mut batches = self.batches.write().await?;
        if let Some(owner) = batches.path_owners.get(path).copied() {
            let batch = batches
                .active
                .get_mut(&owner)
                .ok_or(internal_server_error!(
                    "File {} is owned by an inactive batch",
                    path.display()
                ))?;
            let version = batch.dirty_files.get(path).copied().unwrap_or(0) + 1;
            batch.dirty_files.insert(path.to_path_buf(), version);
            batch.pending_deletes.remove(path);
        }
        Ok(())
    }

    async fn defer_owned_delete(&self, path: &Path) -> Result<bool, ReductError> {
        let mut batches = self.batches.write().await?;
        if let Some(owner) = batches.path_owners.get(path).copied() {
            let batch = batches
                .active
                .get_mut(&owner)
                .ok_or(internal_server_error!(
                    "File {} is owned by an inactive batch",
                    path.display()
                ))?;
            batch.dirty_files.remove(path);
            batch.pending_deletes.insert(path.to_path_buf());
            return Ok(true);
        }
        Ok(false)
    }

    async fn reject_batched_operation(&self, path: &Path) -> Result<(), ReductError> {
        let batches = self.batches.read().await?;
        if batches.path_owners.contains_key(path)
            || batches
                .path_owners
                .keys()
                .any(|owned_path| owned_path.starts_with(path))
        {
            return Err(internal_server_error!(
                "Cannot modify {} because it affects an active file batch",
                path.display()
            ));
        }
        Ok(())
    }

    async fn claim_path(&self, batch: &BatchToken, path: &Path) -> Result<(), ReductError> {
        let mut batches = self.batches.write().await?;
        if !batches.active.contains_key(&batch.id) {
            return Err(internal_server_error!("File batch is no longer active"));
        }
        match batches.path_owners.get(path) {
            Some(owner) if *owner != batch.id => Err(internal_server_error!(
                "File {} is owned by another active file batch",
                path.display()
            )),
            _ => {
                batches.path_owners.insert(path.to_path_buf(), batch.id);
                Ok(())
            }
        }
    }

    async fn path_owner(&self, path: &Path) -> Result<Option<BatchId>, ReductError> {
        Ok(self.batches.read().await?.path_owners.get(path).copied())
    }

    async fn release_claim_if_unused(
        &self,
        batch: &BatchToken,
        path: &Path,
    ) -> Result<(), ReductError> {
        let mut batches = self.batches.write().await?;
        let active = batches
            .active
            .get(&batch.id)
            .ok_or(internal_server_error!("File batch is no longer active"))?;
        if !active.dirty_files.contains_key(path) && !active.pending_deletes.contains(path) {
            batches.path_owners.remove(path);
        }
        Ok(())
    }
}

impl FileBatch {
    pub fn token(&self) -> BatchToken {
        self.token.clone()
    }

    /// Synchronizes one file and waits until its remote upload completes.
    pub async fn sync_file(&mut self, path: &Path) -> Result<(), ReductError> {
        self.validate_path_owner(path).await?;
        let result = self.sync_path(path).await;
        if result.is_err() {
            self.set_state(BatchState::Failed).await?;
        }
        result
    }

    /// Synchronizes every pending upload and deferred delete in this batch.
    pub async fn sync_all(&mut self) -> Result<(), ReductError> {
        let result = self.sync_all_inner().await;
        if result.is_err() {
            self.set_state(BatchState::Failed).await?;
        }
        result
    }

    async fn sync_all_inner(&mut self) -> Result<(), ReductError> {
        let (files, deletes) = {
            let batches = self.batches.read().await?;
            let batch = self.active_batch(&batches)?;
            (
                batch
                    .dirty_files
                    .iter()
                    .map(|(path, version)| (path.clone(), *version))
                    .collect::<Vec<_>>(),
                batch.pending_deletes.iter().cloned().collect::<Vec<_>>(),
            )
        };

        for (path, _) in &files {
            self.sync_path(path).await?;
        }

        let backend = self.backend.read().await?.clone();
        for path in &deletes {
            if let Err(err) = backend.remove(path).await {
                if err.kind() != ErrorKind::NotFound {
                    return Err(err.into());
                }
            }
            let mut batches = self.batches.write().await?;
            self.active_batch_mut(&mut batches)?
                .pending_deletes
                .remove(path);
        }

        let mut batches = self.batches.write().await?;
        self.active_batch_mut(&mut batches)?.state = BatchState::Active;
        Ok(())
    }

    /// Releases owned paths after all pending remote mutations are published.
    pub async fn commit(self) -> Result<(), ReductError> {
        let paths = {
            let mut batches = self.batches.write().await?;
            let batch = batches
                .active
                .get(&self.token.id)
                .ok_or(internal_server_error!("File batch is no longer active"))?;
            if matches!(batch.state, BatchState::Failed) {
                return Err(internal_server_error!(
                    "Cannot commit failed file batch; retry synchronization first"
                ));
            }
            if !batch.dirty_files.is_empty() || !batch.pending_deletes.is_empty() {
                return Err(internal_server_error!(
                    "Cannot commit file batch with pending remote mutations"
                ));
            }
            let paths = batches
                .path_owners
                .iter()
                .filter_map(|(path, owner)| (*owner == self.token.id).then(|| path.clone()))
                .collect::<Vec<_>>();
            for path in &paths {
                batches.path_owners.remove(path);
            }
            batches.active.remove(&self.token.id);
            paths
        };
        for path in paths {
            if let Some(file) = self.cache.read().await?.get(&path).cloned() {
                file.write().await?.set_batch_owner(None);
            }
        }
        Ok(())
    }

    async fn validate_path_owner(&self, path: &Path) -> Result<(), ReductError> {
        let batches = self.batches.read().await?;
        if !batches.active.contains_key(&self.token.id) {
            return Err(internal_server_error!("File batch is no longer active"));
        }
        if batches.path_owners.get(path) != Some(&self.token.id) {
            return Err(internal_server_error!(
                "File {} is not owned by this file batch",
                path.display()
            ));
        }
        Ok(())
    }

    async fn sync_path(&mut self, path: &Path) -> Result<(), ReductError> {
        let version = {
            let batches = self.batches.read().await?;
            self.active_batch(&batches)?.dirty_files.get(path).copied()
        };
        let file = self.cache.read().await?.get(&path.to_path_buf()).cloned();
        if let Some(file) = file {
            file.write().await?.sync_all_in_batch(self.token.id).await?;
        } else {
            let mut file = self
                .backend
                .read()
                .await?
                .open_options()
                .write(true)
                .read(true)
                .open(path)
                .await?;
            file.flush_local().await?;
            file.sync_all_in_batch(self.token.id).await?;
        }
        if let Some(version) = version {
            let mut batches = self.batches.write().await?;
            let batch = self.active_batch_mut(&mut batches)?;
            if batch.dirty_files.get(path) == Some(&version) {
                batch.dirty_files.remove(path);
            }
        }
        Ok(())
    }

    fn active_batch<'a>(&self, batches: &'a BatchRegistry) -> Result<&'a ActiveBatch, ReductError> {
        batches
            .active
            .get(&self.token.id)
            .ok_or(internal_server_error!("File batch is no longer active"))
    }

    fn active_batch_mut<'a>(
        &self,
        batches: &'a mut BatchRegistry,
    ) -> Result<&'a mut ActiveBatch, ReductError> {
        batches
            .active
            .get_mut(&self.token.id)
            .ok_or(internal_server_error!("File batch is no longer active"))
    }

    async fn set_state(&self, state: BatchState) -> Result<(), ReductError> {
        let mut batches = self.batches.write().await?;
        self.active_batch_mut(&mut batches)?.state = state;
        Ok(())
    }
}

impl Drop for FileCache {
    fn drop(&mut self) {
        self.stop_sync_worker.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
pub(crate) fn build_test_file_cache() -> Arc<FileCache> {
    use futures::executor;

    let temp_dir = tempfile::tempdir()
        .expect("Failed to create temporary directory for test FileCache")
        .keep();
    let cache = FileCache::new(
        FILE_CACHE_MAX_SIZE,
        FILE_CACHE_TIME_TO_LIVE,
        FILE_CACHE_SYNC_INTERVAL,
    );
    executor::block_on(async {
        let backend = (Backend::builder().local_data_path(temp_dir).try_build())
            .await
            .expect("Failed to initialise test FileCache backend");
        cache.set_storage_backend(backend).await;
    });
    Arc::new(cache)
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
    #[tokio::test]
    async fn batch_defers_background_sync_until_explicitly_published(tmp_dir: PathBuf) {
        let scope = tmp_dir.join("entry");
        let file_path = scope.join("blocks.idx");
        let backend = build_backend(|mock| {
            expect_path(mock, &tmp_dir, 1);
            mock.expect_create_dir_all().returning(|path| {
                fs::create_dir_all(path)?;
                Ok(())
            });
            expect_try_exists(mock, &file_path, false, 1);
            expect_update_local_cache(mock, &file_path, AccessMode::ReadWrite, 1);
            mock.expect_invalidate_locally_cached_files()
                .returning(Vec::new)
                .times(1);
            expect_upload(mock, &file_path, 1);
        });
        let cache = build_cache(backend);
        let mut batch = cache.begin_batch().await.unwrap();
        let token = batch.token();
        {
            let mut file = cache
                .write_or_create_in_batch(&token, &file_path, SeekFrom::Start(0))
                .await
                .unwrap();
            file.write_all(b"index").unwrap();
        }

        cache.force_sync_all().await.unwrap();
        assert!(!cache
            .cache
            .read()
            .await
            .unwrap()
            .get(&file_path)
            .unwrap()
            .read()
            .await
            .unwrap()
            .is_synced());

        assert_eq!(
            cache
                .cache
                .read()
                .await
                .unwrap()
                .get(&file_path)
                .unwrap()
                .write()
                .await
                .unwrap()
                .sync_all()
                .await
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::PermissionDenied
        );
        batch.sync_file(&file_path).await.unwrap();
        assert!(cache
            .cache
            .read()
            .await
            .unwrap()
            .get(&file_path)
            .unwrap()
            .read()
            .await
            .unwrap()
            .is_synced());
        batch.commit().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn batch_defers_remote_delete_until_sync_all(tmp_dir: PathBuf) {
        let scope = tmp_dir.join("entry");
        let file_path = scope.join("old.blk");
        fs::create_dir_all(&scope).unwrap();
        fs::write(&file_path, b"old").unwrap();
        let backend = build_backend(|mock| {
            let expected = file_path.clone();
            mock.expect_remove()
                .withf(move |path| path == expected.as_path())
                .returning(|_| Ok(()))
                .times(1);
        });
        let cache = build_cache(backend);
        let mut batch = cache.begin_batch().await.unwrap();
        let token = batch.token();

        cache.remove_in_batch(&token, &file_path).await.unwrap();
        assert!(!file_path.exists());

        batch.sync_all().await.unwrap();
        batch.commit().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn batch_rejects_directory_operations_affecting_owned_files(tmp_dir: PathBuf) {
        let scope = tmp_dir.join("entry");
        let file_path = scope.join("blocks.idx");
        let backend = build_backend(|mock| {
            expect_path(mock, &tmp_dir, 1);
            mock.expect_create_dir_all().returning(|path| {
                fs::create_dir_all(path)?;
                Ok(())
            });
            expect_try_exists(mock, &file_path, false, 1);
            expect_update_local_cache(mock, &file_path, AccessMode::ReadWrite, 1);
        });
        let cache = build_cache(backend);
        let mut batch = cache.begin_batch().await.unwrap();
        let token = batch.token();
        drop(
            cache
                .write_or_create_in_batch(&token, &file_path, SeekFrom::Start(0))
                .await
                .unwrap(),
        );

        let err = cache.remove_dir(&scope).await.unwrap_err();
        assert_eq!(
            err,
            internal_server_error!(
                "Cannot modify {} because it affects an active file batch",
                scope.display()
            )
        );
        batch.sync_all().await.unwrap();
        batch.commit().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn batch_rejects_commit_with_pending_upload(tmp_dir: PathBuf) {
        let file_path = tmp_dir.join("blocks.idx");
        let backend = build_backend(|mock| {
            expect_path(mock, &tmp_dir, 1);
            expect_try_exists(mock, &file_path, false, 1);
            expect_update_local_cache(mock, &file_path, AccessMode::ReadWrite, 1);
        });
        let cache = build_cache(backend);
        let batch = cache.begin_batch().await.unwrap();
        let token = batch.token();

        drop(
            cache
                .write_or_create_in_batch(&token, &file_path, SeekFrom::Start(0))
                .await
                .unwrap(),
        );

        assert_eq!(
            batch.commit().await.unwrap_err(),
            internal_server_error!("Cannot commit file batch with pending remote mutations")
        );
    }

    #[rstest]
    #[tokio::test]
    async fn failed_batch_upload_blocks_commit(tmp_dir: PathBuf) {
        let file_path = tmp_dir.join("blocks.idx");
        let backend = build_backend(|mock| {
            expect_path(mock, &tmp_dir, 1);
            expect_try_exists(mock, &file_path, false, 1);
            expect_update_local_cache(mock, &file_path, AccessMode::ReadWrite, 1);
            mock.expect_upload()
                .returning(|_| Err(std::io::Error::from(ErrorKind::Other)))
                .times(1);
        });
        let cache = build_cache(backend);
        let mut batch = cache.begin_batch().await.unwrap();
        let token = batch.token();
        {
            let mut file = cache
                .write_or_create_in_batch(&token, &file_path, SeekFrom::Start(0))
                .await
                .unwrap();
            file.write_all(b"index").unwrap();
        }

        assert_eq!(
            batch.sync_file(&file_path).await.err().unwrap(),
            internal_server_error!("other error")
        );
        assert_eq!(
            batch.commit().await.unwrap_err(),
            internal_server_error!("Cannot commit failed file batch; retry synchronization first")
        );
    }

    #[rstest]
    #[tokio::test]
    async fn batch_rejects_path_owned_by_competing_batch(tmp_dir: PathBuf) {
        let file_path = tmp_dir.join("blocks.idx");
        let backend = build_backend(|mock| {
            expect_path(mock, &tmp_dir, 1);
            expect_try_exists(mock, &file_path, false, 1);
            expect_update_local_cache(mock, &file_path, AccessMode::ReadWrite, 1);
        });
        let cache = build_cache(backend);
        let first_batch = cache.begin_batch().await.unwrap();
        let second_batch = cache.begin_batch().await.unwrap();

        drop(
            cache
                .write_or_create_in_batch(&first_batch.token(), &file_path, SeekFrom::Start(0))
                .await
                .unwrap(),
        );

        assert_eq!(
            cache
                .write_or_create_in_batch(&second_batch.token(), &file_path, SeekFrom::Start(0))
                .await
                .err()
                .unwrap(),
            internal_server_error!(
                "File {} is owned by another active file batch",
                file_path.display()
            )
        );
    }

    #[rstest]
    #[tokio::test]
    async fn batch_rejects_stale_token(tmp_dir: PathBuf) {
        let file_path = tmp_dir.join("blocks.idx");
        let cache = build_cache(build_backend(|_mock| {}));
        let batch = cache.begin_batch().await.unwrap();
        let stale_token = batch.token();
        batch.commit().await.unwrap();

        assert_eq!(
            cache
                .write_or_create_in_batch(&stale_token, &file_path, SeekFrom::Start(0))
                .await
                .err()
                .unwrap(),
            internal_server_error!("File batch is no longer active")
        );
    }

    #[rstest]
    #[tokio::test]
    async fn batch_syncs_dirty_file_evicted_from_cache(tmp_dir: PathBuf) {
        let file_path = tmp_dir.join("blocks.idx");
        let backend = build_backend(|mock| {
            expect_path(mock, &tmp_dir, 2);
            expect_try_exists(mock, &file_path, false, 1);
            expect_update_local_cache(mock, &file_path, AccessMode::ReadWrite, 1);
            expect_upload(mock, &file_path, 1);
        });
        let cache = build_cache(backend);
        let mut batch = cache.begin_batch().await.unwrap();
        let token = batch.token();
        {
            let mut file = cache
                .write_or_create_in_batch(&token, &file_path, SeekFrom::Start(0))
                .await
                .unwrap();
            file.write_all(b"index").unwrap();
        }
        cache.cache.write().await.unwrap().remove(&file_path);

        batch.sync_file(&file_path).await.unwrap();
        batch.commit().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn batch_ignores_not_found_for_deferred_delete(tmp_dir: PathBuf) {
        let file_path = tmp_dir.join("old.blk");
        fs::write(&file_path, b"old").unwrap();
        let backend = build_backend(|mock| {
            mock.expect_remove()
                .returning(|_| Err(std::io::Error::from(ErrorKind::NotFound)))
                .times(1);
        });
        let cache = build_cache(backend);
        let mut batch = cache.begin_batch().await.unwrap();
        let token = batch.token();

        cache.remove_in_batch(&token, &file_path).await.unwrap();
        batch.sync_all().await.unwrap();
        batch.commit().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn failed_deferred_delete_blocks_commit(tmp_dir: PathBuf) {
        let file_path = tmp_dir.join("old.blk");
        fs::write(&file_path, b"old").unwrap();
        let backend = build_backend(|mock| {
            mock.expect_remove()
                .returning(|_| Err(std::io::Error::from(ErrorKind::PermissionDenied)))
                .times(1);
        });
        let cache = build_cache(backend);
        let mut batch = cache.begin_batch().await.unwrap();
        let token = batch.token();

        cache.remove_in_batch(&token, &file_path).await.unwrap();
        assert_eq!(
            batch.sync_all().await.err().unwrap(),
            internal_server_error!("permission denied")
        );
        assert_eq!(
            batch.commit().await.unwrap_err(),
            internal_server_error!("Cannot commit failed file batch; retry synchronization first")
        );
    }

    #[rstest]
    #[tokio::test]
    async fn batch_releases_claim_after_failed_file_open(tmp_dir: PathBuf) {
        let file_path = tmp_dir.join("missing").join("blocks.idx");
        let backend = build_backend(|mock| {
            expect_path(mock, &tmp_dir, 1);
            mock.expect_create_dir_all()
                .returning(|_| Err(std::io::Error::from(ErrorKind::PermissionDenied)))
                .times(1);
        });
        let cache = build_cache(backend);
        let batch = cache.begin_batch().await.unwrap();

        assert_eq!(
            cache
                .write_or_create_in_batch(&batch.token(), &file_path, SeekFrom::Start(0))
                .await
                .err()
                .unwrap(),
            internal_server_error!("permission denied")
        );
        assert!(!cache
            .batches
            .read()
            .await
            .unwrap()
            .path_owners
            .contains_key(&file_path));
    }

    #[rstest]
    #[tokio::test]
    async fn batch_keeps_pending_delete_after_local_delete_failure(tmp_dir: PathBuf) {
        let file_path = tmp_dir.join("missing.blk");
        let backend = build_backend(|mock| {
            expect_remove(mock, &file_path, 1);
        });
        let cache = build_cache(backend);
        let mut batch = cache.begin_batch().await.unwrap();
        let token = batch.token();

        let err = cache.remove_in_batch(&token, &file_path).await.unwrap_err();
        assert_eq!(
            err.status(),
            reduct_base::error::ErrorCode::InternalServerError
        );
        assert!(cache
            .batches
            .read()
            .await
            .unwrap()
            .active
            .get(&token.id)
            .unwrap()
            .pending_deletes
            .contains(&file_path));

        batch.sync_all().await.unwrap();
        batch.commit().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn batch_sync_all_retries_failed_upload_and_clears_failed_state(tmp_dir: PathBuf) {
        let file_path = tmp_dir.join("blocks.idx");
        let first_upload = Arc::new(AtomicBool::new(true));
        let backend = build_backend(|mock| {
            expect_path(mock, &tmp_dir, 1);
            expect_try_exists(mock, &file_path, false, 1);
            expect_update_local_cache(mock, &file_path, AccessMode::ReadWrite, 1);
            let first_upload = Arc::clone(&first_upload);
            mock.expect_upload().returning(move |_| {
                if first_upload.swap(false, Ordering::Relaxed) {
                    Err(std::io::Error::from(ErrorKind::Other))
                } else {
                    Ok(())
                }
            });
        });
        let cache = build_cache(backend);
        let mut batch = cache.begin_batch().await.unwrap();
        let token = batch.token();
        {
            let mut file = cache
                .write_or_create_in_batch(&token, &file_path, SeekFrom::Start(0))
                .await
                .unwrap();
            file.write_all(b"index").unwrap();
        }

        assert_eq!(
            batch.sync_all().await.unwrap_err(),
            internal_server_error!("other error")
        );
        assert!(matches!(
            cache
                .batches
                .read()
                .await
                .unwrap()
                .active
                .get(&token.id)
                .unwrap()
                .state,
            BatchState::Failed
        ));

        batch.sync_all().await.unwrap();
        assert!(matches!(
            cache
                .batches
                .read()
                .await
                .unwrap()
                .active
                .get(&token.id)
                .unwrap()
                .state,
            BatchState::Active
        ));
        batch.commit().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn batch_commit_releases_cached_file_owner(tmp_dir: PathBuf) {
        let file_path = tmp_dir.join("blocks.idx");
        let backend = build_backend(|mock| {
            expect_path(mock, &tmp_dir, 1);
            expect_try_exists(mock, &file_path, false, 1);
            expect_update_local_cache(mock, &file_path, AccessMode::ReadWrite, 1);
            expect_upload(mock, &file_path, 1);
        });
        let cache = build_cache(backend);
        let mut batch = cache.begin_batch().await.unwrap();
        let token = batch.token();
        {
            let mut file = cache
                .write_or_create_in_batch(&token, &file_path, SeekFrom::Start(0))
                .await
                .unwrap();
            file.write_all(b"index").unwrap();
        }

        batch.sync_all().await.unwrap();
        batch.commit().await.unwrap();

        cache
            .cache
            .read()
            .await
            .unwrap()
            .get(&file_path)
            .unwrap()
            .write()
            .await
            .unwrap()
            .sync_all()
            .await
            .unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn batch_sync_file_rejects_stale_and_foreign_paths(tmp_dir: PathBuf) {
        let file_path = tmp_dir.join("blocks.idx");
        let backend = build_backend(|mock| {
            expect_path(mock, &tmp_dir, 1);
            expect_try_exists(mock, &file_path, false, 1);
            expect_update_local_cache(mock, &file_path, AccessMode::ReadWrite, 1);
        });
        let cache = build_cache(backend);

        let batch = cache.begin_batch().await.unwrap();
        let mut stale_batch = FileBatch {
            token: batch.token(),
            cache: Arc::clone(&cache.cache),
            backend: Arc::clone(&cache.backend),
            batches: Arc::clone(&cache.batches),
        };
        batch.commit().await.unwrap();
        assert_eq!(
            stale_batch.sync_file(&file_path).await.unwrap_err(),
            internal_server_error!("File batch is no longer active")
        );
        assert_eq!(
            stale_batch.sync_all().await.unwrap_err(),
            internal_server_error!("File batch is no longer active")
        );

        let first_batch = cache.begin_batch().await.unwrap();
        let mut foreign_batch = cache.begin_batch().await.unwrap();
        drop(
            cache
                .write_or_create_in_batch(&first_batch.token(), &file_path, SeekFrom::Start(0))
                .await
                .unwrap(),
        );
        assert_eq!(
            foreign_batch.sync_file(&file_path).await.unwrap_err(),
            internal_server_error!(
                "File {} is not owned by this file batch",
                file_path.display()
            )
        );
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
        async fn skips_file_locked_for_write_when_collecting_sync_candidates(tmp_dir: PathBuf) {
            let file_path = tmp_dir.join("test_locked_write_sync_candidate.txt");
            let backend = build_backend(|mock| {
                expect_path(mock, &tmp_dir, 1);
                expect_try_exists(mock, &file_path, false, 1);
                expect_update_local_cache(mock, &file_path, AccessMode::ReadWrite, 1);
                mock.expect_invalidate_locally_cached_files()
                    .returning(Vec::new)
                    .times(1);
            });
            let cache = build_cache(backend);
            {
                let mut file_ref = cache
                    .write_or_create(&file_path, SeekFrom::Start(0))
                    .await
                    .unwrap();
                file_ref.write_all(b"test").unwrap();
            }

            let file = cache
                .cache
                .read()
                .await
                .unwrap()
                .get(&file_path)
                .unwrap()
                .clone();
            let _write_guard = file.write().await.unwrap();
            let sync_interval = Some(Arc::new(RwLock::new(Duration::from_millis(0))));

            FileCache::sync_rw_and_unused_files(
                &cache.read_only,
                &cache.backend,
                &cache.cache,
                &cache.batches,
                &sync_interval,
            )
            .await
            .unwrap();

            drop(_write_guard);
            assert!(!file.read().await.unwrap().is_synced());
        }

        #[rstest]
        #[tokio::test]
        async fn skips_file_locked_for_read_when_syncing_candidates(tmp_dir: PathBuf) {
            let file_path = tmp_dir.join("test_locked_read_sync_candidate.txt");
            let backend = build_backend(|mock| {
                expect_path(mock, &tmp_dir, 1);
                expect_try_exists(mock, &file_path, false, 1);
                expect_update_local_cache(mock, &file_path, AccessMode::ReadWrite, 1);
                mock.expect_invalidate_locally_cached_files()
                    .returning(Vec::new)
                    .times(1);
            });
            let cache = build_cache(backend);
            {
                let mut file_ref = cache
                    .write_or_create(&file_path, SeekFrom::Start(0))
                    .await
                    .unwrap();
                file_ref.write_all(b"test").unwrap();
            }

            let file = cache
                .cache
                .read()
                .await
                .unwrap()
                .get(&file_path)
                .unwrap()
                .clone();
            let _read_guard = file.read().await.unwrap();
            let sync_interval = Some(Arc::new(RwLock::new(Duration::from_millis(0))));

            FileCache::sync_rw_and_unused_files(
                &cache.read_only,
                &cache.backend,
                &cache.cache,
                &cache.batches,
                &sync_interval,
            )
            .await
            .unwrap();

            drop(_read_guard);
            assert!(!file.read().await.unwrap().is_synced());
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

        #[rstest]
        #[tokio::test]
        async fn scheduled_sync_processes_only_batch_size(tmp_dir: PathBuf) {
            let file_count = FILE_CACHE_SYNC_BATCH_SIZE + 1;
            let oldest_subset = 10;
            let file_paths: Vec<PathBuf> = (0..file_count)
                .map(|i| tmp_dir.join(format!("test_scheduled_sync_batch_{i}.txt")))
                .collect();

            let backend = build_backend(|mock| {
                expect_path(mock, &tmp_dir, file_count);
                for file_path in &file_paths {
                    expect_try_exists(mock, file_path, false, 1);
                    expect_update_local_cache(mock, file_path, AccessMode::ReadWrite, 1);
                }
                mock.expect_invalidate_locally_cached_files()
                    .returning(Vec::new)
                    .times(1);
                mock.expect_upload()
                    .returning(|_| Ok(()))
                    .times(FILE_CACHE_SYNC_BATCH_SIZE);
            });

            let cache = FileCache::new(
                file_count + 1,
                Duration::from_secs(60),
                Duration::from_secs(60),
            );
            cache.set_storage_backend(backend).await;
            cache.stop_sync_worker.store(true, Ordering::Relaxed);
            for file_path in &file_paths {
                let mut file_ref = cache
                    .write_or_create(file_path, SeekFrom::Start(0))
                    .await
                    .unwrap();
                file_ref.write_all(b"test").unwrap();
                tokio::time::sleep(Duration::from_millis(1)).await;
            }

            let sync_interval = Some(Arc::new(RwLock::new(Duration::from_millis(0))));
            FileCache::sync_rw_and_unused_files(
                &cache.read_only,
                &cache.backend,
                &cache.cache,
                &cache.batches,
                &sync_interval,
            )
            .await
            .unwrap();

            for file_path in &file_paths[..oldest_subset] {
                let file = cache
                    .cache
                    .read()
                    .await
                    .unwrap()
                    .get(file_path)
                    .unwrap()
                    .clone();
                assert!(file.read().await.unwrap().is_synced());
            }
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
