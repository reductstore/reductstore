// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::core::file_cache::FILE_CACHE;
use log::debug;
use parking_lot::Mutex;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{Read, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Once, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DECOMPRESS_CACHE_TTL: Duration = Duration::from_secs(30);
const DECOMPRESS_CACHE_CLEANUP_INTERVAL: Duration = Duration::from_secs(5);

const TEMP_DIR_PREFIX: &str = "reductstore_decompress_cache_";
const LOCK_FILE_NAME: &str = ".lock";
/// Directories created before the lock file was introduced have no owner to check,
/// so we only remove them after they have been idle for a while.
const UNLOCKED_DIR_MAX_IDLE: Duration = Duration::from_secs(3600);

/// Process-wide cache of decompressed blocks.
///
/// Why global:
/// - A server can have any number of entries, so a per-entry limit doesn't bound disk usage.
/// - A single cache limited by bytes keeps the worst case predictable and evicts cold
///   blocks of all entries under shared pressure (the same idea as the global block read cache).
///
/// Block managers own the cache through their handles (see [`DecompressCache::shared`]), and this
/// static keeps only a weak reference. When the last block manager and the last reader are
/// dropped, `Drop for Inner` removes the temporary directory. Each handle has its own keys, so a
/// re-created entry never gets files decompressed for the previous one under the same path.
static SHARED_CACHE: Mutex<Weak<Inner>> = parking_lot::const_mutex(Weak::new());
/// Directories of killed processes are removed once per process, before the first cache is created.
static REMOVE_STALE_DIRS: Once = Once::new();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DecompressedFileType {
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

struct CachedFile {
    file: Arc<DiskFile>,
    last_access: Instant,
}

/// A decompressed file on disk. It is removed when neither the cache nor any reader holds it.
struct DiskFile {
    path: PathBuf,
    size: u64,
    total_size: Arc<AtomicU64>,
}

/// A decompressed file handed out to a reader.
///
/// The cache may evict, expire or invalidate the file in the meantime,
/// but it stays on disk until the last guard for it is dropped.
pub(super) struct DecompressedFile {
    file: Arc<DiskFile>,
    // Keeps the cache directory as well
    _cache: Arc<Inner>,
}

struct CacheState {
    files: HashMap<String, CachedFile>,
    /// Size of all decompressed files on disk, including evicted files that are still in use.
    total_size: Arc<AtomicU64>,
    max_size: u64,
    /// Exclusive lock on `<temp_dir>/.lock`, held while the directory is in use.
    /// Other processes use it to tell a live cache directory from a stale one.
    dir_lock: Option<File>,
}

struct Inner {
    state: Mutex<CacheState>,
    temp_dir: PathBuf,
    ttl: Duration,
    cleanup_started: AtomicBool,
}

static NEXT_OWNER_ID: AtomicU64 = AtomicU64::new(1);
/// Concurrent readers may decompress the same block, and the clock alone can repeat
/// (1 µs resolution on macOS), so file names get a counter.
static NEXT_FILE_ID: AtomicU64 = AtomicU64::new(1);

pub(super) struct DecompressCache {
    /// Unique owner id, part of every key of this handle.
    owner: u64,
    inner: Arc<Inner>,
}

impl DecompressCache {
    fn new(temp_dir: PathBuf, max_size: u64, ttl: Duration) -> Self {
        Self {
            owner: 0,
            inner: Arc::new(Inner {
                state: Mutex::new(CacheState {
                    files: HashMap::new(),
                    total_size: Arc::new(AtomicU64::new(0)),
                    max_size,
                    dir_lock: None,
                }),
                temp_dir,
                ttl,
                cleanup_started: AtomicBool::new(false),
            }),
        }
    }

    /// Get a handle to the process-wide cache, creating the cache if no handle is alive.
    ///
    /// `max_size` applies only when the cache is created: all block managers share one config.
    pub(super) fn shared(max_size: u64) -> Self {
        let parent = std::env::temp_dir();
        REMOVE_STALE_DIRS.call_once(|| remove_stale_temp_dirs(&parent, UNLOCKED_DIR_MAX_IDLE));
        Self::shared_in(&SHARED_CACHE, &parent, max_size, DECOMPRESS_CACHE_TTL)
    }

    fn shared_in(slot: &Mutex<Weak<Inner>>, parent: &Path, max_size: u64, ttl: Duration) -> Self {
        let mut shared = slot.lock();
        if let Some(inner) = shared.upgrade() {
            return Self {
                owner: NEXT_OWNER_ID.fetch_add(1, Ordering::Relaxed),
                inner,
            };
        }

        let cache = Self::new(temp_dir_path(parent), max_size, ttl);
        *shared = Arc::downgrade(&cache.inner);
        cache.handle()
    }

    /// Create a handle with its own keys that shares the files and the size limit with this cache.
    fn handle(&self) -> Self {
        Self {
            owner: NEXT_OWNER_ID.fetch_add(1, Ordering::Relaxed),
            inner: Arc::clone(&self.inner),
        }
    }

    pub(super) async fn get_or_decompress(
        &self,
        entry_path: &Path,
        block_id: u64,
        file_type: DecompressedFileType,
        compressed_path: &PathBuf,
    ) -> Result<DecompressedFile, ReductError> {
        self.start_cleanup_worker();

        let key = self.key(entry_path, block_id, file_type);
        if let Some(file) = self.inner.state.lock().get(&key) {
            return Ok(self.guard(file));
        }

        // Decompress without holding the lock, so that one slow block doesn't block reads of other entries.
        // Invalidation needs `&mut BlockManager`, so it can't interleave with a read of the same entry.
        let (path, size) = self
            .decompress_to_temp(entry_path, block_id, file_type, compressed_path)
            .await?;

        let mut state = self.inner.state.lock();
        if let Some(existing) = state.get(&key) {
            drop(state);
            // Another reader decompressed the same block in the meantime
            cleanup_tmp(&path);
            return Ok(self.guard(existing));
        }

        let file = state.insert(key, path, size);
        let evicted = state.evict_over_limit();
        drop(state);
        // Evicted files are removed from disk here, outside the lock
        drop(evicted);
        Ok(self.guard(file))
    }

    /// Remove cached decompressed files for a block.
    ///
    /// Files that are still in use are removed when their readers drop them.
    pub(super) async fn invalidate(&self, entry_path: &Path, block_id: u64) {
        let removed = {
            let mut state = self.inner.state.lock();
            [DecompressedFileType::Data, DecompressedFileType::Descriptor]
                .into_iter()
                .filter_map(|file_type| state.remove(&self.key(entry_path, block_id, file_type)))
                .collect::<Vec<_>>()
        };
        drop(removed);
    }

    /// Expired files are removed in the background, so they don't stay on disk until the next read.
    fn start_cleanup_worker(&self) {
        if self.inner.cleanup_started.swap(true, Ordering::AcqRel) {
            return;
        }

        let interval = DECOMPRESS_CACHE_CLEANUP_INTERVAL.min(self.inner.ttl);
        let inner: Weak<Inner> = Arc::downgrade(&self.inner);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Some(inner) = inner.upgrade() else {
                    break;
                };
                inner.cleanup();
            }
        });
    }

    fn guard(&self, file: Arc<DiskFile>) -> DecompressedFile {
        DecompressedFile {
            file,
            _cache: Arc::clone(&self.inner),
        }
    }

    fn key(&self, entry_path: &Path, block_id: u64, file_type: DecompressedFileType) -> String {
        format!(
            "{}::{}::{}::{}",
            self.owner,
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
    ) -> Result<(PathBuf, u64), ReductError> {
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

        self.prepare_temp_dir()?;

        let temp_path = self.temp_path(entry_path, block_id, file_type);
        if let Err(err) = write_temp_file(&temp_path, &decompressed) {
            // Don't leave a partial file behind: nobody would ever remove it.
            cleanup_tmp(&temp_path);
            return Err(err);
        }

        Ok((temp_path, decompressed.len() as u64))
    }

    fn prepare_temp_dir(&self) -> Result<(), ReductError> {
        let temp_dir = &self.inner.temp_dir;
        let mut state = self.inner.state.lock();
        if state.dir_lock.is_some() && temp_dir.exists() {
            return Ok(());
        }

        std::fs::create_dir_all(temp_dir).map_err(|err| {
            internal_server_error!(
                "Failed to create decompressed temporary directory {:?}: {}",
                temp_dir,
                err
            )
        })?;

        let lock_path = temp_dir.join(LOCK_FILE_NAME);
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .map_err(|err| {
                internal_server_error!("Failed to create lock file {:?}: {}", lock_path, err)
            })?;
        lock.try_lock()
            .map_err(|err| internal_server_error!("Failed to lock {:?}: {}", lock_path, err))?;
        state.dir_lock = Some(lock);
        Ok(())
    }

    fn temp_path(
        &self,
        entry_path: &Path,
        block_id: u64,
        file_type: DecompressedFileType,
    ) -> PathBuf {
        let mut hasher = DefaultHasher::new();
        entry_path.hash(&mut hasher);
        block_id.hash(&mut hasher);
        file_type.as_key_part().hash(&mut hasher);
        let entry_hash = hasher.finish();

        self.inner.temp_dir.join(format!(
            "{}_{}_{}.{}",
            entry_hash,
            block_id,
            NEXT_FILE_ID.fetch_add(1, Ordering::Relaxed),
            file_type.as_extension()
        ))
    }
}

impl DecompressedFile {
    pub(super) fn path(&self) -> &PathBuf {
        &self.file.path
    }
}

impl Debug for DecompressedFile {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("DecompressedFile")
            .field(self.path())
            .finish()
    }
}

impl Inner {
    /// Remove expired files and, once readers have released them, files over the size limit.
    fn cleanup(&self) {
        let expired = self.state.lock().remove_expired(self.ttl);
        // Drop the files outside the lock, so that the total size is up to date for eviction
        drop(expired);
        let evicted = self.state.lock().evict_over_limit();
        drop(evicted);
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        // Release the lock file first, otherwise the directory can't be removed on Windows
        self.state.get_mut().dir_lock = None;
        cleanup_tmp_dir(&self.temp_dir);
    }
}

impl Drop for DiskFile {
    fn drop(&mut self) {
        // FILE_CACHE may still keep the file open, then the space is freed when it closes the file
        cleanup_tmp(&self.path);
        self.total_size.fetch_sub(self.size, Ordering::Relaxed);
    }
}

impl CachedFile {
    /// A guard holds another reference to the file.
    /// New guards are only created from the cache under the state lock.
    fn in_use(&self) -> bool {
        Arc::strong_count(&self.file) > 1
    }
}

impl CacheState {
    fn total_size(&self) -> u64 {
        self.total_size.load(Ordering::Relaxed)
    }

    fn get(&mut self, key: &str) -> Option<Arc<DiskFile>> {
        self.files.get_mut(key).map(|file| {
            file.last_access = Instant::now();
            Arc::clone(&file.file)
        })
    }

    fn insert(&mut self, key: String, path: PathBuf, size: u64) -> Arc<DiskFile> {
        self.total_size.fetch_add(size, Ordering::Relaxed);
        let file = Arc::new(DiskFile {
            path,
            size,
            total_size: Arc::clone(&self.total_size),
        });
        self.files.insert(
            key,
            CachedFile {
                file: Arc::clone(&file),
                last_access: Instant::now(),
            },
        );
        file
    }

    fn remove(&mut self, key: &str) -> Option<Arc<DiskFile>> {
        self.files.remove(key).map(|file| file.file)
    }

    /// Files in use are skipped, they expire in a later run after their readers are done.
    fn remove_expired(&mut self, ttl: Duration) -> Vec<Arc<DiskFile>> {
        let expired = self
            .files
            .iter()
            .filter(|(_, file)| file.last_access.elapsed() > ttl && !file.in_use())
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        expired.iter().filter_map(|key| self.remove(key)).collect()
    }

    /// Evict the least recently used files until the total size fits the limit.
    ///
    /// Files in use are not evicted but count toward the limit, so the cache can stay over it
    /// while they are read, e.g. a file that alone is larger than the limit. After they are
    /// released, the next insert or cleanup run evicts them.
    fn evict_over_limit(&mut self) -> Vec<Arc<DiskFile>> {
        let mut total_size = self.total_size();
        let mut evicted = Vec::new();
        while total_size > self.max_size {
            let oldest = self
                .files
                .iter()
                .filter(|(_, file)| !file.in_use())
                .min_by_key(|(_, file)| file.last_access)
                .map(|(key, _)| key.clone());
            match oldest.and_then(|key| self.remove(&key)) {
                Some(file) => {
                    total_size = total_size.saturating_sub(file.size);
                    evicted.push(file);
                }
                None => break,
            }
        }
        evicted
    }
}

fn write_temp_file(path: &PathBuf, content: &[u8]) -> Result<(), ReductError> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .read(true)
        .open(path)
        .map_err(|err| {
            internal_server_error!(
                "Failed to create decompressed temporary file {:?}: {}",
                path,
                err
            )
        })?;
    file.write_all(content).map_err(|err| {
        internal_server_error!(
            "Failed to write decompressed temporary file {:?}: {}",
            path,
            err
        )
    })?;
    file.sync_all().map_err(|err| {
        internal_server_error!(
            "Failed to sync decompressed temporary file {:?}: {}",
            path,
            err
        )
    })
}

/// Remove cache directories left by processes that terminated without cleanup.
///
/// A directory is stale if nobody holds the lock on its lock file. The OS releases
/// the lock when the process dies, so live caches of other instances are kept.
fn remove_stale_temp_dirs(parent: &Path, unlocked_max_idle: Duration) {
    let Ok(dirs) = std::fs::read_dir(parent) else {
        return;
    };

    for dir in dirs.flatten() {
        let path = dir.path();
        let is_cache_dir = dir
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with(TEMP_DIR_PREFIX));
        if !is_cache_dir || !path.is_dir() {
            continue;
        }

        let stale = match File::open(path.join(LOCK_FILE_NAME)) {
            // The lock is released together with the file handle at the end of the scope
            Ok(lock) => lock.try_lock().is_ok(),
            Err(_) => is_idle(&path, unlocked_max_idle),
        };
        if stale {
            debug!("Remove stale decompression cache {:?}", path);
            cleanup_tmp_dir(&path);
        }
    }
}

fn is_idle(path: &Path, max_idle: Duration) -> bool {
    std::fs::metadata(path)
        .and_then(|meta| meta.modified())
        .ok()
        .and_then(|modified| modified.elapsed().ok())
        .is_some_and(|idle| idle > max_idle)
}

fn temp_dir_path(parent: &Path) -> PathBuf {
    parent.join(format!(
        "{}{}_{}",
        TEMP_DIR_PREFIX,
        std::process::id(),
        unique_suffix()
    ))
}

fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default()
}

fn cleanup_tmp(path: &Path) {
    let _ = std::fs::remove_file(path);
}

fn cleanup_tmp_dir(path: &Path) {
    let _ = std::fs::remove_dir_all(path);
}

#[cfg(test)]
mod tests {
    use super::*;
    use reduct_base::error::ErrorCode;
    use rstest::{fixture, rstest};
    use serial_test::serial;
    use tempfile::tempdir;

    #[fixture]
    fn dir() -> PathBuf {
        tempdir().unwrap().keep()
    }

    fn cache_in(dir: &Path, max_size: u64, ttl: Duration) -> DecompressCache {
        DecompressCache::new(temp_dir_path(dir), max_size, ttl)
    }

    fn compressed(dir: &Path, name: &str, content: &str) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, zstd::encode_all(content.as_bytes(), 3).unwrap()).unwrap();
        path
    }

    async fn wait_until(condition: impl Fn() -> bool) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !condition() {
            if Instant::now() > deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        true
    }

    fn cached_files(temp_dir: &Path) -> Vec<PathBuf> {
        let mut files = std::fs::read_dir(temp_dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.file_name().unwrap() != LOCK_FILE_NAME)
            .collect::<Vec<_>>();
        files.sort();
        files
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_get_or_decompress_caches_decompressed_file(dir: PathBuf) {
        let compressed_path = compressed(&dir, "1.blk.zst", "content");
        let cache = cache_in(&dir, 1000, DECOMPRESS_CACHE_TTL);

        let path = cache
            .get_or_decompress(&dir, 1, DecompressedFileType::Data, &compressed_path)
            .await
            .unwrap();
        let cached_path = cache
            .get_or_decompress(&dir, 1, DecompressedFileType::Data, &compressed_path)
            .await
            .unwrap();

        assert_eq!(path.path(), cached_path.path());
        assert_eq!(path.path().parent().unwrap(), cache.inner.temp_dir);
        assert_eq!(std::fs::read(path.path()).unwrap(), b"content");
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_get_or_decompress_descriptor_type(dir: PathBuf) {
        let compressed_path = compressed(&dir, "1.meta.zst", "descriptor content");
        let cache = cache_in(&dir, 1000, DECOMPRESS_CACHE_TTL);

        let path = cache
            .get_or_decompress(&dir, 1, DecompressedFileType::Descriptor, &compressed_path)
            .await
            .unwrap();

        assert_eq!(
            std::fs::read_to_string(path.path()).unwrap(),
            "descriptor content"
        );
        assert!(path.path().to_str().unwrap().ends_with(".meta"));
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_get_or_decompress_with_corrupted_data_returns_error(dir: PathBuf) {
        let compressed_path = dir.join("1.blk.zst");
        std::fs::write(&compressed_path, b"not valid zstd").unwrap();
        let cache = cache_in(&dir, 1000, DECOMPRESS_CACHE_TTL);

        let err = cache
            .get_or_decompress(&dir, 1, DecompressedFileType::Data, &compressed_path)
            .await
            .unwrap_err();

        assert_eq!(err.status(), ErrorCode::InternalServerError);
        assert_eq!(cache.inner.state.lock().total_size(), 0);
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_get_or_decompress_returns_error_when_temp_dir_is_file(dir: PathBuf) {
        let compressed_path = compressed(&dir, "1.blk.zst", "content");
        let temp_dir = dir.join("temp-file");
        std::fs::write(&temp_dir, b"not a directory").unwrap();
        let cache = DecompressCache::new(temp_dir, 1000, DECOMPRESS_CACHE_TTL);

        let err = cache
            .get_or_decompress(&dir, 1, DecompressedFileType::Data, &compressed_path)
            .await
            .unwrap_err();

        assert_eq!(err.status(), ErrorCode::InternalServerError);
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_invalidate_removes_cached_files(dir: PathBuf) {
        let data_path = compressed(&dir, "1.blk.zst", "data");
        let desc_path = compressed(&dir, "1.meta.zst", "desc");
        let cache = cache_in(&dir, 1000, DECOMPRESS_CACHE_TTL);

        let cached_data = cache
            .get_or_decompress(&dir, 1, DecompressedFileType::Data, &data_path)
            .await
            .unwrap()
            .path()
            .clone();
        let cached_desc = cache
            .get_or_decompress(&dir, 1, DecompressedFileType::Descriptor, &desc_path)
            .await
            .unwrap()
            .path()
            .clone();

        cache.invalidate(&dir, 1).await;

        assert!(!cached_data.exists());
        assert!(!cached_desc.exists());
        assert_eq!(cache.inner.state.lock().total_size(), 0);
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn test_cache_eviction_removes_temp_file(dir: PathBuf) {
        let entries = ["entry-1", "entry-2", "entry-3"].map(|name| dir.join(name));
        let compressed_path = compressed(&dir, "1.blk.zst", "1234");
        let cache = cache_in(&dir, 10, DECOMPRESS_CACHE_TTL);

        let mut paths = vec![];
        for entry in &entries[..2] {
            paths.push(
                cache
                    .get_or_decompress(entry, 1, DecompressedFileType::Data, &compressed_path)
                    .await
                    .unwrap()
                    .path()
                    .clone(),
            );
        }
        // Read the first entry again, so the second one becomes the least recently used
        cache
            .get_or_decompress(&entries[0], 1, DecompressedFileType::Data, &compressed_path)
            .await
            .unwrap();
        paths.push(
            cache
                .get_or_decompress(&entries[2], 1, DecompressedFileType::Data, &compressed_path)
                .await
                .unwrap()
                .path()
                .clone(),
        );

        assert!(paths[0].exists());
        assert!(!paths[1].exists());
        assert!(paths[2].exists());
        assert_eq!(cache.inner.state.lock().total_size(), 8);
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn handles_share_limit_but_not_files(dir: PathBuf) {
        let compressed_path = compressed(&dir, "1.blk.zst", "1234");
        let cache = cache_in(&dir, 6, DECOMPRESS_CACHE_TTL);
        let old_manager = cache.handle();
        let new_manager = cache.handle();

        let old_path = old_manager
            .get_or_decompress(&dir, 1, DecompressedFileType::Data, &compressed_path)
            .await
            .unwrap()
            .path()
            .clone();
        let new_path = new_manager
            .get_or_decompress(&dir, 1, DecompressedFileType::Data, &compressed_path)
            .await
            .unwrap()
            .path()
            .clone();

        assert_ne!(
            old_path, new_path,
            "same entry path and block, different owners"
        );
        assert!(!old_path.exists(), "evicted by the shared size limit");
        new_manager.invalidate(&dir, 1).await;
        assert!(!new_path.exists());
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn keeps_requested_file_larger_than_limit(dir: PathBuf) {
        let small_path = compressed(&dir, "1.blk.zst", "12");
        let large_path = compressed(&dir, "2.blk.zst", "1234567890");
        let cache = cache_in(&dir, 5, DECOMPRESS_CACHE_TTL);

        let small = cache
            .get_or_decompress(&dir, 1, DecompressedFileType::Data, &small_path)
            .await
            .unwrap()
            .path()
            .clone();
        let large = cache
            .get_or_decompress(&dir, 2, DecompressedFileType::Data, &large_path)
            .await
            .unwrap();

        assert!(!small.exists());
        assert_eq!(std::fs::read(large.path()).unwrap(), b"1234567890");
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn removes_expired_files_without_next_read(dir: PathBuf) {
        let compressed_path = compressed(&dir, "1.blk.zst", "content");
        let cache = cache_in(&dir, 1000, Duration::from_millis(20));

        let path = cache
            .get_or_decompress(&dir, 1, DecompressedFileType::Data, &compressed_path)
            .await
            .unwrap()
            .path()
            .clone();
        assert!(path.exists());
        // Poll instead of a fixed sleep to stay stable on slow CI runners
        for _ in 0..100 {
            if !path.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        assert!(!path.exists());
        assert_eq!(cache.inner.state.lock().total_size(), 0);
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn expired_file_in_use_is_removed_after_release(dir: PathBuf) {
        let file_type = DecompressedFileType::Data;
        let compressed_path = compressed(&dir, "1.blk.zst", "content");
        let cache = cache_in(&dir, 1000, Duration::from_millis(20));
        let in_use = cache
            .get_or_decompress(&dir.join("a"), 1, file_type, &compressed_path)
            .await
            .unwrap();
        let released = cache
            .get_or_decompress(&dir.join("b"), 1, file_type, &compressed_path)
            .await
            .unwrap()
            .path()
            .clone();

        // Both files have expired when the released one is removed
        assert!(wait_until(|| !released.exists()).await);
        assert_eq!(std::fs::read(in_use.path()).unwrap(), b"content");

        let path = in_use.path().clone();
        drop(in_use);
        assert!(wait_until(|| !path.exists()).await);
        assert_eq!(cache.inner.state.lock().total_size(), 0);
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn files_in_use_are_kept_over_limit_until_released(dir: PathBuf) {
        let file_type = DecompressedFileType::Data;
        let compressed_path = compressed(&dir, "1.blk.zst", "1234");
        let cache = cache_in(&dir, 5, DECOMPRESS_CACHE_TTL);
        let first = cache
            .get_or_decompress(&dir.join("a"), 1, file_type, &compressed_path)
            .await
            .unwrap();
        let second = cache
            .get_or_decompress(&dir.join("b"), 1, file_type, &compressed_path)
            .await
            .unwrap();
        cache.inner.cleanup();

        assert_eq!(std::fs::read(first.path()).unwrap(), b"1234");
        assert_eq!(std::fs::read(second.path()).unwrap(), b"1234");
        assert_eq!(cache.inner.state.lock().total_size(), 8);

        let (first_path, second_path) = (first.path().clone(), second.path().clone());
        drop((first, second));
        cache.inner.cleanup();

        assert!(!first_path.exists());
        assert_eq!(cache.inner.state.lock().total_size(), 4);

        let _third = cache
            .get_or_decompress(&dir.join("c"), 1, file_type, &compressed_path)
            .await
            .unwrap();
        assert!(!second_path.exists());
        assert_eq!(cache.inner.state.lock().total_size(), 4);
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn invalidated_file_is_removed_after_last_reader(dir: PathBuf) {
        let compressed_path = compressed(&dir, "1.blk.zst", "old");
        let cache = cache_in(&dir, 1000, DECOMPRESS_CACHE_TTL);
        let old = cache
            .get_or_decompress(&dir, 1, DecompressedFileType::Data, &compressed_path)
            .await
            .unwrap();

        cache.invalidate(&dir, 1).await;
        let new_path = compressed(&dir, "2.blk.zst", "new");
        let new = cache
            .get_or_decompress(&dir, 1, DecompressedFileType::Data, &new_path)
            .await
            .unwrap();

        assert_eq!(std::fs::read(old.path()).unwrap(), b"old");
        assert_eq!(std::fs::read(new.path()).unwrap(), b"new");

        let old_path = old.path().clone();
        drop(old);
        assert!(!old_path.exists());
        assert_eq!(cache.inner.state.lock().total_size(), 3);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn eviction_does_not_remove_file_being_read(dir: PathBuf) {
        let file_type = DecompressedFileType::Data;
        let content = "0123456789".repeat(10);
        let compressed_path = compressed(&dir, "1.blk.zst", &content);
        // Room for one file only
        let cache = Arc::new(cache_in(&dir, 150, DECOMPRESS_CACHE_TTL));
        let file = cache
            .get_or_decompress(&dir.join("reader"), 1, file_type, &compressed_path)
            .await
            .unwrap();

        let added = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let evictor = {
            let (cache, dir, compressed_path) =
                (Arc::clone(&cache), dir.clone(), compressed_path.clone());
            let (added, stop) = (Arc::clone(&added), Arc::clone(&stop));
            tokio::spawn(async move {
                for entry in 0.. {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    cache
                        .get_or_decompress(
                            &dir.join(entry.to_string()),
                            1,
                            file_type,
                            &compressed_path,
                        )
                        .await
                        .unwrap();
                    added.fetch_add(1, Ordering::Relaxed);
                }
            })
        };

        // Reopen the file for every chunk, as a reader does after FILE_CACHE has closed it
        let mut read = vec![];
        for offset in (0..content.len() as u64).step_by(10) {
            let target = added.load(Ordering::Relaxed) + 2;
            assert!(wait_until(|| added.load(Ordering::Relaxed) >= target).await);

            let mut chunk = [0; 10];
            let mut opened = File::open(file.path()).unwrap();
            std::io::Seek::seek(&mut opened, SeekFrom::Start(offset)).unwrap();
            opened.read_exact(&mut chunk).unwrap();
            read.extend_from_slice(&chunk);
        }
        stop.store(true, Ordering::Relaxed);
        evictor.await.unwrap();

        assert_eq!(read, content.as_bytes());
        let files = cached_files(&cache.inner.temp_dir);
        assert!(files.contains(file.path()));
        assert_eq!(
            cache.inner.state.lock().total_size(),
            100 * files.len() as u64
        );
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn drop_removes_temp_dir(dir: PathBuf) {
        let compressed_path = compressed(&dir, "1.blk.zst", "content");
        let cache = cache_in(&dir, 1000, DECOMPRESS_CACHE_TTL);
        let temp_dir = cache.inner.temp_dir.clone();
        cache
            .get_or_decompress(&dir, 1, DecompressedFileType::Data, &compressed_path)
            .await
            .unwrap();
        assert!(temp_dir.exists());

        drop(cache);

        assert!(!temp_dir.exists());
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn shared_cache_lives_while_any_handle_is_alive(dir: PathBuf) {
        let compressed_path = compressed(&dir, "1.blk.zst", "content");
        let slot = parking_lot::const_mutex(Weak::new());
        let first = DecompressCache::shared_in(&slot, &dir, 1000, DECOMPRESS_CACHE_TTL);
        let second = DecompressCache::shared_in(&slot, &dir, 500, DECOMPRESS_CACHE_TTL);
        assert!(Arc::ptr_eq(&first.inner, &second.inner));
        assert_ne!(first.owner, second.owner);
        assert_eq!(
            first.inner.state.lock().max_size,
            1000,
            "the limit is set on creation"
        );

        let temp_dir = first.inner.temp_dir.clone();
        first
            .get_or_decompress(&dir, 1, DecompressedFileType::Data, &compressed_path)
            .await
            .unwrap();

        drop(first);
        assert!(temp_dir.exists(), "the second handle still uses the cache");

        drop(second);
        assert!(!temp_dir.exists(), "the last handle removes the directory");

        let next = DecompressCache::shared_in(&slot, &dir, 1000, DECOMPRESS_CACHE_TTL);
        assert_ne!(next.inner.temp_dir, temp_dir);
    }

    #[rstest]
    #[serial]
    fn removes_only_stale_temp_dirs(dir: PathBuf) {
        let make_dir = |name: &str, with_lock: bool| {
            let path = dir.join(name);
            std::fs::create_dir(&path).unwrap();
            std::fs::write(path.join("1.blk"), b"data").unwrap();
            if with_lock {
                std::fs::write(path.join(LOCK_FILE_NAME), b"").unwrap();
            }
            path
        };
        let dead_owner = make_dir("reductstore_decompress_cache_1_1", true);
        let live_owner = make_dir("reductstore_decompress_cache_2_2", true);
        let unlocked = make_dir("reductstore_decompress_cache_3_3", false);
        let unrelated = make_dir("other_dir", true);

        let live_lock = File::open(live_owner.join(LOCK_FILE_NAME)).unwrap();
        live_lock.try_lock().unwrap();

        remove_stale_temp_dirs(&dir, UNLOCKED_DIR_MAX_IDLE);

        assert!(!dead_owner.exists());
        assert!(live_owner.exists());
        assert!(
            unlocked.exists(),
            "recently used directory without lock is kept"
        );
        assert!(unrelated.exists());

        std::thread::sleep(Duration::from_millis(20));
        remove_stale_temp_dirs(&dir, Duration::from_millis(10));

        assert!(!unlocked.exists(), "idle directory without lock is removed");
        assert!(live_owner.exists());
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn live_cache_survives_stale_dir_cleanup(dir: PathBuf) {
        let compressed_path = compressed(&dir, "1.blk.zst", "content");
        let cache = cache_in(&dir, 1000, DECOMPRESS_CACHE_TTL);
        let file = cache
            .get_or_decompress(&dir, 1, DecompressedFileType::Data, &compressed_path)
            .await
            .unwrap();

        remove_stale_temp_dirs(&dir, Duration::ZERO);

        assert!(file.path().exists());
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn concurrent_readers_of_same_block_keep_one_file(dir: PathBuf) {
        let compressed_path = compressed(&dir, "1.blk.zst", "content");
        let cache = Arc::new(cache_in(&dir, 1000, DECOMPRESS_CACHE_TTL));

        // Hold the compressed file: both readers check the cache in their first poll
        // and wait for the file, so the second one finds the block already cached
        let guard = FILE_CACHE
            .read(&compressed_path, SeekFrom::Start(0))
            .await
            .unwrap();
        let readers = (0..2)
            .map(|_| {
                let (cache, dir, compressed_path) =
                    (Arc::clone(&cache), dir.clone(), compressed_path.clone());
                tokio::spawn(async move {
                    cache
                        .get_or_decompress(&dir, 1, DecompressedFileType::Data, &compressed_path)
                        .await
                        .unwrap()
                })
            })
            .collect::<Vec<_>>();
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        drop(guard);

        let mut files = vec![];
        for reader in readers {
            files.push(reader.await.unwrap());
        }

        assert_eq!(files[0].path(), files[1].path());
        assert_eq!(
            cached_files(&cache.inner.temp_dir),
            vec![files[0].path().clone()],
            "the second decompressed copy is removed"
        );
        assert_eq!(cache.inner.state.lock().total_size(), 7);
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn get_or_decompress_returns_error_when_block_is_unreadable(dir: PathBuf) {
        // A directory instead of the compressed block: it can't be read as a file
        let compressed_path = dir.join("1.blk.zst");
        std::fs::create_dir(&compressed_path).unwrap();
        let cache = cache_in(&dir, 1000, DECOMPRESS_CACHE_TTL);

        let result = cache
            .get_or_decompress(&dir, 1, DecompressedFileType::Data, &compressed_path)
            .await;

        let err = result.unwrap_err();
        assert_eq!(err.status(), ErrorCode::InternalServerError);
        // On Windows a directory can't even be opened, so the read itself isn't reached
        if !cfg!(windows) {
            assert!(err.message.contains("Failed to read compressed file"));
        }
        assert!(!cache.inner.temp_dir.exists(), "nothing decompressed");
        assert_eq!(cache.inner.state.lock().total_size(), 0);
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn get_or_decompress_returns_error_when_lock_file_cannot_be_created(dir: PathBuf) {
        let compressed_path = compressed(&dir, "1.blk.zst", "content");
        let cache = cache_in(&dir, 1000, DECOMPRESS_CACHE_TTL);
        std::fs::create_dir_all(cache.inner.temp_dir.join(LOCK_FILE_NAME)).unwrap();

        let err = cache
            .get_or_decompress(&dir, 1, DecompressedFileType::Data, &compressed_path)
            .await
            .unwrap_err();

        assert_eq!(err.status(), ErrorCode::InternalServerError);
        assert!(err.message.contains("Failed to create lock file"));
        assert!(cached_files(&cache.inner.temp_dir).is_empty());
        assert!(cache.inner.state.lock().dir_lock.is_none());
    }

    #[cfg(unix)]
    #[rstest]
    #[tokio::test]
    #[serial]
    async fn failed_temp_file_create_is_not_cached(dir: PathBuf) {
        use std::os::unix::fs::PermissionsExt;

        let first_path = compressed(&dir, "1.blk.zst", "first");
        let second_path = compressed(&dir, "2.blk.zst", "second");
        let cache = cache_in(&dir, 1000, DECOMPRESS_CACHE_TTL);
        let first = cache
            .get_or_decompress(&dir, 1, DecompressedFileType::Data, &first_path)
            .await
            .unwrap();

        let temp_dir = cache.inner.temp_dir.clone();
        std::fs::set_permissions(&temp_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
        if File::create(temp_dir.join("probe")).is_ok() {
            // Running as root: permissions don't prevent writing
            std::fs::set_permissions(&temp_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
            return;
        }
        let result = cache
            .get_or_decompress(&dir, 2, DecompressedFileType::Data, &second_path)
            .await;
        std::fs::set_permissions(&temp_dir, std::fs::Permissions::from_mode(0o755)).unwrap();

        let err = result.unwrap_err();
        assert_eq!(err.status(), ErrorCode::InternalServerError);
        assert!(err
            .message
            .contains("Failed to create decompressed temporary file"));
        assert_eq!(cached_files(&temp_dir), vec![first.path().clone()]);
        assert_eq!(cache.inner.state.lock().total_size(), 5);
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn cleanup_worker_stops_when_cache_is_dropped(dir: PathBuf) {
        let compressed_path = compressed(&dir, "1.blk.zst", "content");
        // Initialize the global file cache first, it starts its own worker
        drop(
            FILE_CACHE
                .read(&compressed_path, SeekFrom::Start(0))
                .await
                .unwrap(),
        );
        let metrics = tokio::runtime::Handle::current().metrics();
        let tasks_before = metrics.num_alive_tasks();

        let cache = cache_in(&dir, 1000, Duration::from_millis(20));
        cache
            .get_or_decompress(&dir, 1, DecompressedFileType::Data, &compressed_path)
            .await
            .unwrap();
        assert_eq!(metrics.num_alive_tasks(), tasks_before + 1);

        drop(cache);
        for _ in 0..100 {
            if metrics.num_alive_tasks() == tasks_before {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(metrics.num_alive_tasks(), tasks_before);
    }
}
