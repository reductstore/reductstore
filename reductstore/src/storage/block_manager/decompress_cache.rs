// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::core::file_cache::FILE_CACHE;
use log::debug;
use parking_lot::Mutex;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{Read, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub(crate) const DECOMPRESS_CACHE_DEFAULT_SIZE: u64 = 1_000_000_000;
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
/// Block managers use their own handles (see [`DecompressCache::handle`]), so a re-created entry
/// never gets files decompressed for the previous one under the same path.
pub(crate) static DECOMPRESS_CACHE: LazyLock<DecompressCache> = LazyLock::new(|| {
    remove_stale_temp_dirs(&std::env::temp_dir(), UNLOCKED_DIR_MAX_IDLE);
    DecompressCache::new(
        temp_dir_path(&std::env::temp_dir()),
        DECOMPRESS_CACHE_DEFAULT_SIZE,
        DECOMPRESS_CACHE_TTL,
    )
});

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

struct CachedFile {
    path: PathBuf,
    size: u64,
    last_access: Instant,
}

struct CacheState {
    files: HashMap<String, CachedFile>,
    total_size: u64,
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

pub(crate) struct DecompressCache {
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
                    total_size: 0,
                    max_size,
                    dir_lock: None,
                }),
                temp_dir,
                ttl,
                cleanup_started: AtomicBool::new(false),
            }),
        }
    }

    /// Create a handle with its own keys that shares the files and the size limit with this cache.
    pub(crate) fn handle(&self) -> Self {
        Self {
            owner: NEXT_OWNER_ID.fetch_add(1, Ordering::Relaxed),
            inner: Arc::clone(&self.inner),
        }
    }

    /// Set the maximum total size of decompressed files on disk in bytes.
    pub(crate) fn set_max_size(&self, max_size: u64) {
        let evicted = {
            let mut state = self.inner.state.lock();
            state.max_size = max_size;
            state.evict_over_limit(None)
        };
        remove_cached_files(evicted);
    }

    pub(crate) async fn get_or_decompress(
        &self,
        entry_path: &Path,
        block_id: u64,
        file_type: DecompressedFileType,
        compressed_path: &PathBuf,
    ) -> Result<PathBuf, ReductError> {
        self.start_cleanup_worker();

        let key = self.key(entry_path, block_id, file_type);
        if let Some(path) = self.inner.state.lock().get(&key) {
            return Ok(path);
        }

        // Decompress without holding the lock, so that one slow block doesn't block reads of other entries.
        // Invalidation needs `&mut BlockManager`, so it can't interleave with a read of the same entry.
        let (path, size) = self
            .decompress_to_temp(entry_path, block_id, file_type, compressed_path)
            .await?;

        let (path, evicted) = {
            let mut state = self.inner.state.lock();
            if let Some(existing) = state.get(&key) {
                // Another reader decompressed the same block in the meantime
                (existing, vec![CachedFile::new(path, size)])
            } else {
                state.insert(key.clone(), CachedFile::new(path.clone(), size));
                (path, state.evict_over_limit(Some(&key)))
            }
        };

        remove_cached_files(evicted);
        Ok(path)
    }

    /// Remove cached decompressed files for a block.
    pub(crate) async fn invalidate(&self, entry_path: &Path, block_id: u64) {
        let removed = {
            let mut state = self.inner.state.lock();
            [DecompressedFileType::Data, DecompressedFileType::Descriptor]
                .into_iter()
                .filter_map(|file_type| state.remove(&self.key(entry_path, block_id, file_type)))
                .collect::<Vec<_>>()
        };
        remove_cached_files(removed);
    }

    /// Remove all decompressed files and the temporary directory (on shutdown).
    pub(crate) fn clear(&self) {
        let mut state = self.inner.state.lock();
        state.files.clear();
        state.total_size = 0;
        state.dir_lock = None;
        cleanup_tmp_dir(&self.inner.temp_dir);
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
                inner.discard_expired();
            }
        });
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

impl Inner {
    fn discard_expired(&self) {
        let expired = self.state.lock().remove_expired(self.ttl);
        remove_cached_files(expired);
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        // Release the lock file first, otherwise the directory can't be removed on Windows
        self.state.get_mut().dir_lock = None;
        cleanup_tmp_dir(&self.temp_dir);
    }
}

impl CachedFile {
    fn new(path: PathBuf, size: u64) -> Self {
        Self {
            path,
            size,
            last_access: Instant::now(),
        }
    }
}

impl CacheState {
    fn get(&mut self, key: &str) -> Option<PathBuf> {
        self.files.get_mut(key).map(|file| {
            file.last_access = Instant::now();
            file.path.clone()
        })
    }

    fn insert(&mut self, key: String, file: CachedFile) {
        self.total_size += file.size;
        if let Some(old) = self.files.insert(key, file) {
            self.total_size -= old.size;
        }
    }

    fn remove(&mut self, key: &str) -> Option<CachedFile> {
        let file = self.files.remove(key)?;
        self.total_size -= file.size;
        Some(file)
    }

    fn remove_expired(&mut self, ttl: Duration) -> Vec<CachedFile> {
        let expired = self
            .files
            .iter()
            .filter(|(_, file)| file.last_access.elapsed() > ttl)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        expired.iter().filter_map(|key| self.remove(key)).collect()
    }

    /// Evict the least recently used files until the total size fits the limit.
    ///
    /// The file in `keep` has just been handed out to a reader, so it stays
    /// even if it alone is larger than the limit.
    fn evict_over_limit(&mut self, keep: Option<&str>) -> Vec<CachedFile> {
        let mut evicted = Vec::new();
        while self.total_size > self.max_size {
            let oldest = self
                .files
                .iter()
                .filter(|(key, _)| Some(key.as_str()) != keep)
                .min_by_key(|(_, file)| file.last_access)
                .map(|(key, _)| key.clone());
            match oldest.and_then(|key| self.remove(&key)) {
                Some(file) => evicted.push(file),
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

fn remove_cached_files(files: Vec<CachedFile>) {
    for file in files {
        // Only unlink the file: a reader in the middle of a record keeps reading it
        // through the descriptor in FILE_CACHE, which frees the space when it closes the file.
        cleanup_tmp(&file.path);
    }
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

        assert_eq!(path, cached_path);
        assert_eq!(path.parent().unwrap(), cache.inner.temp_dir);
        assert_eq!(std::fs::read(path).unwrap(), b"content");
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
            std::fs::read_to_string(&path).unwrap(),
            "descriptor content"
        );
        assert!(path.to_str().unwrap().ends_with(".meta"));
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
        assert_eq!(cache.inner.state.lock().total_size, 0);
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
            .unwrap();
        let cached_desc = cache
            .get_or_decompress(&dir, 1, DecompressedFileType::Descriptor, &desc_path)
            .await
            .unwrap();

        cache.invalidate(&dir, 1).await;

        assert!(!cached_data.exists());
        assert!(!cached_desc.exists());
        assert_eq!(cache.inner.state.lock().total_size, 0);
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
                    .unwrap(),
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
                .unwrap(),
        );

        assert!(paths[0].exists());
        assert!(!paths[1].exists());
        assert!(paths[2].exists());
        assert_eq!(cache.inner.state.lock().total_size, 8);
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
            .unwrap();
        let new_path = new_manager
            .get_or_decompress(&dir, 1, DecompressedFileType::Data, &compressed_path)
            .await
            .unwrap();

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
            .unwrap();
        let large = cache
            .get_or_decompress(&dir, 2, DecompressedFileType::Data, &large_path)
            .await
            .unwrap();

        assert!(!small.exists());
        assert_eq!(std::fs::read(large).unwrap(), b"1234567890");
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn set_max_size_evicts_files_over_new_limit(dir: PathBuf) {
        let compressed_path = compressed(&dir, "1.blk.zst", "1234");
        let cache = cache_in(&dir, 100, DECOMPRESS_CACHE_TTL);
        let path = cache
            .get_or_decompress(&dir, 1, DecompressedFileType::Data, &compressed_path)
            .await
            .unwrap();

        cache.set_max_size(2);

        assert!(!path.exists());
        assert_eq!(cache.inner.state.lock().total_size, 0);
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
            .unwrap();
        assert!(path.exists());
        // Poll instead of a fixed sleep to stay stable on slow CI runners
        for _ in 0..100 {
            if !path.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        assert!(!path.exists());
        assert_eq!(cache.inner.state.lock().total_size, 0);
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn reader_finishes_record_after_file_expires(dir: PathBuf) {
        let compressed_path = compressed(&dir, "1.blk.zst", "content");
        let cache = cache_in(&dir, 1000, Duration::from_millis(20));
        let path = cache
            .get_or_decompress(&dir, 1, DecompressedFileType::Data, &compressed_path)
            .await
            .unwrap();
        // The reader opens the file for the first chunk, as read_in_chunks does
        drop(FILE_CACHE.read(&path, SeekFrom::Start(0)).await.unwrap());

        // On Windows a deleted file stays visible while it is open, so check the path elsewhere only
        let removed =
            || cache.inner.state.lock().total_size == 0 && (cfg!(windows) || !path.exists());
        for _ in 0..100 {
            if removed() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(removed());

        let mut content = String::new();
        FILE_CACHE
            .read(&path, SeekFrom::Start(0))
            .await
            .unwrap()
            .read_to_string(&mut content)
            .unwrap();
        assert_eq!(content, "content");
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn clear_and_drop_remove_temp_dir(dir: PathBuf) {
        let compressed_path = compressed(&dir, "1.blk.zst", "content");
        for clear in [true, false] {
            let cache = cache_in(&dir, 1000, DECOMPRESS_CACHE_TTL);
            let temp_dir = cache.inner.temp_dir.clone();
            cache
                .get_or_decompress(&dir, 1, DecompressedFileType::Data, &compressed_path)
                .await
                .unwrap();
            assert!(temp_dir.exists());

            if clear {
                cache.clear();
            } else {
                drop(cache);
            }

            assert!(!temp_dir.exists());
        }
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
        let path = cache
            .get_or_decompress(&dir, 1, DecompressedFileType::Data, &compressed_path)
            .await
            .unwrap();

        remove_stale_temp_dirs(&dir, Duration::ZERO);

        assert!(path.exists());
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

        let mut paths = vec![];
        for reader in readers {
            paths.push(reader.await.unwrap());
        }

        assert_eq!(paths[0], paths[1]);
        assert_eq!(
            cached_files(&cache.inner.temp_dir),
            vec![paths[0].clone()],
            "the second decompressed copy is removed"
        );
        assert_eq!(cache.inner.state.lock().total_size, 7);
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
        assert_eq!(cache.inner.state.lock().total_size, 0);
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
        assert_eq!(cached_files(&temp_dir), vec![first]);
        assert_eq!(cache.inner.state.lock().total_size, 5);
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
