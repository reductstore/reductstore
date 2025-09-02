// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod wrapper;

use crate::backend::s3::wrapper::S3ClientWrapper;
use crate::backend::StorageBackend;
use crate::fs::AccessMode;
use log::{debug, info, warn};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Instant;
use std::{fs, io};

pub(crate) struct S3BackendSettings {
    pub cache_path: PathBuf,
    pub cache_size: u64,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    pub bucket: String,
}

struct LocalCacheEntry {
    last_accessed: Instant,
    size: u64,
}

pub(crate) struct S3Backend {
    cache_path: PathBuf,
    wrapper: S3ClientWrapper,
    local_cache: Mutex<LocalCache>,
}

struct LocalCache {
    path: PathBuf,
    max_size: u64,
    current_size: u64,
    entries: HashMap<PathBuf, LocalCacheEntry>,
}

impl LocalCache {
    fn new(path: PathBuf, max_size: u64) -> Self {
        info!("Cleaning up local cache at {:?}", path);
        if let Err(err) = fs::remove_dir_all(&path) {
            if err.kind() != io::ErrorKind::NotFound {
                warn!("Failed to clean up local cache at {:?}: {}", path, err);
            }
        }

        LocalCache {
            path,
            max_size,
            current_size: 0,
            entries: HashMap::new(),
        }
    }

    fn build_key(&self, path: &Path) -> String {
        path.strip_prefix(&self.path)
            .unwrap()
            .to_str()
            .unwrap_or("")
            .to_string()
    }

    fn remove(&mut self, path: &Path) -> io::Result<()> {
        if let Err(err) = fs::remove_file(path) {
            if err.kind() != io::ErrorKind::NotFound {
                return Err(err);
            }
        }

        if let Some(entry) = self.entries.remove(path) {
            self.current_size -= entry.size;
        }
        Ok(())
    }

    fn remove_all(&mut self, dir: &Path) -> io::Result<()> {
        fs::remove_dir_all(dir)?;

        let paths_to_remove: Vec<PathBuf> = self
            .entries
            .keys()
            .filter(|p| p.starts_with(dir))
            .cloned()
            .collect();

        for path in paths_to_remove {
            if let Err(err) = self.remove(&path) {
                warn!("Failed to remove cached file {:?}: {}", path, err);
            }
        }

        Ok(())
    }

    fn rename(&mut self, from: &Path, to: &Path) -> io::Result<()> {
        fs::rename(from, to)?;

        if let Some(entry) = self.entries.remove(from) {
            self.entries.insert(to.to_path_buf(), entry);
        }
        Ok(())
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        fs::create_dir_all(path)
    }

    fn try_exists(&self, path: &Path) -> io::Result<bool> {
        let full_path = self.path.join(path);
        full_path.try_exists()
    }

    fn register_file(&mut self, path: &Path) -> io::Result<()> {
        let metadata = fs::metadata(&path)?;
        let file_size = metadata.len();

        self.current_size += file_size;
        if let Some(previous) = self.entries.insert(
            path.to_path_buf(),
            LocalCacheEntry {
                last_accessed: Instant::now(),
                size: file_size,
            },
        ) {
            self.current_size -= previous.size;
        }

        Ok(())
    }

    fn access_file(&mut self, path: &Path) -> io::Result<()> {
        if let Some(entry) = self.entries.get_mut(path) {
            entry.last_accessed = Instant::now();
        } else {
            self.register_file(path)?;
        }
        Ok(())
    }

    fn invalidate_old_files(&mut self) -> Vec<PathBuf> {
        let mut removed_files = vec![];

        if self.current_size <= self.max_size {
            return removed_files;
        }

        let mut entries: Vec<(&PathBuf, &LocalCacheEntry)> = self.entries.iter().collect();
        entries.sort_by_key(|&(_, entry)| entry.last_accessed);

        for (path, entry) in entries {
            if self.current_size <= self.max_size {
                break;
            }

            self.current_size -= entry.size;
            removed_files.push(path.clone());
        }

        for path in &removed_files {
            self.entries.remove(path);
        }

        removed_files
    }
}

impl S3Backend {
    pub fn new(settings: S3BackendSettings) -> Self {
        let local_cache = Mutex::new(LocalCache::new(
            settings.cache_path.clone(),
            settings.cache_size,
        ));
        S3Backend {
            cache_path: settings.cache_path.clone(),
            wrapper: S3ClientWrapper::new(settings),
            local_cache,
        }
    }
}

impl StorageBackend for S3Backend {
    fn path(&self) -> &PathBuf {
        &self.cache_path
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let (from_key, to_key) = {
            let cache = &mut self.local_cache.lock().unwrap();
            cache.rename(from, to)?;
            let from_key = cache.build_key(from);
            let to_key = cache.build_key(to);
            (from_key, to_key)
        };

        debug!(
            "Renaming S3 object from key: {} to key: {}",
            from_key, to_key
        );
        // at least minio doesn't remove folders recursively, so we need to list and remove all objects
        for key in self.wrapper.list_objects(&from_key, true)? {
            self.wrapper.rename_object(
                &format!("{}/{}", from_key, key),
                &format!("{}/{}", to_key, key),
            )?;
        }

        if to.is_dir() {
            self.wrapper
                .rename_object(&format!("{}/", from_key), &format!("{}/", to_key))?;
        } else {
            self.wrapper.rename_object(&from_key, &to_key)?;
        }

        Ok(())
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        let s3_key = {
            let cache = &mut self.local_cache.lock().unwrap();
            cache.remove(&path)?;
            cache.build_key(path)
        };

        debug!("Removing S3 object for key: {}", s3_key);
        self.wrapper.remove_object(&s3_key)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        let s3_key = {
            let cache = &mut self.local_cache.lock().unwrap();
            cache.remove_all(&path)?;
            cache.build_key(path)
        };

        debug!("Removing S3 directory for key: {}", s3_key);
        // at least minio doesn't remove folders recursively, so we need to list and remove all objects
        for key in self.wrapper.list_objects(&s3_key, true)? {
            self.wrapper.remove_object(&format!("{}/{}", s3_key, key))?;
        }

        self.wrapper.remove_object(&format!("{}/", s3_key))
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        let s3_key = {
            let cache = &self.local_cache.lock().unwrap();
            cache.create_dir_all(&path)?;
            cache.build_key(path)
        };

        if s3_key.is_empty() {
            return Ok(());
        }

        self.wrapper.create_dir_all(&s3_key)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        let cache = &self.local_cache.lock().unwrap();
        let s3_key = cache.build_key(path);

        let mut paths = vec![];
        for key in self.wrapper.list_objects(&s3_key, false)? {
            if key == s3_key {
                continue;
            }

            let local_path = self.cache_path.join(path).join(&key);
            if key.ends_with('/') {
                debug!(
                    "Creating local directory {:?} for S3 key: {}",
                    local_path, key
                );
                cache.create_dir_all(&local_path)?;
            }

            paths.push(local_path);
        }
        Ok(paths)
    }

    fn try_exists(&self, path: &Path) -> io::Result<bool> {
        // check cache first and then load from s3 if not in cache
        let s3_key = {
            let cache = &self.local_cache.lock().unwrap();
            if cache.try_exists(path)? {
                return Ok(true);
            }

            cache.build_key(path)
        };

        debug!("Checking S3 key: {} to local path: {:?}", s3_key, path);
        self.wrapper.head_object(&s3_key)
    }

    fn upload(&self, full_path: &Path) -> io::Result<()> {
        // upload to s3
        let s3_key = {
            let cache = &mut self.local_cache.lock().unwrap();
            cache.register_file(full_path)?;
            cache.build_key(full_path)
        };

        let full_path = self.cache_path.join(full_path);
        debug!(
            "Syncing local file {} to S3 key: {}",
            full_path.display(),
            s3_key
        );
        self.wrapper
            .upload_object(&s3_key, &full_path.to_path_buf())?;

        Ok(())
    }

    fn download(&self, path: &Path) -> io::Result<()> {
        let s3_key = {
            let cache = &mut self.local_cache.lock().unwrap();
            if cache.try_exists(path)? {
                return Ok(());
            }
            cache.build_key(path)
        };

        let full_path = self.cache_path.join(path);
        debug!(
            "Downloading S3 key: {} to local path: {:?}",
            s3_key, full_path
        );
        self.wrapper.download_object(&s3_key, &full_path)?;
        self.local_cache.lock().unwrap().register_file(&full_path)?;
        Ok(())
    }

    fn update_local_cache(&self, path: &Path, _mode: &AccessMode) -> std::io::Result<()> {
        self.local_cache.lock().unwrap().access_file(path)
    }

    fn invalidate_locally_cached_files(&self) -> Vec<PathBuf> {
        let mut cache = self.local_cache.lock().unwrap();
        cache.invalidate_old_files()
    }
}
