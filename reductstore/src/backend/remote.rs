// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod local_cache;

#[cfg(feature = "s3-backend")]
mod s3_connector;

use crate::backend::file::AccessMode;
use crate::backend::remote::local_cache::LocalCache;
use crate::backend::{BackendType, StorageBackend};
use log::debug;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Instant;

#[cfg(feature = "s3-backend")]
use crate::backend::remote::s3_connector::S3Connector;

#[allow(dead_code)]
pub(super) trait RemoteStorageConnector {
    fn download_object(&self, key: &str, dest: &PathBuf) -> Result<(), io::Error>;
    fn upload_object(&self, key: &str, src: &PathBuf) -> Result<(), io::Error>;
    fn create_dir_all(&self, key: &str) -> Result<(), io::Error>;
    fn list_objects(&self, key: &str, recursive: bool) -> Result<Vec<String>, io::Error>;
    fn remove_object(&self, key: &str) -> Result<(), io::Error>;
    fn head_object(&self, key: &str) -> Result<bool, io::Error>;
    fn rename_object(&self, from: &str, to: &str) -> Result<(), io::Error>;
}

#[allow(dead_code)]
pub(crate) struct RemoteBackendSettings {
    pub connector_type: BackendType,
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

pub(crate) struct RemoteBackend {
    cache_path: PathBuf,
    connector: Box<dyn RemoteStorageConnector + Send + Sync>,
    local_cache: Mutex<LocalCache>,
}

impl RemoteBackend {
    #[allow(dead_code, unused_variables)]
    pub fn new(settings: RemoteBackendSettings) -> Self {
        let cache_path = settings.cache_path.clone();
        let local_cache = Mutex::new(LocalCache::new(cache_path.clone(), settings.cache_size));

        let connector = match settings.connector_type {
            #[cfg(feature = "s3-backend")]
            BackendType::S3 => Box::new(S3Connector::new(settings)),
            #[cfg(feature = "fs-backend")]
            BackendType::Filesystem =>
            // panic because we shouldn't be here if filesystem is selected
                panic!("Filesystem remote storage backend is not supported, falling back to S3 connector"),
        };

        #[allow(unreachable_code)]
        RemoteBackend {
            cache_path,
            connector,
            local_cache,
        }
    }
}

impl StorageBackend for RemoteBackend {
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
        for key in self.connector.list_objects(&from_key, true)? {
            self.connector.rename_object(
                &format!("{}/{}", from_key, key),
                &format!("{}/{}", to_key, key),
            )?;
        }

        if to.is_dir() {
            self.connector
                .rename_object(&format!("{}/", from_key), &format!("{}/", to_key))?;
        } else {
            self.connector.rename_object(&from_key, &to_key)?;
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
        self.connector.remove_object(&s3_key)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        let s3_key = {
            let cache = &mut self.local_cache.lock().unwrap();
            cache.remove_all(&path)?;
            cache.build_key(path)
        };

        debug!("Removing S3 directory for key: {}", s3_key);
        // at least minio doesn't remove folders recursively, so we need to list and remove all objects
        for key in self.connector.list_objects(&s3_key, true)? {
            self.connector
                .remove_object(&format!("{}/{}", s3_key, key))?;
        }

        self.connector.remove_object(&format!("{}/", s3_key))
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

        self.connector.create_dir_all(&s3_key)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        let cache = &self.local_cache.lock().unwrap();
        let s3_key = cache.build_key(path);

        let mut paths = vec![];
        for key in self.connector.list_objects(&s3_key, false)? {
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
        // check cache first and then load from remote if not in cache
        let s3_key = {
            let cache = &self.local_cache.lock().unwrap();
            if cache.try_exists(path)? {
                return Ok(true);
            }

            cache.build_key(path)
        };

        debug!("Checking S3 key: {} to local path: {:?}", s3_key, path);
        self.connector.head_object(&s3_key)
    }

    fn upload(&self, full_path: &Path) -> io::Result<()> {
        // upload to remote
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
        self.connector
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
        self.connector.download_object(&s3_key, &full_path)?;
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
