// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod local_cache;

#[cfg(feature = "s3-backend")]
mod s3_connector;

use crate::backend::file::AccessMode;
use crate::backend::remote::local_cache::LocalCache;
#[cfg(feature = "s3-backend")]
use crate::backend::remote::s3_connector::S3Connector;
use crate::backend::{BackendType, StorageBackend};
use log::debug;
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Instant;

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
    pub backend_type: BackendType,
    pub cache_path: PathBuf,
    pub cache_size: u64,
    pub endpoint: Option<String>,
    pub access_key: String,
    pub secret_key: String,
    pub region: Option<String>,
    pub bucket: String,
    pub default_storage_class: Option<String>,
}

struct LocalCacheEntry {
    last_accessed: Instant,
    size: u64,
}

pub(crate) struct RemoteBackend {
    cache_path: PathBuf,
    connector: Box<dyn RemoteStorageConnector + Send + Sync>,
    local_cache: Mutex<LocalCache>,
    #[allow(dead_code)]
    backend_type: BackendType,
}

impl RemoteBackend {
    #[allow(dead_code, unused_variables)]
    pub fn new(settings: RemoteBackendSettings) -> Self {
        let cache_path = settings.cache_path.clone();
        let local_cache = Mutex::new(LocalCache::new(cache_path.clone(), settings.cache_size));
        let backend_type = settings.backend_type.clone();

        let connector = match settings.backend_type {
            #[cfg(feature = "s3-backend")]
            BackendType::S3 => Box::new(S3Connector::new(settings)),
            #[cfg(feature = "fs-backend")]
            BackendType::Filesystem =>
            // panic because we shouldn't be here if filesystem is selected
                panic!("Filesystem remote storage backend is not supported, falling back to S3 connector"),
            #[allow(unreachable_patterns)]
            _ => panic!("Unsupported remote storage backend"),
        };

        #[allow(unreachable_code)]
        RemoteBackend {
            cache_path,
            connector,
            local_cache,
            backend_type,
        }
    }

    #[cfg(test)]
    fn new_test(
        connector: Box<dyn RemoteStorageConnector + Send + Sync>,
        cache_path: PathBuf,
        cache_size: u64,
    ) -> Self {
        let local_cache = Mutex::new(LocalCache::new(cache_path.clone(), cache_size));

        RemoteBackend {
            cache_path,
            connector,
            local_cache,
            backend_type: BackendType::S3, // for tests we can assume S3
        }
    }
}

impl StorageBackend for RemoteBackend {
    fn path(&self) -> &PathBuf {
        &self.cache_path
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        if self.backend_type == BackendType::S3 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Renaming in S3 backend is not supported",
            ));
        }

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

#[cfg(test)]
mod tests {
    use super::*;
    use mockall::mock;
    use rstest::*;
    use tempfile::tempdir;

    #[rstest]
    fn test_remote_backend_creation(mock_connector: MockRemoteStorageConnector, path: PathBuf) {
        let remote_backend = make_remote_backend(mock_connector, path.clone());
        assert_eq!(remote_backend.path(), &path);
    }

    mod rename {
        use super::*;
        use std::fs;
        use std::path::PathBuf;

        #[rstest]
        fn test_rename_file(mock_connector: MockRemoteStorageConnector, path: PathBuf) {
            let from_key = "file1.txt";
            let to_key = "file2.txt";

            let remote_backend = make_remote_backend(mock_connector, path.clone());

            let from = path.join(from_key);
            let to = path.join(to_key);
            fs::create_dir_all(&path).unwrap();
            fs::write(&from, b"test").unwrap();

            assert_eq!(
                remote_backend.rename(&from, &to).err().unwrap().to_string(),
                "Renaming in S3 backend is not supported"
            );
        }

        #[rstest]
        fn test_rename_directory(mock_connector: MockRemoteStorageConnector, path: PathBuf) {
            let from_key = "dir1/";
            let to_key = "dir2/";

            let remote_backend = make_remote_backend(mock_connector, path.clone());

            let from = path.join(from_key);
            let to = path.join(to_key);
            fs::create_dir_all(&from).unwrap();
            fs::write(from.join("file_in_dir.txt"), b"test").unwrap();

            assert_eq!(
                remote_backend.rename(&from, &to).err().unwrap().to_string(),
                "Renaming in S3 backend is not supported"
            );
        }
    }

    mod remove {
        use super::*;
        use mockall::predicate::eq;
        use std::fs;
        use std::path::PathBuf;

        #[rstest]
        fn test_remove_file(mut mock_connector: MockRemoteStorageConnector, path: PathBuf) {
            let key = "file1.txt";

            mock_connector
                .expect_remove_object()
                .with(eq(key))
                .times(1)
                .returning(|_| Ok(()));

            let remote_backend = make_remote_backend(mock_connector, path.clone());

            let file_path = path.join(key);
            fs::create_dir_all(&path).unwrap();
            fs::write(&file_path, b"test").unwrap();

            remote_backend.remove(&file_path).unwrap();
        }

        #[rstest]
        fn test_remove_directory(mut mock_connector: MockRemoteStorageConnector, path: PathBuf) {
            let dir_key = "dir1/";
            let file_key = "dir1/file_in_dir.txt";

            mock_connector
                .expect_list_objects()
                .with(eq("dir1"), eq(true))
                .times(1)
                .returning(|_, _| Ok(vec!["file_in_dir.txt".to_string()]));

            mock_connector
                .expect_remove_object()
                .with(eq(file_key))
                .times(1)
                .returning(|_| Ok(()));

            mock_connector
                .expect_remove_object()
                .with(eq(dir_key))
                .times(1)
                .returning(|_| Ok(()));

            let remote_backend = make_remote_backend(mock_connector, path.clone());

            let dir_path = path.join("dir1");
            fs::create_dir_all(&dir_path).unwrap();
            fs::write(dir_path.join("file_in_dir.txt"), b"test").unwrap();

            remote_backend.remove_dir_all(&dir_path).unwrap();
        }
    }

    mod create {
        use super::*;
        use mockall::predicate::eq;
        use std::path::PathBuf;

        #[rstest]
        fn test_create_dir_all(mut mock_connector: MockRemoteStorageConnector, path: PathBuf) {
            mock_connector
                .expect_create_dir_all()
                .with(eq("dir1"))
                .times(1)
                .returning(|_| Ok(()));

            let remote_backend = make_remote_backend(mock_connector, path.clone());

            let dir_path = path.join("dir1");
            remote_backend.create_dir_all(&dir_path).unwrap();
            assert!(dir_path.exists());
            assert!(dir_path.is_dir());
        }
    }

    mod read_dir {
        use super::*;
        use mockall::predicate::eq;
        use std::fs;
        use std::path::PathBuf;

        #[rstest]
        fn test_read_dir(mut mock_connector: MockRemoteStorageConnector, path: PathBuf) {
            mock_connector
                .expect_list_objects()
                .with(eq("dir1"), eq(false))
                .times(1)
                .returning(|_, _| Ok(vec!["file1.txt".to_string(), "subdir/".to_string()]));

            let remote_backend = make_remote_backend(mock_connector, path.clone());

            let dir_path = path.join("dir1");
            fs::create_dir_all(&dir_path).unwrap();

            let entries = remote_backend.read_dir(&dir_path).unwrap();
            assert_eq!(entries.len(), 2);
            assert!(entries.contains(&dir_path.join("file1.txt")));
            assert!(entries.contains(&dir_path.join("subdir")));
            assert!(dir_path.join("subdir").is_dir());
        }
    }

    mod try_exists {
        use super::*;
        use mockall::predicate::eq;
        use std::fs;
        use std::path::PathBuf;

        #[rstest]
        fn test_try_exists_file_in_cache(
            mock_connector: MockRemoteStorageConnector,
            path: PathBuf,
        ) {
            let file_key = "file1.txt";
            let remote_backend = make_remote_backend(mock_connector, path.clone());

            let file_path = path.join(file_key);
            fs::create_dir_all(&path).unwrap();
            fs::write(&file_path, b"test").unwrap();

            assert!(remote_backend.try_exists(&file_path).unwrap());
        }

        #[rstest]
        fn test_try_exists_file_in_remote(
            mut mock_connector: MockRemoteStorageConnector,
            path: PathBuf,
        ) {
            let file_key = "file1.txt";

            mock_connector
                .expect_head_object()
                .with(eq(file_key))
                .times(1)
                .returning(|_| Ok(true));

            let remote_backend = make_remote_backend(mock_connector, path.clone());

            let file_path = path.join(file_key);
            assert!(remote_backend.try_exists(&file_path).unwrap());
        }

        #[rstest]
        fn test_try_exists_file_not_exist(
            mut mock_connector: MockRemoteStorageConnector,
            path: PathBuf,
        ) {
            let file_key = "file1.txt";

            mock_connector
                .expect_head_object()
                .with(eq(file_key))
                .times(1)
                .returning(|_| Ok(false));

            let remote_backend = make_remote_backend(mock_connector, path.clone());

            let file_path = path.join(file_key);
            assert!(!remote_backend.try_exists(&file_path).unwrap());
        }
    }

    mod upload {
        use super::*;
        use mockall::predicate::eq;
        use std::fs;
        use std::path::PathBuf;

        #[rstest]
        fn test_upload_file(mut mock_connector: MockRemoteStorageConnector, path: PathBuf) {
            let file_key = "file1.txt";

            mock_connector
                .expect_upload_object()
                .with(eq(file_key), eq(path.join(file_key).to_path_buf()))
                .times(1)
                .returning(|_, _| Ok(()));

            let remote_backend = make_remote_backend(mock_connector, path.clone());

            let file_path = path.join(file_key);
            fs::create_dir_all(&path).unwrap();
            fs::write(&file_path, b"test").unwrap();

            remote_backend.upload(&file_path).unwrap();
        }
    }

    mod download {
        use super::*;
        use mockall::predicate::eq;
        use std::fs;
        use std::path::PathBuf;

        #[rstest]
        fn test_download_file(mut mock_connector: MockRemoteStorageConnector, path: PathBuf) {
            let file_key = "file1.txt";
            let file_path = path.join(file_key);

            mock_connector
                .expect_download_object()
                .with(eq(file_key), eq(path.join(file_key).to_path_buf()))
                .times(1)
                .returning(|_, path| {
                    fs::create_dir_all(path.parent().unwrap()).unwrap();
                    fs::write(path.clone(), b"test").unwrap(); // we need to create the file to register it in cache
                    Ok(())
                });

            let remote_backend = make_remote_backend(mock_connector, path.clone());

            remote_backend.download(&file_path).unwrap();
        }
    }

    mod update_local_cache {
        use super::*;
        use std::fs;
        use std::path::PathBuf;

        #[rstest]
        fn test_update_local_cache(path: PathBuf) {
            let remote_backend =
                make_remote_backend(MockRemoteStorageConnector::new(), path.clone());

            let file_key = "file1.txt";
            let file_path = path.join(file_key);
            fs::create_dir_all(&path).unwrap();
            fs::write(&file_path, b"test").unwrap();

            assert!(remote_backend
                .update_local_cache(&file_path, &AccessMode::Read)
                .is_ok());
        }
    }

    mod invalidate_locally_cached_files {
        use super::*;
        use std::fs;
        use std::path::PathBuf;

        #[rstest]
        fn test_invalidate_locally_cached_files(path: PathBuf) {
            let remote_backend = make_remote_backend_with_size(
                MockRemoteStorageConnector::new(),
                path.clone(),
                1, // 1 byte cache size to force invalidation
            );

            let file_key = "file1.txt";
            let file_path = path.join(file_key);
            fs::create_dir_all(&path).unwrap();
            fs::write(&file_path, b"test").unwrap();

            remote_backend
                .update_local_cache(&file_path, &AccessMode::Read)
                .unwrap();
            let invalidated_files = remote_backend.invalidate_locally_cached_files();
            assert_eq!(invalidated_files, vec![file_path])
        }
    }

    mock! {
        pub RemoteStorageConnector {}

        impl RemoteStorageConnector for RemoteStorageConnector {
            fn download_object(&self, key: &str, dest: &PathBuf) -> Result<(), io::Error>;
            fn upload_object(&self, key: &str, src: &PathBuf) -> Result<(), io::Error>;
            fn create_dir_all(&self, key: &str) -> Result<(), io::Error>;
            fn list_objects(&self, key: &str, recursive: bool) -> Result<Vec<String>, io::Error>;
            fn remove_object(&self, key: &str) -> Result<(), io::Error>;
            fn head_object(&self, key: &str) -> Result<bool, io::Error>;
            fn rename_object(&self, from: &str, to: &str) -> Result<(), io::Error>;
        }
    }

    #[fixture]
    fn mock_connector() -> MockRemoteStorageConnector {
        let mock = MockRemoteStorageConnector::new();
        mock
    }

    #[fixture]
    fn path() -> PathBuf {
        tempdir().unwrap().keep()
    }

    fn make_remote_backend(
        mock_connector: MockRemoteStorageConnector,
        path: PathBuf,
    ) -> RemoteBackend {
        let cache_size = 10 * 1024 * 1024; // 10 MB
        make_remote_backend_with_size(mock_connector, path, cache_size)
    }

    fn make_remote_backend_with_size(
        mock_connector: MockRemoteStorageConnector,
        path: PathBuf,
        cache_size: u64,
    ) -> RemoteBackend {
        RemoteBackend::new_test(Box::new(mock_connector), path, cache_size)
    }
}
