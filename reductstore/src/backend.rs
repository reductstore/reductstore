// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#[cfg(feature = "fs-backend")]
pub(super) mod fs;

pub mod file;
mod noop;

use crate::backend::file::{AccessMode, OpenOptions};
use crate::backend::noop::NoopBackend;
use async_trait::async_trait;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

#[derive(Default)]
pub struct ObjectMetadata {
    #[allow(dead_code)]
    pub size: Option<i64>,
    pub modified_time: Option<SystemTime>,
}

#[async_trait]
pub trait StorageBackend {
    fn path(&self) -> &PathBuf;
    async fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()>;

    async fn remove(&self, path: &Path) -> std::io::Result<()>;

    async fn remove_dir_all(&self, path: &Path) -> std::io::Result<()>;

    async fn create_dir_all(&self, path: &Path) -> std::io::Result<()>;

    async fn read_dir(&self, path: &Path) -> std::io::Result<Vec<PathBuf>>;

    async fn try_exists(&self, _path: &Path) -> std::io::Result<bool>;

    async fn upload(&self, path: &Path) -> std::io::Result<()>;

    async fn download(&self, path: &Path) -> std::io::Result<()>;

    async fn update_local_cache(&self, path: &Path, mode: &AccessMode) -> std::io::Result<()>;

    async fn invalidate_locally_cached_files(&self) -> Vec<PathBuf>;

    async fn get_stats(&self, path: &Path) -> std::io::Result<Option<ObjectMetadata>>;

    async fn remove_from_local_cache(&self, path: &Path) -> std::io::Result<()>;
}

pub type BoxedBackend = Box<dyn StorageBackend + Send + Sync>;

#[derive(Default, Clone, Debug, PartialEq)]
pub enum BackendType {
    #[default]
    Filesystem,
    S3,
}

#[derive(Default)]
pub struct FsBackendBuilder {
    backend_type: BackendType,
    local_data_path: Option<PathBuf>,
}

impl FsBackendBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn backend_type(mut self, backend_type: BackendType) -> Self {
        self.backend_type = backend_type;
        self
    }

    pub fn local_data_path(mut self, path: PathBuf) -> Self {
        self.local_data_path = Some(path);
        self
    }


    pub async fn try_build(self) -> Result<Backend, ReductError> {
        let backend: BoxedBackend = match self.backend_type {
            BackendType::Filesystem => {
                let Some(data_path) = self.local_data_path else {
                    Err(internal_server_error!(
                        "local_data_path is required for Filesystem backend",
                    ))?
                };

                Box::new(fs::FileSystemBackend::new(PathBuf::from(data_path)))
            }
            _ => Err(internal_server_error!("Unsupported backend type"))?,
        };

        Ok(Backend {
            backend: Arc::new(backend),
        })
    }
}

#[derive(Clone)]
pub struct Backend {
    backend: Arc<BoxedBackend>,
}

impl Default for Backend {
    fn default() -> Self {
        Self {
            backend: Arc::new(Box::new(NoopBackend::new())),
        }
    }
}

impl Backend {
    pub fn builder() -> FsBackendBuilder {
        FsBackendBuilder::new()
    }

    pub fn from_backend(backend: BoxedBackend) -> Self {
        Self {
            backend: Arc::new(backend),
        }
    }

    /// Create a new instance of `fs::OpenOptions`.
    pub fn open_options(&self) -> OpenOptions {
        OpenOptions::new(Arc::clone(&self.backend))
    }

    pub async fn rename<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        from: P,
        to: Q,
    ) -> std::io::Result<()> {
        self.backend.rename(from.as_ref(), to.as_ref()).await
    }

    pub async fn remove<P: AsRef<Path>>(&self, path: P) -> std::io::Result<()> {
        self.backend.remove(path.as_ref()).await
    }

    pub async fn remove_dir_all<P: AsRef<Path>>(&self, path: P) -> std::io::Result<()> {
        self.backend.remove_dir_all(path.as_ref()).await
    }

    pub async fn create_dir_all<P: AsRef<Path>>(&self, path: P) -> std::io::Result<()> {
        self.backend.create_dir_all(path.as_ref()).await
    }

    pub async fn read_dir(&self, path: &PathBuf) -> std::io::Result<Vec<PathBuf>> {
        self.backend.read_dir(path).await
    }

    pub async fn try_exists<P: AsRef<Path>>(&self, path: P) -> std::io::Result<bool> {
        self.backend.try_exists(path.as_ref()).await
    }

    pub async fn get_stats<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> std::io::Result<Option<ObjectMetadata>> {
        self.backend.get_stats(path.as_ref()).await
    }

    pub async fn invalidate_locally_cached_files(&self) -> Vec<PathBuf> {
        self.backend.invalidate_locally_cached_files().await
    }

    /// Remove the file only from local cache, without affecting remote storage.
    /// This is useful for initiating re-download of the file on next access.
    pub async fn remove_from_local_cache<P: AsRef<Path>>(&self, path: P) -> std::io::Result<()> {
        self.backend.remove_from_local_cache(path.as_ref()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockall::mock;
    use rstest::*;
    use tempfile::tempdir;

    #[cfg(feature = "fs-backend")]
    mod fs {
        use super::*;
        #[rstest]
        #[tokio::test]
        async fn test_backend_builder_fs() {
            let backend = Backend::builder()
                .backend_type(BackendType::Filesystem)
                .local_data_path(PathBuf::from("/tmp/data"))
                .try_build()
                .await
                .expect("Failed to build Filesystem backend");
            assert_eq!(backend.backend.path(), &PathBuf::from("/tmp/data"));
        }
    }


    mod open {
        use super::*;
        #[rstest]
        #[tokio::test]
        async fn test_backend_open_options(mut mock_backend: MockBackend) {
            let path = mock_backend.path().join("test.txt").clone();

            // download because it is not cached yet
            mock_backend
                .expect_try_exists()
                .returning(move |_| Ok(true));
            mock_backend.expect_download().returning(move |p| {
                assert_eq!(path, p);
                Ok(())
            });

            let backend = build_backend(mock_backend);
            let file = backend
                .open_options()
                .create(true)
                .write(true)
                .open("test.txt")
                .await
                .unwrap();

            assert!(file.is_synced());
            assert_eq!(file.mode(), &AccessMode::ReadWrite);
            assert_eq!(file.metadata().unwrap().len(), 0);
        }
    }

    mod rename {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_backend_rename(mut mock_backend: MockBackend) {
            mock_backend
                .expect_rename()
                .returning(move |old_path, new_path| {
                    assert_eq!(old_path, Path::new("old_name.txt"));
                    assert_eq!(new_path, Path::new("new_name.txt"));
                    Ok(())
                });

            let backend = build_backend(mock_backend);
            backend
                .rename("old_name.txt", "new_name.txt")
                .await
                .unwrap();
        }
    }

    mod remove {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_backend_remove(mut mock_backend: MockBackend) {
            mock_backend.expect_remove().returning(move |path| {
                assert_eq!(path, Path::new("temp_file.txt"));
                Ok(())
            });

            let backend = build_backend(mock_backend);
            backend.remove("temp_file.txt").await.unwrap();
        }
    }

    mod remove_dir_all {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_backend_remove_dir_all(mut mock_backend: MockBackend) {
            mock_backend.expect_remove_dir_all().returning(move |path| {
                assert_eq!(path, Path::new("temp_dir"));
                Ok(())
            });

            let backend = build_backend(mock_backend);
            backend.remove_dir_all("temp_dir").await.unwrap();
        }
    }

    mod create_dir_all {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_backend_create_dir_all(mut mock_backend: MockBackend) {
            mock_backend.expect_create_dir_all().returning(move |path| {
                assert_eq!(path, Path::new("new_dir"));
                Ok(())
            });

            let backend = build_backend(mock_backend);
            backend.create_dir_all("new_dir").await.unwrap();
        }
    }

    mod read_dir {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_backend_read_dir(mut mock_backend: MockBackend) {
            let expected_files = vec![PathBuf::from("file1.txt"), PathBuf::from("file2.txt")];
            let copy_of_expected = expected_files.clone();
            mock_backend.expect_read_dir().returning(move |path| {
                assert_eq!(path, Path::new("some_dir"));
                Ok(copy_of_expected.clone())
            });

            let backend = build_backend(mock_backend);
            let files = backend.read_dir(&PathBuf::from("some_dir")).await.unwrap();
            assert_eq!(files, expected_files);
        }
    }

    mod try_exists {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_backend_try_exists(mut mock_backend: MockBackend) {
            mock_backend.expect_try_exists().returning(move |path| {
                assert_eq!(path, Path::new("existing_file.txt"));
                Ok(true)
            });

            let backend = build_backend(mock_backend);
            let exists = backend.try_exists("existing_file.txt").await.unwrap();
            assert!(exists);
        }
    }

    mod invalidate_locally_cached_files {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_backend_invalidate_locally_cached_files(mut mock_backend: MockBackend) {
            let expected_invalidated = vec![
                PathBuf::from("cached_file1.txt"),
                PathBuf::from("cached_file2.txt"),
            ];
            let copy_of_expected = expected_invalidated.clone();
            mock_backend
                .expect_invalidate_locally_cached_files()
                .returning(move || copy_of_expected.clone());

            let backend = build_backend(mock_backend);
            let invalidated = backend.invalidate_locally_cached_files().await;
            assert_eq!(invalidated, expected_invalidated);
        }
    }

    mock! {
        pub Backend {}

        #[async_trait]
        impl StorageBackend for Backend {
            fn path(&self) -> &PathBuf;
            async fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()>;
            async fn remove(&self, path: &Path) -> std::io::Result<()>;
            async fn remove_dir_all(&self, path: &Path) -> std::io::Result<()>;
            async fn create_dir_all(&self, path: &Path) -> std::io::Result<()>;
            async fn read_dir(&self, path: &Path) -> std::io::Result<Vec<PathBuf>>;
            async fn try_exists(&self, path: &Path) -> std::io::Result<bool>;
            async fn upload(&self, path: &Path) -> std::io::Result<()>;
            async fn download(&self, path: &Path) -> std::io::Result<()>;
            async fn update_local_cache(&self, path: &Path, mode: &AccessMode) -> std::io::Result<()>;
            async fn invalidate_locally_cached_files(&self) -> Vec<PathBuf>;
            async fn get_stats(&self, path: &Path) -> std::io::Result<Option<ObjectMetadata>>;
            async fn remove_from_local_cache(&self, path: &Path) -> std::io::Result<()>;
        }

    }

    #[fixture]
    fn mock_backend(path: PathBuf) -> MockBackend {
        let mut mock = MockBackend::new();
        mock.expect_path().return_const(path.clone());
        mock
    }

    #[fixture]
    fn path() -> PathBuf {
        tempdir().unwrap().keep()
    }

    #[fixture]
    fn license() -> License {
        License {
            licensee: "".to_string(),
            invoice: "".to_string(),
            expiry_date: chrono::Utc::now() + chrono::Duration::days(30),
            plan: "".to_string(),
            device_number: 0,
            disk_quota: 0,
            fingerprint: "".to_string(),
        }
    }

    fn build_backend(mock_backend: MockBackend) -> Backend {
        Backend {
            backend: Arc::new(Box::new(mock_backend)),
        }
    }
}
