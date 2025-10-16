// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#[cfg(feature = "fs-backend")]
pub(super) mod fs;

pub(super) mod remote;

pub(crate) mod file;
mod noop;

use crate::backend::file::{AccessMode, OpenOptions};
use crate::backend::noop::NoopBackend;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use reduct_base::msg::server_api::License;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(crate) trait StorageBackend {
    fn path(&self) -> &PathBuf;
    fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()>;

    fn remove(&self, path: &Path) -> std::io::Result<()>;

    fn remove_dir_all(&self, path: &Path) -> std::io::Result<()>;

    fn create_dir_all(&self, path: &Path) -> std::io::Result<()>;

    fn read_dir(&self, path: &Path) -> std::io::Result<Vec<PathBuf>>;

    fn try_exists(&self, _path: &Path) -> std::io::Result<bool>;

    fn upload(&self, path: &Path) -> std::io::Result<()>;

    fn download(&self, path: &Path) -> std::io::Result<()>;

    fn update_local_cache(&self, path: &Path, mode: &AccessMode) -> std::io::Result<()>;

    fn invalidate_locally_cached_files(&self) -> Vec<PathBuf>;
}

pub type BoxedBackend = Box<dyn StorageBackend + Send + Sync>;

#[derive(Default, Clone, Debug, PartialEq)]
pub enum BackendType {
    #[default]
    Filesystem,
    S3,
}

#[derive(Default)]
pub struct BackpackBuilder {
    backend_type: BackendType,

    local_data_path: Option<PathBuf>,
    remote_bucket: Option<String>,
    remote_cache_path: Option<PathBuf>,
    remote_region: Option<String>,
    remote_endpoint: Option<String>,
    remote_access_key: Option<String>,
    remote_secret_key: Option<String>,
    remote_cache_size: Option<u64>,
    remote_default_storage_class: Option<String>,
    license: Option<License>,
}

impl BackpackBuilder {
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

    pub fn remote_bucket(mut self, bucket: &str) -> Self {
        self.remote_bucket = Some(bucket.to_string());
        self
    }

    pub fn remote_cache_path(mut self, path: PathBuf) -> Self {
        self.remote_cache_path = Some(path);
        self
    }

    pub fn cache_size(mut self, size: u64) -> Self {
        self.remote_cache_size = Some(size);
        self
    }

    pub fn remote_region(mut self, region: &str) -> Self {
        self.remote_region = Some(region.to_string());
        self
    }

    pub fn remote_endpoint(mut self, endpoint: &str) -> Self {
        self.remote_endpoint = Some(endpoint.to_string());
        self
    }

    pub fn remote_access_key(mut self, access_key: &str) -> Self {
        self.remote_access_key = Some(access_key.to_string());
        self
    }

    pub fn remote_secret_key(mut self, secret_key: &str) -> Self {
        self.remote_secret_key = Some(secret_key.to_string());
        self
    }

    pub fn remote_default_storage_class(mut self, storage_class: Option<String>) -> Self {
        self.remote_default_storage_class = storage_class;
        self
    }

    pub fn license(mut self, license: License) -> Self {
        self.license = Some(license);
        self
    }

    pub fn try_build(self) -> Result<Backend, ReductError> {
        let backend: BoxedBackend = match self.backend_type {
            #[cfg(feature = "fs-backend")]
            BackendType::Filesystem => {
                let Some(data_path) = self.local_data_path else {
                    Err(internal_server_error!(
                        "local_data_path is required for Filesystem backend",
                    ))?
                };

                Box::new(fs::FileSystemBackend::new(PathBuf::from(data_path)))
            }

            #[cfg(feature = "s3-backend")]
            BackendType::S3 => {
                if self.license.is_none()
                    || self.license.as_ref().unwrap().expiry_date > chrono::Utc::now()
                {
                    return Err(internal_server_error!(
                        "S3 backend requires a valid commercial license"
                    ));
                }

                let Some(bucket) = self.remote_bucket else {
                    Err(internal_server_error!(
                        "remote_bucket is required remote S3 backend"
                    ))?
                };

                let Some(cache_path) = self.remote_cache_path else {
                    Err(internal_server_error!(
                        "remote_cache_path is required remote S3 backend"
                    ))?
                };

                let Some(access_key) = self.remote_access_key else {
                    Err(internal_server_error!(
                        "remote_access_key is required for S3 backend"
                    ))?
                };

                let Some(secret_key) = self.remote_secret_key else {
                    Err(internal_server_error!(
                        "remote_secret_key is required for S3 backend"
                    ))?
                };

                let Some(cache_size) = self.remote_cache_size else {
                    Err(internal_server_error!(
                        "remote_cache_size is required for S3 backend"
                    ))?
                };

                let settings = remote::RemoteBackendSettings {
                    backend_type: BackendType::S3,
                    cache_path: PathBuf::from(cache_path),
                    endpoint: self.remote_endpoint,
                    access_key,
                    secret_key,
                    region: self.remote_region,
                    bucket,
                    cache_size,
                    default_storage_class: self.remote_default_storage_class,
                };

                Box::new(remote::RemoteBackend::new(settings))
            }
            #[allow(unreachable_patterns)]
            _ => Err(internal_server_error!("Unsupported backend type"))?,
        };

        Ok(Backend {
            backend: Arc::new(backend),
        })
    }
}

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
    pub fn builder() -> BackpackBuilder {
        BackpackBuilder::new()
    }

    /// Create a new instance of `fs::OpenOptions`.
    pub fn open_options(&self) -> OpenOptions {
        OpenOptions::new(Arc::clone(&self.backend))
    }

    pub fn rename<P: AsRef<std::path::Path>, Q: AsRef<std::path::Path>>(
        &self,
        from: P,
        to: Q,
    ) -> std::io::Result<()> {
        self.backend.rename(from.as_ref(), to.as_ref())
    }

    pub fn remove<P: AsRef<std::path::Path>>(&self, path: P) -> std::io::Result<()> {
        self.backend.remove(path.as_ref())
    }

    pub fn remove_dir_all<P: AsRef<std::path::Path>>(&self, path: P) -> std::io::Result<()> {
        self.backend.remove_dir_all(path.as_ref())
    }

    pub fn create_dir_all<P: AsRef<std::path::Path>>(&self, path: P) -> std::io::Result<()> {
        self.backend.create_dir_all(path.as_ref())
    }

    pub fn read_dir(&self, path: &PathBuf) -> std::io::Result<Vec<PathBuf>> {
        self.backend.read_dir(path)
    }

    pub fn try_exists<P: AsRef<std::path::Path>>(&self, path: P) -> std::io::Result<bool> {
        self.backend.try_exists(path.as_ref())
    }

    pub fn invalidate_locally_cached_files(&self) -> Vec<PathBuf> {
        self.backend.invalidate_locally_cached_files()
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
        fn test_backend_builder_fs() {
            {
                let backend = Backend::builder()
                    .backend_type(BackendType::Filesystem)
                    .local_data_path(PathBuf::from("/tmp/data"))
                    .try_build()
                    .expect("Failed to build Filesystem backend");
                assert_eq!(backend.backend.path(), &PathBuf::from("/tmp/data"));
            }
        }
    }

    #[cfg(feature = "s3-backend")]
    mod s3 {
        use super::*;
        #[rstest]
        fn test_backend_builder_s3() {
            {
                let backend = Backend::builder()
                    .backend_type(BackendType::S3)
                    .remote_bucket("my-bucket")
                    .remote_cache_path(PathBuf::from("/tmp/cache"))
                    .remote_region("us-east-1")
                    .remote_endpoint("http://localhost:9000")
                    .remote_access_key("access_key")
                    .remote_secret_key("secret_key")
                    .cache_size(1024 * 1024 * 1024) // 1 GB
                    .try_build()
                    .expect("Failed to build S3 backend");
                assert_eq!(backend.backend.path(), &PathBuf::from("/tmp/cache"));
            }
        }

        #[rstest]
        fn test_backend_builder_s3_bucket_missing() {
            let err = Backend::builder()
                .backend_type(BackendType::S3)
                .remote_cache_path(PathBuf::from("/tmp/cache"))
                .remote_region("us-east-1")
                .remote_endpoint("http://localhost:9000")
                .remote_access_key("access_key")
                .remote_secret_key("secret_key")
                .cache_size(1024 * 1024 * 1024) // 1 GB
                .try_build()
                .err()
                .unwrap();

            assert_eq!(
                err,
                internal_server_error!("remote_bucket is required remote S3 backend")
            );
        }

        #[rstest]
        fn test_backend_builder_s3_cache_path_missing() {
            let err = Backend::builder()
                .backend_type(BackendType::S3)
                .remote_bucket("my-bucket")
                .remote_region("us-east-1")
                .remote_endpoint("http://localhost:9000")
                .remote_access_key("access_key")
                .remote_secret_key("secret_key")
                .cache_size(1024 * 1024 * 1024) // 1 GB
                .try_build()
                .err()
                .unwrap();

            assert_eq!(
                err,
                internal_server_error!("remote_cache_path is required remote S3 backend")
            );
        }

        #[rstest]
        fn test_backend_builder_s3_region_missing() {
            let result = Backend::builder()
                .backend_type(BackendType::S3)
                .remote_bucket("my-bucket")
                .remote_cache_path(PathBuf::from("/tmp/cache"))
                .remote_region("us-east-1")
                .remote_endpoint("http://localhost:9000")
                .remote_access_key("access_key")
                .remote_secret_key("secret_key")
                .cache_size(1024 * 1024 * 1024) // 1 GB
                .try_build();

            assert!(
                result.is_ok(),
                "Is not needed for MinIO and other S3-compatible storages"
            );
        }

        #[rstest]
        fn test_backend_builder_s3_endpoint_missing() {
            let result = Backend::builder()
                .backend_type(BackendType::S3)
                .remote_bucket("my-bucket")
                .remote_cache_path(PathBuf::from("/tmp/cache"))
                .remote_region("us-east-1")
                .remote_access_key("access_key")
                .remote_secret_key("secret_key")
                .cache_size(1024 * 1024 * 1024)
                .try_build();

            assert!(
                result.is_ok(),
                "Is not needed for AWS S3 and other S3-compatible storages"
            );
        }

        #[rstest]
        fn test_backend_builder_s3_access_key_missing() {
            let err = Backend::builder()
                .backend_type(BackendType::S3)
                .remote_bucket("my-bucket")
                .remote_cache_path(PathBuf::from("/tmp/cache"))
                .remote_region("us-east-1")
                .remote_endpoint("http://localhost:9000")
                .remote_secret_key("secret_key")
                .cache_size(1024 * 1024 * 1024)
                .try_build()
                .err()
                .unwrap();

            assert_eq!(
                err,
                internal_server_error!("remote_access_key is required for S3 backend")
            );
        }

        #[rstest]
        fn test_backend_builder_s3_secret_key_missing() {
            let err = Backend::builder()
                .backend_type(BackendType::S3)
                .remote_bucket("my-bucket")
                .remote_cache_path(PathBuf::from("/tmp/cache"))
                .remote_region("us-east-1")
                .remote_endpoint("http://localhost:9000")
                .remote_access_key("access_key")
                .cache_size(1024 * 1024 * 1024)
                .try_build()
                .err()
                .unwrap();

            assert_eq!(
                err,
                internal_server_error!("remote_secret_key is required for S3 backend")
            );
        }

        #[rstest]
        fn test_backend_builder_s3_cache_size_missing() {
            let err = Backend::builder()
                .backend_type(BackendType::S3)
                .backend_type(BackendType::S3)
                .remote_bucket("my-bucket")
                .remote_cache_path(PathBuf::from("/tmp/cache"))
                .remote_region("us-east-1")
                .remote_endpoint("http://localhost:9000")
                .remote_access_key("access_key")
                .remote_secret_key("secret_key")
                .try_build()
                .err()
                .unwrap();

            assert_eq!(
                err,
                internal_server_error!("remote_cache_size is required for S3 backend")
            );
        }
    }

    mod open {
        use super::*;
        #[rstest]
        fn test_backend_open_options(mut mock_backend: MockBackend) {
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
                .unwrap();

            assert!(file.is_synced());
            assert_eq!(file.mode(), &AccessMode::ReadWrite);
            assert_eq!(file.metadata().unwrap().len(), 0);
        }
    }

    mod rename {
        use super::*;

        #[rstest]
        fn test_backend_rename(mut mock_backend: MockBackend) {
            mock_backend
                .expect_rename()
                .returning(move |old_path, new_path| {
                    assert_eq!(old_path, Path::new("old_name.txt"));
                    assert_eq!(new_path, Path::new("new_name.txt"));
                    Ok(())
                });

            let backend = build_backend(mock_backend);
            backend.rename("old_name.txt", "new_name.txt").unwrap();
        }
    }

    mod remove {
        use super::*;

        #[rstest]
        fn test_backend_remove(mut mock_backend: MockBackend) {
            mock_backend.expect_remove().returning(move |path| {
                assert_eq!(path, Path::new("temp_file.txt"));
                Ok(())
            });

            let backend = build_backend(mock_backend);
            backend.remove("temp_file.txt").unwrap();
        }
    }

    mod remove_dir_all {
        use super::*;

        #[rstest]
        fn test_backend_remove_dir_all(mut mock_backend: MockBackend) {
            mock_backend.expect_remove_dir_all().returning(move |path| {
                assert_eq!(path, Path::new("temp_dir"));
                Ok(())
            });

            let backend = build_backend(mock_backend);
            backend.remove_dir_all("temp_dir").unwrap();
        }
    }

    mod create_dir_all {
        use super::*;

        #[rstest]
        fn test_backend_create_dir_all(mut mock_backend: MockBackend) {
            mock_backend.expect_create_dir_all().returning(move |path| {
                assert_eq!(path, Path::new("new_dir"));
                Ok(())
            });

            let backend = build_backend(mock_backend);
            backend.create_dir_all("new_dir").unwrap();
        }
    }

    mod read_dir {
        use super::*;

        #[rstest]
        fn test_backend_read_dir(mut mock_backend: MockBackend) {
            let expected_files = vec![PathBuf::from("file1.txt"), PathBuf::from("file2.txt")];
            let copy_of_expected = expected_files.clone();
            mock_backend.expect_read_dir().returning(move |path| {
                assert_eq!(path, Path::new("some_dir"));
                Ok(copy_of_expected.clone())
            });

            let backend = build_backend(mock_backend);
            let files = backend.read_dir(&PathBuf::from("some_dir")).unwrap();
            assert_eq!(files, expected_files);
        }
    }

    mod try_exists {
        use super::*;

        #[rstest]
        fn test_backend_try_exists(mut mock_backend: MockBackend) {
            mock_backend.expect_try_exists().returning(move |path| {
                assert_eq!(path, Path::new("existing_file.txt"));
                Ok(true)
            });

            let backend = build_backend(mock_backend);
            let exists = backend.try_exists("existing_file.txt").unwrap();
            assert!(exists);
        }
    }

    mod invalidate_locally_cached_files {
        use super::*;

        #[rstest]
        fn test_backend_invalidate_locally_cached_files(mut mock_backend: MockBackend) {
            let expected_invalidated = vec![
                PathBuf::from("cached_file1.txt"),
                PathBuf::from("cached_file2.txt"),
            ];
            let copy_of_expected = expected_invalidated.clone();
            mock_backend
                .expect_invalidate_locally_cached_files()
                .returning(move || copy_of_expected.clone());

            let backend = build_backend(mock_backend);
            let invalidated = backend.invalidate_locally_cached_files();
            assert_eq!(invalidated, expected_invalidated);
        }
    }

    mock! {
        pub Backend {}

        impl StorageBackend for Backend {
            fn path(&self) -> &PathBuf;
            fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()>;
            fn remove(&self, path: &Path) -> std::io::Result<()>;
            fn remove_dir_all(&self, path: &Path) -> std::io::Result<()>;
            fn create_dir_all(&self, path: &Path) -> std::io::Result<()>;
            fn read_dir(&self, path: &Path) -> std::io::Result<Vec<PathBuf>>;
            fn try_exists(&self, path: &Path) -> std::io::Result<bool>;
            fn upload(&self, path: &Path) -> std::io::Result<()>;
            fn download(&self, path: &Path) -> std::io::Result<()>;
            fn update_local_cache(&self, path: &Path, mode: &AccessMode) -> std::io::Result<()>;
            fn invalidate_locally_cached_files(&self) -> Vec<PathBuf>;
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

    fn build_backend(mock_backend: MockBackend) -> Backend {
        Backend {
            backend: Arc::new(Box::new(mock_backend)),
        }
    }
}
