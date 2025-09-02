// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#[cfg(feature = "fs-backend")]
pub(super) mod fs;

#[cfg(feature = "s3-backend")]
pub(super) mod s3;

pub(crate) mod file;

use crate::backend::file::{AccessMode, OpenOptions};
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use url::Url;

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

pub(crate) struct NoopBackend;

impl StorageBackend for NoopBackend {
    fn path(&self) -> &PathBuf {
        panic!("NoopBackend does not have a path");
    }

    fn rename(&self, _from: &Path, _to: &Path) -> std::io::Result<()> {
        Ok(())
    }

    fn remove(&self, _path: &Path) -> std::io::Result<()> {
        Ok(())
    }

    fn remove_dir_all(&self, _path: &Path) -> std::io::Result<()> {
        Ok(())
    }

    fn create_dir_all(&self, _path: &Path) -> std::io::Result<()> {
        Ok(())
    }

    fn read_dir(&self, _path: &Path) -> std::io::Result<Vec<PathBuf>> {
        Ok(vec![])
    }

    fn try_exists(&self, _path: &Path) -> std::io::Result<bool> {
        Ok(false)
    }

    fn upload(&self, _path: &Path) -> std::io::Result<()> {
        Ok(())
    }

    fn download(&self, _path: &Path) -> std::io::Result<()> {
        Ok(())
    }

    fn update_local_cache(&self, _path: &Path, _mode: &AccessMode) -> std::io::Result<()> {
        Ok(())
    }

    fn invalidate_locally_cached_files(&self) -> Vec<PathBuf> {
        vec![]
    }
}

impl NoopBackend {
    pub fn new() -> Self {
        NoopBackend
    }
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

    local_data_path: Option<String>,
    remote_bucket: Option<String>,
    remote_cache_path: Option<String>,
    remote_region: Option<String>,
    remote_endpoint: Option<String>,
    remote_access_key: Option<String>,
    remote_secret_key: Option<String>,
    remote_cache_size: Option<u64>,
}

impl BackpackBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn backend_type(mut self, backend_type: BackendType) -> Self {
        self.backend_type = backend_type;
        self
    }

    pub fn local_data_path(mut self, path: &str) -> Self {
        self.local_data_path = Some(path.to_string());
        self
    }

    pub fn remote_bucket(mut self, bucket: &str) -> Self {
        self.remote_bucket = Some(bucket.to_string());
        self
    }

    pub fn remote_cache_path(mut self, path: &str) -> Self {
        self.remote_cache_path = Some(path.to_string());
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
                let Some(bucket) = self.remote_bucket else {
                    Err(internal_server_error!(
                        "remote_bucket is required for S3 backend"
                    ))?
                };

                let Some(cache_path) = self.remote_cache_path else {
                    Err(internal_server_error!(
                        "remote_cache_path is required for S3 backend"
                    ))?
                };

                let Some(region) = self.remote_region else {
                    Err(internal_server_error!(
                        "remote_region is required for S3 backend"
                    ))?
                };

                let Some(endpoint) = self.remote_endpoint else {
                    Err(internal_server_error!(
                        "remote_endpoint is required for S3 backend"
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

                let url = Url::parse(&endpoint)
                    .map_err(|e| internal_server_error!("Invalid endpoint URL: {}", e))?;
                let settings = s3::S3BackendSettings {
                    cache_path: PathBuf::from(cache_path),
                    endpoint: url.to_string(),
                    access_key,
                    secret_key,
                    region,
                    bucket,
                    cache_size,
                };

                Box::new(s3::S3Backend::new(settings))
            }

            #[allow(unreachable_patterns)]
            _ => Err(internal_server_error!(
                "Unsupported backend type or feature not enabled",
            ))?,
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
    use rstest::*;
}
