// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::backend::fs::FileSystemBackend;
use crate::backend::s3::{S3Backend, S3BackendSettings};
use crate::backend::{BoxedBackend, NoopBackend};
use crate::error::Error;
use std::path::PathBuf;
use std::sync::Arc;
use url::Url;

mod backend;
pub mod error;
pub mod fs;

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

    pub fn try_build(self) -> Result<Backpack, Error> {
        let backend: BoxedBackend = match self.backend_type {
            #[cfg(feature = "fs")]
            BackendType::Filesystem => {
                let Some(data_path) = self.local_data_path else {
                    Err(Error::new(
                        "local_data_path is required for Filesystem backend",
                    ))?
                };

                Box::new(FileSystemBackend::new(PathBuf::from(data_path)))
            }

            #[cfg(feature = "s3")]
            BackendType::S3 => {
                let Some(bucket) = self.remote_bucket else {
                    Err(Error::new("remote_bucket is required for S3 backend"))?
                };

                let Some(cache_path) = self.remote_cache_path else {
                    Err(Error::new("remote_cache_path is required for S3 backend"))?
                };

                let Some(region) = self.remote_region else {
                    Err(Error::new("remote_region is required for S3 backend"))?
                };

                let Some(endpoint) = self.remote_endpoint else {
                    Err(Error::new("remote_endpoint is required for S3 backend"))?
                };

                let Some(access_key) = self.remote_access_key else {
                    Err(Error::new("remote_access_key is required for S3 backend"))?
                };

                let Some(secret_key) = self.remote_secret_key else {
                    Err(Error::new("remote_secret_key is required for S3 backend"))?
                };

                let Some(cache_size) = self.remote_cache_size else {
                    Err(Error::new("remote_cache_size is required for S3 backend"))?
                };

                let url = Url::parse(&endpoint)
                    .map_err(|e| Error::new(&format!("Invalid endpoint URL: {}", e)))?;
                let settings = S3BackendSettings {
                    cache_path: PathBuf::from(cache_path),
                    endpoint: url.to_string(),
                    access_key,
                    secret_key,
                    region,
                    bucket,
                    cache_size,
                };

                Box::new(S3Backend::new(settings))
            }

            #[allow(unreachable_patterns)]
            _ => Err(Error::new(
                "Unsupported backend type or feature not enabled",
            ))?,
        };

        Ok(Backpack {
            backend: Arc::new(backend),
        })
    }
}

pub struct Backpack {
    backend: Arc<BoxedBackend>,
}

impl Default for Backpack {
    fn default() -> Self {
        Self {
            backend: Arc::new(Box::new(NoopBackend::new())),
        }
    }
}

impl Backpack {
    pub fn builder() -> BackpackBuilder {
        BackpackBuilder::new()
    }

    /// Create a new instance of `fs::OpenOptions`.
    ///
    /// # Examples
    ///
    /// ```
    /// use backpack_rs::Backpack;
    ///
    /// let backpack = Backpack::builder().local_data_path(".").try_build().unwrap();
    /// let mut options = backpack.open_options();
    /// let file = options.read(true).open("example.txt");
    /// ```
    pub fn open_options(&self) -> fs::OpenOptions {
        fs::OpenOptions::new(Arc::clone(&self.backend))
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

    #[rstest]
    fn open_options() {
        let backpack = Backpack::builder()
            .local_data_path("tmp")
            .try_build()
            .unwrap();
        let mut options = backpack.open_options();
        let file = options
            .write(true)
            .create(true)
            .open("example.txt")
            .unwrap();
    }
}
