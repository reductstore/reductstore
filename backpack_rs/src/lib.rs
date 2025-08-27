// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::backend::fs::FileSystemBackend;
use crate::backend::s3::{S3Backend, S3BackendSettings};
use crate::backend::{BoxedBackend, NoopBackend};
use crate::error::Error;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use url::Url;

mod backend;
pub mod error;
pub mod fs;

pub struct BackpackBuilder {
    location: Option<String>,
    cache_size: usize,
    local_cache_path: Option<String>,
    region: Option<String>,
}

impl BackpackBuilder {
    pub fn new() -> Self {
        Self {
            location: None,
            cache_size: 1024 * 1024 * 100, // 100 MiB default cache size
            region: None,
            local_cache_path: None,
        }
    }

    pub fn location<S: ToString>(mut self, location: S) -> Self {
        self.location = Some(location.to_string());
        self
    }

    pub fn cache_size(mut self, size: usize) -> Self {
        self.cache_size = size;
        self
    }

    pub fn local_cache_path<S: ToString>(mut self, path: S) -> Self {
        self.local_cache_path = Some(path.to_string());
        self
    }

    pub fn region<S: ToString>(mut self, region: S) -> Self {
        self.region = Some(region.to_string());
        self
    }

    pub fn try_build(self) -> Result<Backpack, Error> {
        let location = self
            .location
            .ok_or_else(|| Error::new("Location is required"))?;
        let backend: Arc<RwLock<BoxedBackend>> = if let Ok(url) = Url::parse(&location) {
            match url.scheme() {
                #[cfg(feature = "fs")]
                "file" => Arc::new(RwLock::new(Box::new(FileSystemBackend::new(
                    location[5..].into(),
                )))),
                #[cfg(feature = "s3")]
                "s3" => {
                    let Some(region) = self.region else {
                        return Err(Error::new("Region is required for S3 backend"));
                    };

                    let Some(local_cache_path) = self.local_cache_path else {
                        return Err(Error::new("Local cache path is required for S3 backend"));
                    };

                    let settings = S3BackendSettings {
                        region,
                        local_cache_path: PathBuf::from(local_cache_path),
                    };

                    Arc::new(RwLock::new(Box::new(S3Backend::new(settings))))
                }
                _ => {
                    return Err(Error::new(&format!("Unsupported scheme: {}", url.scheme())));
                }
            }
        } else {
            // treat as local path
            #[cfg(not(feature = "fs"))]
            {
                return Err(Error::new(
                    "Filesystem backend is not enabled. Enable the 'fs' feature.",
                ));
            }

            #[cfg(feature = "fs")]
            Arc::new(RwLock::new(Box::new(FileSystemBackend::new(
                location.into(),
            ))))
        };

        Ok(Backpack {
            cache_size: self.cache_size,
            backend,
        })
    }
}

pub struct Backpack {
    cache_size: usize,
    backend: Arc<RwLock<BoxedBackend>>,
}

impl Default for Backpack {
    fn default() -> Self {
        Self {
            cache_size: 1024 * 1024 * 100, // 100 MiB default cache size
            backend: Arc::new(RwLock::new(Box::new(NoopBackend::new()))),
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
    /// let backpack = Backpack::builder().location("file:.").try_build().unwrap();
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
        let backend = self.backend.read().unwrap();
        backend.rename(from.as_ref(), to.as_ref())
    }

    pub fn remove<P: AsRef<std::path::Path>>(&self, path: P) -> std::io::Result<()> {
        let backend = self.backend.read().unwrap();
        backend.remove(path.as_ref())
    }

    pub fn remove_dir_all<P: AsRef<std::path::Path>>(&self, path: P) -> std::io::Result<()> {
        let backend = self.backend.read().unwrap();
        backend.remove_dir_all(path.as_ref())
    }

    pub fn create_dir_all<P: AsRef<std::path::Path>>(&self, path: P) -> std::io::Result<()> {
        let backend = self.backend.read().unwrap();
        backend.create_dir_all(path.as_ref())
    }

    pub fn read_dir(&self, path: &PathBuf) -> std::io::Result<std::fs::ReadDir> {
        let backend = self.backend.read().unwrap();
        backend.read_dir(path)
    }

    pub fn try_exists<P: AsRef<std::path::Path>>(&self, path: P) -> std::io::Result<bool> {
        let backend = self.backend.read().unwrap();
        backend.try_exists(path.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    #[rstest]
    fn open_options() {
        let backpack = Backpack::builder()
            .location("file:///tmp")
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
