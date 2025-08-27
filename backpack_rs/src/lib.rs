// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::backend::{BoxedBackend, FileSystemBackend, NoopBackend};
use crate::error::Error;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use url::Url;

mod backend;
pub mod error;
pub mod fs;

pub struct BackpackBuilder {
    location: String,
    cache_size: usize,
}

impl BackpackBuilder {
    pub fn new() -> Self {
        Self {
            location: String::new(),
            cache_size: 1024 * 1024 * 100, // 100 MiB default cache size
        }
    }

    pub fn location<S: ToString>(mut self, location: S) -> Self {
        self.location = location.to_string();
        self
    }

    pub fn cache_size(mut self, size: usize) -> Self {
        self.cache_size = size;
        self
    }

    pub fn try_build(self) -> Result<Backpack, Error> {
        let backend: Arc<RwLock<BoxedBackend>> = if let Ok(url) = Url::parse(&self.location) {
            match url.scheme() {
                "file" => {
                    // handle file:// scheme with relative and absolute paths
                    Arc::new(RwLock::new(Box::new(FileSystemBackend::new(
                        self.location[5..].into(),
                    ))))
                }
                _ => {
                    return Err(Error::new(&format!("Unsupported scheme: {}", url.scheme())));
                }
            }
        } else {
            // treat as local path
            Arc::new(RwLock::new(Box::new(FileSystemBackend::new(
                self.location.into(),
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
    /// let backpack = Backpack::builder().location("s3://mybucket.com/mnt/data").try_build().unwrap();
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
