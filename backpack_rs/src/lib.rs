// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::backend::{BoxedBackend, FileSystemBackend};
use crate::error::Error;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use url::Url;

mod backend;
mod error;
mod fs;

pub struct BackpackBuilder {
    locations: Vec<String>,
    cache_size: usize,
    backends: Vec<Arc<RwLock<BoxedBackend>>>,
}

impl BackpackBuilder {
    pub fn new() -> Self {
        Self {
            locations: Vec::new(),
            cache_size: 1024 * 1024 * 100, // 100 MiB default cache size
            backends: Vec::new(),
        }
    }

    pub fn add_location<S: ToString>(mut self, location: S) -> Self {
        if self.locations.len() > 0 {
            panic!("Only one storage location is supported for now");
        }

        self.locations.push(location.to_string());
        self
    }

    pub fn cache_size(mut self, size: usize) -> Self {
        self.cache_size = size;
        self
    }

    pub fn try_build(mut self) -> Result<Backpack, Error> {
        if self.locations.len() == 0 {
            return Err(Error::new("At least one location must be specified"));
        }

        for location in &self.locations {
            let url = Url::parse(location)?;
            match url.scheme() {
                "file" => {
                    // handle file:// scheme with relative and absolute paths
                    let backend = FileSystemBackend::new(location[5..].into());
                    self.backends.push(Arc::new(RwLock::new(Box::new(backend))));
                }
                _ => {
                    return Err(Error::new(&format!("Unsupported scheme: {}", url.scheme())));
                }
            }
        }

        Ok(Backpack {
            locations: self.locations,
            cache_size: self.cache_size,
            backends: self.backends,
        })
    }
}

pub struct Backpack {
    locations: Vec<String>,
    cache_size: usize,
    backends: Vec<Arc<RwLock<BoxedBackend>>>,
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
    /// let backpack = Backpack::builder().add_location("s3://mybucket.com/mnt/data").try_build().unwrap();
    /// let mut options = backpack.open_options();
    /// let file = options.read(true).open("example.txt");
    /// ```
    pub fn open_options(&self) -> fs::OpenOptions {
        fs::OpenOptions::new(Arc::clone(&self.backends[0]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    #[rstest]
    fn open_options() {
        let backpack = Backpack::builder()
            .add_location("file:///tmp")
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
