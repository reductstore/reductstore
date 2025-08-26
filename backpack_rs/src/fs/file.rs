// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::backend::BoxedBackend;
use std::fs::File as StdFile;
use std::fs::OpenOptions as StdOpenOptions;
use std::sync::{Arc, RwLock};

pub struct File {
    inner: StdFile,
}

pub struct OpenOptions {
    inner: StdOpenOptions,
    backend: Arc<RwLock<BoxedBackend>>,
}

impl OpenOptions {
    pub(crate) fn new(backend: Arc<RwLock<BoxedBackend>>) -> Self {
        Self {
            inner: StdOpenOptions::new(),
            backend,
        }
    }

    pub fn read(&mut self, read: bool) -> &mut Self {
        self.inner.read(read);
        self
    }

    pub fn write(&mut self, write: bool) -> &mut Self {
        self.inner.write(write);
        self
    }

    pub fn append(&mut self, append: bool) -> &mut Self {
        self.inner.append(append);
        self
    }

    pub fn truncate(&mut self, truncate: bool) -> &mut Self {
        self.inner.truncate(truncate);
        self
    }

    pub fn create(&mut self, create: bool) -> &mut Self {
        self.inner.create(create);
        self
    }

    pub fn create_new(&mut self, create_new: bool) -> &mut Self {
        self.inner.create_new(create_new);
        self
    }

    pub fn open<P: AsRef<std::path::Path>>(&self, path: P) -> std::io::Result<File> {
        let full_path = self.backend.read().unwrap().path().join(path);
        let file = self.inner.open(full_path)?;
        Ok(File { inner: file })
    }
}
