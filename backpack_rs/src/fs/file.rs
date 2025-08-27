// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::backend::BoxedBackend;
use std::fs::File as StdFile;
use std::fs::OpenOptions as StdOpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
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

impl File {
    pub fn sync_all(&self) -> std::io::Result<()> {
        self.inner.sync_all()
    }

    pub fn sync_data(&self) -> std::io::Result<()> {
        self.inner.sync_data()
    }

    pub fn set_len(&self, size: u64) -> std::io::Result<()> {
        self.inner.set_len(size)
    }

    pub fn metadata(&self) -> std::io::Result<std::fs::Metadata> {
        self.inner.metadata()
    }
}

impl Read for File {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

impl Write for File {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl Seek for File {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(pos)
    }
}
