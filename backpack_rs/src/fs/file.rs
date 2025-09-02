// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::backend::BoxedBackend;
use log::info;
use std::fs::File as StdFile;
use std::fs::OpenOptions as StdOpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

pub struct File {
    inner: StdFile,
    backend: Arc<BoxedBackend>,
    path: PathBuf,
    last_synced: std::time::Instant,
    is_synced: bool,
}

pub struct OpenOptions {
    inner: StdOpenOptions,
    backend: Arc<BoxedBackend>,
    create: bool,
}

impl OpenOptions {
    pub(crate) fn new(backend: Arc<BoxedBackend>) -> Self {
        Self {
            inner: StdOpenOptions::new(),
            backend,
            create: false,
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
        self.create = true;
        self
    }

    pub fn create_new(&mut self, create_new: bool) -> &mut Self {
        self.inner.create_new(create_new);
        self
    }

    pub fn open<P: AsRef<std::path::Path>>(&mut self, mut path: P) -> std::io::Result<File> {
        let full_path = self.backend.path().join(path.as_ref());
        if !full_path.exists() {
            // the call initiates downloading the file from remote storage if needed
            if let Err(err) = self.backend.download(&full_path) {
                if !self.create {
                    // it's ok if the file does not exist and we are going to create it
                    return Err(err);
                }
            }
        }

        let file = self.inner.open(full_path.clone())?;
        Ok(File {
            inner: file,
            backend: Arc::clone(&self.backend),
            path: full_path,
            last_synced: std::time::Instant::now(),
            is_synced: true,
        })
    }
}

impl File {
    pub fn sync_all(&mut self) -> std::io::Result<()> {
        if self.is_synced() {
            return Ok(());
        }

        info!("File {} synced to disk", self.path.display());

        self.inner.sync_all()?;
        self.backend.sync(&self.path)?;
        self.last_synced = std::time::Instant::now();
        self.is_synced = true;
        Ok(())
    }

    pub fn last_synced(&self) -> std::time::Instant {
        self.last_synced
    }

    pub fn is_synced(&self) -> bool {
        self.is_synced
    }

    pub fn set_len(&mut self, size: u64) -> std::io::Result<()> {
        self.is_synced = false;
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
        self.is_synced = false;
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
