// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
mod fs;

use std::fs::ReadDir;
use std::path::{Path, PathBuf};

pub trait Backend {
    fn path(&self) -> &PathBuf;
    fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()>;

    fn remove(&self, path: &Path) -> std::io::Result<()>;

    fn remove_dir_all(&self, path: &Path) -> std::io::Result<()>;

    fn create_dir_all(&self, path: &Path) -> std::io::Result<()>;

    fn read_dir(&self, path: &Path) -> std::io::Result<std::fs::ReadDir>;
}

pub(crate) struct NoopBackend;

impl Backend for NoopBackend {
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

    fn read_dir(&self, path: &Path) -> std::io::Result<ReadDir> {
        panic!(
            "NoopBackend does not support read_dir on path: {}",
            path.display()
        );
    }
}

impl NoopBackend {
    pub fn new() -> Self {
        NoopBackend
    }
}

pub type BoxedBackend = Box<dyn Backend + Send + Sync>;

pub(crate) use fs::FileSystemBackend;
