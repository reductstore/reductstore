// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
#[cfg(feature = "fs")]
pub(super) mod fs;

#[cfg(feature = "s3")]
pub(super) mod s3;

use crate::fs::AccessMode;
use std::path::{Path, PathBuf};

pub trait StorageBackend {
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
