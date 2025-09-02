// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::backend::StorageBackend;
use crate::fs::AccessMode;
use std::path::{Path, PathBuf};

pub(crate) struct FileSystemBackend {
    path: PathBuf,
}

impl FileSystemBackend {
    pub fn new(path: PathBuf) -> Self {
        FileSystemBackend { path }
    }
}

impl StorageBackend for FileSystemBackend {
    fn path(&self) -> &PathBuf {
        &self.path
    }

    fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()> {
        std::fs::rename(from, to)
    }

    fn remove(&self, path: &Path) -> std::io::Result<()> {
        std::fs::remove_file(path)
    }

    fn remove_dir_all(&self, path: &Path) -> std::io::Result<()> {
        std::fs::remove_dir_all(path)
    }

    fn create_dir_all(&self, path: &Path) -> std::io::Result<()> {
        std::fs::create_dir_all(path)
    }

    fn read_dir(&self, path: &Path) -> std::io::Result<Vec<PathBuf>> {
        std::fs::read_dir(path).map(|read_dir| {
            read_dir
                .filter_map(|entry| entry.ok().map(|e| e.path()))
                .collect()
        })
    }

    fn try_exists(&self, path: &Path) -> std::io::Result<bool> {
        path.try_exists()
    }

    fn upload(&self, _path: &Path) -> std::io::Result<()> {
        // do nothing because the file owner is responsible for syncing with fs
        Ok(())
    }

    fn download(&self, _path: &Path) -> std::io::Result<()> {
        // do nothing because filesystem backend does not need downloading
        Ok(())
    }

    fn update_local_cache(&self, path: &Path, mode: &AccessMode) -> std::io::Result<()> {
        // do nothing because filesystem backend does not need access tracking
        Ok(())
    }

    fn invalidate_locally_cached_files(&self) -> Vec<PathBuf> {
        // do nothing because filesystem backend does not have a cache
        vec![]
    }
}
