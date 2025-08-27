// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::backend::StorageBackend;
use std::path::PathBuf;

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

    fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
        std::fs::rename(from, to)
    }

    fn remove(&self, path: &std::path::Path) -> std::io::Result<()> {
        std::fs::remove_file(path)
    }

    fn remove_dir_all(&self, path: &std::path::Path) -> std::io::Result<()> {
        std::fs::remove_dir_all(path)
    }

    fn create_dir_all(&self, path: &std::path::Path) -> std::io::Result<()> {
        std::fs::create_dir_all(path)
    }

    fn read_dir(&self, path: &std::path::Path) -> std::io::Result<std::fs::ReadDir> {
        std::fs::read_dir(path)
    }

    fn try_exists(&self, path: &std::path::Path) -> std::io::Result<bool> {
        path.try_exists()
    }
}
