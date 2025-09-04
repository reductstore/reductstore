// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::backend::file::AccessMode;
use crate::backend::StorageBackend;
use std::path::{Path, PathBuf};

pub(super) struct NoopBackend;

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

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    #[rstest]
    fn test_noop_backend() {
        let backend = NoopBackend::new();
        assert!(!backend.try_exists(Path::new("some/path")).unwrap());
        assert!(backend.read_dir(Path::new("some/path")).unwrap().is_empty());
        assert!(backend.rename(Path::new("from"), Path::new("to")).is_ok());
        assert!(backend.remove(Path::new("some/path")).is_ok());
        assert!(backend.remove_dir_all(Path::new("some/path")).is_ok());
        assert!(backend.create_dir_all(Path::new("some/path")).is_ok());
        assert!(backend.upload(Path::new("some/path")).is_ok());
        assert!(backend.download(Path::new("some/path")).is_ok());
        assert!(backend
            .update_local_cache(Path::new("some/path"), &AccessMode::Read)
            .is_ok());
        assert!(backend.invalidate_locally_cached_files().is_empty());
    }

    #[rstest]
    #[should_panic(expected = "NoopBackend does not have a path")]
    fn test_noop_backend_path() {
        let backend = NoopBackend::new();
        let _ = backend.path();
    }
}
