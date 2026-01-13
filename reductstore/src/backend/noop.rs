// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::backend::file::AccessMode;
use crate::backend::{ObjectMetadata, StorageBackend};
use async_trait::async_trait;
use std::path::{Path, PathBuf};

pub(super) struct NoopBackend;

#[async_trait]
impl StorageBackend for NoopBackend {
    fn path(&self) -> &PathBuf {
        panic!("NoopBackend does not have a path");
    }

    async fn rename(&self, _from: &Path, _to: &Path) -> std::io::Result<()> {
        Ok(())
    }

    async fn remove(&self, _path: &Path) -> std::io::Result<()> {
        Ok(())
    }

    async fn remove_dir_all(&self, _path: &Path) -> std::io::Result<()> {
        Ok(())
    }

    async fn create_dir_all(&self, _path: &Path) -> std::io::Result<()> {
        Ok(())
    }

    async fn read_dir(&self, _path: &Path) -> std::io::Result<Vec<PathBuf>> {
        Ok(vec![])
    }

    async fn try_exists(&self, _path: &Path) -> std::io::Result<bool> {
        Ok(false)
    }

    async fn upload(&self, _path: &Path) -> std::io::Result<()> {
        Ok(())
    }

    async fn download(&self, _path: &Path) -> std::io::Result<()> {
        Ok(())
    }

    async fn update_local_cache(&self, _path: &Path, _mode: &AccessMode) -> std::io::Result<()> {
        Ok(())
    }

    async fn invalidate_locally_cached_files(&self) -> Vec<PathBuf> {
        vec![]
    }

    async fn get_stats(&self, _path: &Path) -> std::io::Result<Option<ObjectMetadata>> {
        Ok(None)
    }

    async fn remove_from_local_cache(&self, _path: &Path) -> std::io::Result<()> {
        Ok(())
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
        assert!(backend.get_stats(Path::new("some/path")).unwrap().is_none());
    }

    #[rstest]
    #[should_panic(expected = "NoopBackend does not have a path")]
    fn test_noop_backend_path() {
        let backend = NoopBackend::new();
        let _ = backend.path();
    }
}
