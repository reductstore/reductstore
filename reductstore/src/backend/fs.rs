// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::backend::file::AccessMode;
use crate::backend::{ObjectMetadata, StorageBackend};
use async_trait::async_trait;
use std::path::{Path, PathBuf};

pub(crate) struct FileSystemBackend {
    path: PathBuf,
}

impl FileSystemBackend {
    pub fn new(path: PathBuf) -> Self {
        FileSystemBackend { path }
    }
}

#[async_trait]
impl StorageBackend for FileSystemBackend {
    fn path(&self) -> &PathBuf {
        &self.path
    }

    async fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()> {
        // On Windows, file handles may not be released immediately even after closing,
        // causing "Access is denied" errors. Retry with exponential backoff.
        #[cfg(target_os = "windows")]
        {
            use tokio::time::{sleep, Duration};
            const MAX_RETRIES: u32 = 5;
            const INITIAL_DELAY_MS: u64 = 10;

            for attempt in 0..MAX_RETRIES {
                match tokio::fs::rename(from, to).await {
                    Ok(()) => return Ok(()),
                    Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                        if attempt < MAX_RETRIES - 1 {
                            // Exponential backoff: 10ms, 20ms, 40ms, 80ms
                            let delay = Duration::from_millis(INITIAL_DELAY_MS * (1 << attempt));
                            sleep(delay).await;
                            continue;
                        }
                        return Err(e);
                    }
                    Err(e) => return Err(e),
                }
            }
            unreachable!()
        }

        #[cfg(not(target_os = "windows"))]
        {
            tokio::fs::rename(from, to).await
        }
    }

    async fn remove(&self, path: &Path) -> std::io::Result<()> {
        // On Windows, file handles may not be released immediately even after closing,
        // causing "Access is denied" errors. Retry with exponential backoff.
        #[cfg(target_os = "windows")]
        {
            use tokio::time::{sleep, Duration};
            const MAX_RETRIES: u32 = 5;
            const INITIAL_DELAY_MS: u64 = 10;

            for attempt in 0..MAX_RETRIES {
                match tokio::fs::remove_file(path).await {
                    Ok(()) => return Ok(()),
                    Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                        if attempt < MAX_RETRIES - 1 {
                            // Exponential backoff: 10ms, 20ms, 40ms, 80ms
                            let delay = Duration::from_millis(INITIAL_DELAY_MS * (1 << attempt));
                            sleep(delay).await;
                            continue;
                        }
                        return Err(e);
                    }
                    Err(e) => return Err(e),
                }
            }
            unreachable!()
        }

        #[cfg(not(target_os = "windows"))]
        {
            tokio::fs::remove_file(path).await
        }
    }

    async fn remove_dir_all(&self, path: &Path) -> std::io::Result<()> {
        // On Windows, file handles may not be released immediately even after closing,
        // causing "Access is denied" errors. Retry with exponential backoff.
        #[cfg(target_os = "windows")]
        {
            use tokio::time::{sleep, Duration};
            const MAX_RETRIES: u32 = 5;
            const INITIAL_DELAY_MS: u64 = 10;

            for attempt in 0..MAX_RETRIES {
                match tokio::fs::remove_dir_all(path).await {
                    Ok(()) => return Ok(()),
                    Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                        if attempt < MAX_RETRIES - 1 {
                            // Exponential backoff: 10ms, 20ms, 40ms, 80ms
                            let delay = Duration::from_millis(INITIAL_DELAY_MS * (1 << attempt));
                            sleep(delay).await;
                            continue;
                        }
                        return Err(e);
                    }
                    Err(e) => return Err(e),
                }
            }
            unreachable!()
        }

        #[cfg(not(target_os = "windows"))]
        {
            tokio::fs::remove_dir_all(path).await
        }
    }

    async fn create_dir_all(&self, path: &Path) -> std::io::Result<()> {
        tokio::fs::create_dir_all(path).await
    }

    async fn read_dir(&self, path: &Path) -> std::io::Result<Vec<PathBuf>> {
        let mut dir = tokio::fs::read_dir(path).await?;
        let mut entries = Vec::new();
        while let Some(entry) = dir.next_entry().await? {
            entries.push(entry.path());
        }

        Ok(entries)
    }

    async fn try_exists(&self, path: &Path) -> std::io::Result<bool> {
        path.try_exists()
    }

    async fn get_stats(&self, path: &Path) -> std::io::Result<Option<ObjectMetadata>> {
        match tokio::fs::metadata(path).await {
            Ok(metadata) => {
                let modified = metadata.modified().ok();
                let size = metadata.len();
                Ok(Some(ObjectMetadata {
                    size: Some(size as i64),
                    modified_time: modified,
                }))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn upload(&self, _path: &Path) -> std::io::Result<()> {
        // do nothing because the file owner is responsible for syncing with fs
        Ok(())
    }

    async fn download(&self, _path: &Path) -> std::io::Result<()> {
        // do nothing because filesystem backend does not need downloading
        Ok(())
    }

    async fn update_local_cache(&self, _path: &Path, _mode: &AccessMode) -> std::io::Result<()> {
        // do nothing because filesystem backend does not need access tracking
        Ok(())
    }

    async fn invalidate_locally_cached_files(&self) -> Vec<PathBuf> {
        // do nothing because filesystem backend does not have a cache
        vec![]
    }

    async fn remove_from_local_cache(&self, _path: &Path) -> std::io::Result<()> {
        // do nothing because filesystem backend does not have a cache
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;
    use std::fs::OpenOptions;
    use std::io::Write;
    use tempfile::tempdir;

    mod rename_file {
        use super::*;
        use std::io::Read;

        #[rstest]
        #[tokio::test(flavor = "current_thread")]
        async fn test_rename_file(fs_backend: FileSystemBackend) {
            let path = fs_backend.path().join("old_name.txt");
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .open(&path)
                .unwrap();
            writeln!(file, "This is a test file.").unwrap();

            let new_path = path.with_file_name("new_name.txt");
            fs_backend.rename(&path, &new_path).await.unwrap();
            assert!(!fs_backend.try_exists(&path).await.unwrap());
            assert!(fs_backend.try_exists(&new_path).await.unwrap());

            let mut read_file = OpenOptions::new().read(true).open(&new_path).unwrap();
            let mut content = String::new();
            read_file.read_to_string(&mut content).unwrap();
            assert_eq!(content, "This is a test file.\n");
        }
    }

    mod remove_file {
        use super::*;
        use std::fs::OpenOptions;

        #[rstest]
        #[tokio::test(flavor = "current_thread")]
        async fn test_remove_file(fs_backend: FileSystemBackend) {
            let path = fs_backend.path().join("temp_file.txt");
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .open(&path)
                .unwrap();
            writeln!(file, "Temporary file content.").unwrap();

            assert!(fs_backend.try_exists(&path).await.unwrap());
            fs_backend.remove(&path).await.unwrap();
            assert!(!fs_backend.try_exists(&path).await.unwrap());
        }
    }

    mod remove_dir_all {
        use super::*;
        use std::fs::OpenOptions;

        #[rstest]
        #[tokio::test(flavor = "current_thread")]
        async fn test_remove_dir_all(fs_backend: FileSystemBackend) {
            let dir_path = fs_backend.path().join("temp_dir");
            fs_backend.create_dir_all(&dir_path).await.unwrap();
            let file_path = dir_path.join("file.txt");
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .open(&file_path)
                .unwrap();
            writeln!(file, "File in temporary directory.").unwrap();
            file.sync_all().unwrap();

            fs_backend.remove_dir_all(&dir_path).await.unwrap();
            assert!(!fs_backend.try_exists(&dir_path).await.unwrap());
        }
    }

    mod create_dir_all {
        use super::*;

        #[rstest]
        #[tokio::test(flavor = "current_thread")]
        async fn test_create_dir_all(fs_backend: FileSystemBackend) {
            let dir_path = fs_backend.path().join("new_dir/sub_dir");
            fs_backend.create_dir_all(&dir_path).await.unwrap();
            assert!(fs_backend.try_exists(&dir_path).await.unwrap());
        }
    }

    mod read_dir {
        use super::*;
        use std::fs::OpenOptions;

        #[rstest]
        #[tokio::test(flavor = "current_thread")]
        async fn test_read_dir(fs_backend: FileSystemBackend) {
            let dir_path = fs_backend.path().join("read_dir_test");
            fs_backend.create_dir_all(&dir_path).await.unwrap();

            let file1_path = dir_path.join("file1.txt");
            let _ = OpenOptions::new()
                .write(true)
                .create(true)
                .open(&file1_path)
                .unwrap();

            let file2_path = dir_path.join("file2.txt");
            let _ = OpenOptions::new()
                .write(true)
                .create(true)
                .open(&file2_path)
                .unwrap();

            fs_backend
                .create_dir_all(&dir_path.join("child/"))
                .await
                .unwrap();

            let entries = fs_backend.read_dir(&dir_path).await.unwrap();
            let entry_names: Vec<String> = entries
                .iter()
                .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
                .collect();

            assert_eq!(entry_names.len(), 3);
            assert!(entry_names.contains(&"file1.txt".to_string()));
            assert!(entry_names.contains(&"file2.txt".to_string()));
            assert!(entry_names.contains(&"child".to_string()));
        }
    }

    #[fixture]
    fn fs_backend() -> FileSystemBackend {
        let dir = tempdir().unwrap().keep();
        let backend = FileSystemBackend::new(dir.to_path_buf());
        backend
    }
}
