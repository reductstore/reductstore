// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::backend::file::AccessMode;
use crate::backend::{ObjectMetadata, StorageBackend};
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

    fn get_stats(&self, path: &Path) -> std::io::Result<Option<ObjectMetadata>> {
        match std::fs::metadata(path) {
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

    fn upload(&self, _path: &Path) -> std::io::Result<()> {
        // do nothing because the file owner is responsible for syncing with fs
        Ok(())
    }

    fn download(&self, _path: &Path) -> std::io::Result<()> {
        // do nothing because filesystem backend does not need downloading
        Ok(())
    }

    fn update_local_cache(&self, _path: &Path, _mode: &AccessMode) -> std::io::Result<()> {
        // do nothing because filesystem backend does not need access tracking
        Ok(())
    }

    fn invalidate_locally_cached_files(&self) -> Vec<PathBuf> {
        // do nothing because filesystem backend does not have a cache
        vec![]
    }

    fn remove_only_locally(&self, path: &Path) -> std::io::Result<()> {
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
        fn test_rename_file(fs_backend: FileSystemBackend) {
            let path = fs_backend.path().join("old_name.txt");
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .open(&path)
                .unwrap();
            writeln!(file, "This is a test file.").unwrap();

            let new_path = path.with_file_name("new_name.txt");
            fs_backend.rename(&path, &new_path).unwrap();
            assert!(!fs_backend.try_exists(&path).unwrap());
            assert!(fs_backend.try_exists(&new_path).unwrap());

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
        fn test_remove_file(fs_backend: FileSystemBackend) {
            let path = fs_backend.path().join("temp_file.txt");
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .open(&path)
                .unwrap();
            writeln!(file, "Temporary file content.").unwrap();

            assert!(fs_backend.try_exists(&path).unwrap());
            fs_backend.remove(&path).unwrap();
            assert!(!fs_backend.try_exists(&path).unwrap());
        }
    }

    mod remove_dir_all {
        use super::*;
        use std::fs::OpenOptions;

        #[rstest]
        fn test_remove_dir_all(fs_backend: FileSystemBackend) {
            let dir_path = fs_backend.path().join("temp_dir");
            fs_backend.create_dir_all(&dir_path).unwrap();
            let file_path = dir_path.join("file.txt");
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .open(&file_path)
                .unwrap();
            writeln!(file, "File in temporary directory.").unwrap();
            file.sync_all().unwrap();

            fs_backend.remove_dir_all(&dir_path).unwrap();
            assert!(!fs_backend.try_exists(&dir_path).unwrap());
        }
    }

    mod create_dir_all {
        use super::*;

        #[rstest]
        fn test_create_dir_all(fs_backend: FileSystemBackend) {
            let dir_path = fs_backend.path().join("new_dir/sub_dir");
            fs_backend.create_dir_all(&dir_path).unwrap();
            assert!(fs_backend.try_exists(&dir_path).unwrap());
        }
    }

    mod read_dir {
        use super::*;
        use std::fs::OpenOptions;

        #[rstest]
        fn test_read_dir(fs_backend: FileSystemBackend) {
            let dir_path = fs_backend.path().join("read_dir_test");
            fs_backend.create_dir_all(&dir_path).unwrap();

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

            fs_backend.create_dir_all(&dir_path.join("child/")).unwrap();

            let entries = fs_backend.read_dir(&dir_path).unwrap();
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
