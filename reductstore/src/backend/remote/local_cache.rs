// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use log::{info, warn};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;
use std::{fs, io};

pub(super) struct LocalCache {
    path: PathBuf,
    max_size: u64,
    current_size: u64,
    entries: HashMap<PathBuf, crate::backend::remote::LocalCacheEntry>,
}

#[allow(dead_code)]
impl LocalCache {
    pub fn new(path: PathBuf, max_size: u64) -> Self {
        info!("Cleaning up local cache at {:?}", path);
        if let Err(err) = fs::remove_dir_all(&path) {
            if err.kind() != io::ErrorKind::NotFound {
                warn!("Failed to clean up local cache at {:?}: {}", path, err);
            }
        }

        fs::create_dir_all(&path).expect("Failed to create local cache directory");

        LocalCache {
            path,
            max_size,
            current_size: 0,
            entries: HashMap::new(),
        }
    }

    pub fn build_key(&self, path: &Path) -> String {
        path.strip_prefix(&self.path)
            .unwrap()
            .to_str()
            .unwrap_or("")
            .to_string()
            .replace('\\', "/") // normalize Windows paths
    }

    pub fn remove(&mut self, path: &Path) -> io::Result<()> {
        if let Err(err) = fs::remove_file(path) {
            if err.kind() != io::ErrorKind::NotFound {
                return Err(err);
            }
        }

        if let Some(entry) = self.entries.remove(path) {
            self.current_size -= entry.size;
        }
        Ok(())
    }

    pub fn remove_all(&mut self, dir: &Path) -> io::Result<()> {
        fs::remove_dir_all(dir)?;

        let paths_to_remove: Vec<PathBuf> = self
            .entries
            .keys()
            .filter(|p| p.starts_with(dir))
            .cloned()
            .collect();

        for path in paths_to_remove {
            if let Err(err) = self.remove(&path) {
                warn!("Failed to remove cached file {:?}: {}", path, err);
            }
        }

        Ok(())
    }

    pub fn rename(&mut self, from: &Path, to: &Path) -> io::Result<()> {
        fs::rename(from, to)?;

        if let Some(entry) = self.entries.remove(from) {
            self.entries.insert(to.to_path_buf(), entry);
        }
        Ok(())
    }

    pub fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        fs::create_dir_all(path)
    }

    pub fn try_exists(&self, path: &Path) -> io::Result<bool> {
        let full_path = self.path.join(path);
        full_path.try_exists()
    }

    pub fn register_file(&mut self, path: &Path) -> io::Result<()> {
        let metadata = fs::metadata(&path)?;
        let file_size = metadata.len();

        self.current_size += file_size;
        if let Some(previous) = self.entries.insert(
            path.to_path_buf(),
            crate::backend::remote::LocalCacheEntry {
                last_accessed: Instant::now(),
                size: file_size,
            },
        ) {
            self.current_size -= previous.size;
        }

        Ok(())
    }

    pub fn access_file(&mut self, path: &Path) -> io::Result<()> {
        if let Some(entry) = self.entries.get_mut(path) {
            entry.last_accessed = Instant::now();
        } else {
            self.register_file(path)?;
        }
        Ok(())
    }

    pub fn invalidate_old_files(&mut self) -> Vec<PathBuf> {
        let mut removed_files = vec![];

        if self.current_size <= self.max_size {
            return removed_files;
        }

        let mut entries: Vec<(&PathBuf, &crate::backend::remote::LocalCacheEntry)> =
            self.entries.iter().collect();
        entries.sort_by_key(|&(_, entry)| entry.last_accessed);

        for (path, entry) in entries {
            if self.current_size <= self.max_size {
                break;
            }

            self.current_size -= entry.size;
            removed_files.push(path.clone());
        }

        for path in &removed_files {
            self.entries.remove(path);
        }

        removed_files
    }

    pub fn invalidate_file(&mut self, path: &Path) -> io::Result<()> {
        if let Some(entry) = self.entries.remove(path) {
            self.current_size -= entry.size;
            fs::remove_file(path)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    mod new {
        use super::*;
        use std::fs;

        #[rstest]
        fn test_clean_folder_before_create(path: PathBuf) {
            fs::create_dir_all(&path).unwrap();
            fs::write(path.join("test_file"), b"test").unwrap();

            let _local_cache = LocalCache::new(path.clone(), 1024 * 1024);

            assert!(path.exists());
            assert!(!path.join("test_file").exists());
        }

        #[rstest]
        fn test_if_path_does_not_exist(path: PathBuf) {
            let cache_path = path.join("non_existent_cache");
            let _local_cache = LocalCache::new(cache_path.clone(), 1024 * 1024);

            assert!(cache_path.exists(), "Cache directory should be created");
        }
    }

    mod build_key {
        use super::*;

        #[rstest]
        fn test_build_key(local_cache: LocalCache) {
            let file_path = local_cache.path.join("dir").join("file.txt");
            let key = local_cache.build_key(&file_path);

            assert_eq!(key, "dir/file.txt");
        }
    }

    mod remove {
        use super::*;

        #[rstest]
        fn test_remove_registered_file(mut local_cache: LocalCache) {
            let file_path = local_cache.path.join("file_to_remove.txt");
            fs::create_dir_all(local_cache.path.clone()).unwrap();
            fs::write(&file_path, b"test").unwrap();
            local_cache.register_file(&file_path).unwrap();

            assert!(file_path.exists());
            assert!(local_cache.entries.contains_key(&file_path));
            assert_eq!(local_cache.current_size, 4);

            local_cache.remove(&file_path).unwrap();

            assert!(!file_path.exists());
            assert!(!local_cache.entries.contains_key(&file_path));
            assert_eq!(local_cache.current_size, 0);
        }

        #[rstest]
        fn test_remove_non_registered_file(mut local_cache: LocalCache) {
            let file_path = local_cache.path.join("file_to_remove.txt");
            fs::create_dir_all(local_cache.path.clone()).unwrap();
            fs::write(&file_path, b"test").unwrap();

            assert!(file_path.exists());
            assert!(!local_cache.entries.contains_key(&file_path));
            assert_eq!(local_cache.current_size, 0);

            local_cache.remove(&file_path).unwrap();

            assert!(!file_path.exists());
            assert!(!local_cache.entries.contains_key(&file_path));
            assert_eq!(local_cache.current_size, 0);
        }
    }

    mod remove_all {
        use super::*;

        #[rstest]
        fn test_remove_all_registered_files(mut local_cache: LocalCache) {
            let dir_to_remove = local_cache.path.join("dir_to_remove");
            let file_path1 = dir_to_remove.join("file1.txt");
            let file_path2 = dir_to_remove.join("file2.txt");
            fs::create_dir_all(&dir_to_remove).unwrap();
            fs::write(&file_path1, b"test1").unwrap();
            fs::write(&file_path2, b"test2").unwrap();
            local_cache.register_file(&file_path1).unwrap();
            local_cache.register_file(&file_path2).unwrap();

            let dir_to_keep = local_cache.path.join("dir_to_keep");
            let file_path3 = dir_to_keep.join("file1.txt");
            fs::create_dir_all(&dir_to_keep).unwrap();
            fs::write(&file_path3, b"test1").unwrap();
            local_cache.register_file(&file_path3).unwrap();

            assert!(dir_to_remove.exists());
            assert!(dir_to_keep.exists());
            assert!(local_cache.entries.contains_key(&file_path1));
            assert!(local_cache.entries.contains_key(&file_path2));
            assert!(local_cache.entries.contains_key(&file_path3));
            assert_eq!(local_cache.current_size, 15);

            local_cache.remove_all(&dir_to_remove).unwrap();

            assert!(!dir_to_remove.exists());
            assert!(dir_to_keep.exists());
            assert!(!local_cache.entries.contains_key(&file_path1));
            assert!(!local_cache.entries.contains_key(&file_path2));
            assert!(local_cache.entries.contains_key(&file_path3));
            assert_eq!(local_cache.current_size, 5);
        }

        #[rstest]
        fn test_remove_all_non_registered_files(mut local_cache: LocalCache) {
            let dir_to_remove = local_cache.path.join("dir_to_remove");
            let file_path1 = dir_to_remove.join("file1.txt");
            let file_path2 = dir_to_remove.join("file2.txt");
            fs::create_dir_all(&dir_to_remove).unwrap();
            fs::write(&file_path1, b"test1").unwrap();
            fs::write(&file_path2, b"test2").unwrap();

            assert!(dir_to_remove.exists());
            assert!(local_cache.entries.is_empty());
            assert_eq!(local_cache.current_size, 0);

            local_cache.remove_all(&dir_to_remove).unwrap();

            assert!(!dir_to_remove.exists());
            assert!(local_cache.entries.is_empty());
            assert_eq!(local_cache.current_size, 0);
        }
    }

    mod create_dir_all {
        use super::*;

        #[rstest]
        fn test_create_new_directory(local_cache: LocalCache) {
            let new_dir = local_cache.path.join("new_dir");

            assert!(!new_dir.exists());

            local_cache.create_dir_all(&new_dir).unwrap();

            assert!(new_dir.exists());
            assert!(new_dir.is_dir());
        }

        #[rstest]
        fn test_create_existing_directory(local_cache: LocalCache) {
            let existing_dir = local_cache.path.join("existing_dir");
            fs::create_dir_all(&existing_dir).unwrap();

            assert!(existing_dir.exists());
            assert!(existing_dir.is_dir());

            local_cache.create_dir_all(&existing_dir).unwrap();

            assert!(existing_dir.exists());
            assert!(existing_dir.is_dir());
        }
    }

    mod try_exists {
        use super::*;

        #[rstest]
        fn test_try_exists_existing_file(local_cache: LocalCache) {
            let file_path = local_cache.path.join("existing_file.txt");
            fs::create_dir_all(local_cache.path.clone()).unwrap();
            fs::write(&file_path, b"test").unwrap();

            assert!(local_cache.try_exists(&file_path).unwrap());
        }

        #[rstest]
        fn test_try_exists_non_existing_file(local_cache: LocalCache) {
            let file_path = local_cache.path.join("non_existing_file.txt");

            assert!(!local_cache.try_exists(&file_path).unwrap());
        }
    }

    mod register_file {
        use super::*;

        #[rstest]
        fn test_register_new_file(local_cache_with_file: (LocalCache, PathBuf)) {
            let (local_cache, file_path) = local_cache_with_file;
            assert!(local_cache.entries.contains_key(&file_path));
            assert_eq!(local_cache.current_size, 4);
        }

        #[rstest]
        fn test_register_existing_file(local_cache_with_file: (LocalCache, PathBuf)) {
            let (mut local_cache, file_path) = local_cache_with_file;
            fs::write(&file_path, b"test-more-data").unwrap();

            local_cache.register_file(&file_path).unwrap();
            assert!(local_cache.entries.contains_key(&file_path));
            assert_eq!(local_cache.current_size, 14, "should recalculate size");
        }
    }

    mod access_file {
        use super::*;
        use std::thread;
        use std::time::Duration;

        #[rstest]
        fn test_access_registered_file(local_cache_with_file: (LocalCache, PathBuf)) {
            let (mut local_cache, file_path) = local_cache_with_file;
            let previous_access_time = local_cache.entries.get(&file_path).unwrap().last_accessed;

            thread::sleep(Duration::from_millis(10));
            local_cache.access_file(&file_path).unwrap();
            let new_access_time = local_cache.entries.get(&file_path).unwrap().last_accessed;

            assert!(new_access_time > previous_access_time);
            assert_eq!(local_cache.current_size, 4);
        }
    }

    mod invalidate_old_files {
        use super::*;
        use std::thread;
        use std::time::Duration;

        #[rstest]
        fn test_invalidate_when_under_limit(mut local_cache: LocalCache) {
            let removed_files = local_cache.invalidate_old_files();
            assert!(removed_files.is_empty());
            assert_eq!(local_cache.current_size, 0);
        }

        #[rstest]
        fn test_invalidate_when_over_limit(mut local_cache: LocalCache) {
            let file_path1 = local_cache.path.join("file1.txt");
            let file_path2 = local_cache.path.join("file2.txt");
            fs::create_dir_all(local_cache.path.clone()).unwrap();
            fs::write(&file_path1, b"data1").unwrap();
            fs::write(&file_path2, b"data2").unwrap();
            local_cache.register_file(&file_path1).unwrap();
            local_cache.register_file(&file_path2).unwrap();

            local_cache.access_file(&file_path2).unwrap();
            thread::sleep(Duration::from_millis(10)); // ensure different access times
            local_cache.access_file(&file_path1).unwrap();

            assert_eq!(local_cache.current_size, 10);

            // Set max size to 6 to force invalidation
            local_cache.max_size = 6;
            let removed_files = local_cache.invalidate_old_files();

            assert_eq!(removed_files.len(), 1);
            assert!(removed_files.contains(&file_path2), "file2 remains");
            assert!(!removed_files.contains(&file_path1));
            assert_eq!(local_cache.current_size, 5);
        }
    }

    mod invalidate_file {
        use super::*;

        #[rstest]
        fn test_invalidate_registered_file(local_cache_with_file: (LocalCache, PathBuf)) {
            let (mut local_cache, file_path) = local_cache_with_file;

            assert!(file_path.exists());
            assert!(local_cache.entries.contains_key(&file_path));
            assert_eq!(local_cache.current_size, 4);

            local_cache.invalidate_file(&file_path).unwrap();

            assert!(!file_path.exists());
            assert!(!local_cache.entries.contains_key(&file_path));
            assert_eq!(local_cache.current_size, 0);
        }

        #[rstest]
        fn test_invalidate_non_registered_file(mut local_cache: LocalCache) {
            let file_path = local_cache.path.join("non_registered_file.txt");
            fs::create_dir_all(local_cache.path.clone()).unwrap();
            fs::write(&file_path, b"test").unwrap();

            assert!(file_path.exists());
            assert!(!local_cache.entries.contains_key(&file_path));
            assert_eq!(local_cache.current_size, 0);

            local_cache.invalidate_file(&file_path).unwrap();

            assert!(file_path.exists());
            assert!(!local_cache.entries.contains_key(&file_path));
            assert_eq!(local_cache.current_size, 0);
        }
    }

    #[fixture]
    fn path() -> PathBuf {
        let temp_dir = tempfile::tempdir().unwrap();
        temp_dir.keep().join("cache")
    }

    #[fixture]
    fn local_cache(path: PathBuf) -> LocalCache {
        LocalCache::new(path.clone(), 1024 * 1024)
    }

    #[fixture]
    fn local_cache_with_file(path: PathBuf) -> (LocalCache, PathBuf) {
        let mut local_cache = LocalCache::new(path.clone(), 1024 * 1024);
        let file_path = path.join("file.txt");
        fs::create_dir_all(path.clone()).unwrap();
        fs::write(&file_path, b"test").unwrap();
        local_cache.register_file(&file_path).unwrap();
        (local_cache, file_path)
    }
}
