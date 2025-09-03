// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

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
}
