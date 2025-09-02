// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

struct LocalCacheEntry {
    size: u64,
    last_accessed: Instant,
}

pub(super) struct LocalCache {
    size_limit: u64,
    current_size: u64,
    files: HashMap<PathBuf, LocalCacheEntry>,
}

impl LocalCache {
    pub fn new(size_limit: u64) -> Self {
        Self {
            size_limit,
            current_size: 0,
            files: HashMap::new(),
        }
    }

    pub fn access_file(&mut self, path: &PathBuf, size: u64) {
        let now = Instant::now();
        if let Some(entry) = self.files.get_mut(path) {
            entry.last_accessed = now;
        } else {
            self.files.insert(
                path.to_path_buf(),
                LocalCacheEntry {
                    size,
                    last_accessed: now,
                },
            );
            self.current_size += size;
            self.evict_if_needed();
        }
    }

    fn evict_if_needed(&mut self) {
        while self.current_size > self.size_limit {
            if let Some((oldest_path, oldest_entry)) = self
                .files
                .iter()
                .min_by_key(|(_, entry)| entry.last_accessed)
                .map(|(path, entry)| (path.clone(), entry))
            {
                self.current_size -= oldest_entry.size;
                self.files.remove(&oldest_path);
                // Here you would also remove the file from the filesystem
            } else {
                break; // No files to evict
            }
        }
    }
}
