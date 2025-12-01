// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::core::file_cache::FILE_CACHE;
use crate::core::sync::RwLock;
use crate::storage::proto::folder_map::Item;
use crate::storage::proto::FolderMap;
use parking_lot::RwLockWriteGuard;
use prost::Message;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::io::SeekFrom::Start;
use std::io::{Read, Write};
use std::path::PathBuf;

/// A simple folder browser that lists and manages folders using a protobuf file for mapping.
///
/// Mostly needed for S3 compatible storage backends that do not support listing folders natively.
pub(super) struct FolderBrowser {
    path: PathBuf,
    mapper: RwLock<FolderMap>,
}

impl FolderBrowser {
    pub fn new(path: PathBuf) -> Self {
        let list_path = path.join(".folder");
        let proto = if FILE_CACHE.try_exists(&list_path).unwrap_or(false) {
            let file = FILE_CACHE
                .read(&list_path, Start(0))
                .unwrap()
                .upgrade()
                .unwrap();

            let mut lock = file.write().unwrap();
            let mut buf = Vec::new();
            lock.read_to_end(&mut buf).unwrap();

            FolderMap::decode(&buf[..]).unwrap()
        } else {
            let mut proto = FolderMap { items: vec![] };
            for path in FILE_CACHE.read_dir(&path).unwrap() {
                if path.is_dir() {
                    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                        if !name.starts_with('.') {
                            proto.items.push(Item {
                                name: name.to_string(),
                                folder_name: name.to_string(),
                            });
                        }
                    }
                }
            }

            let mut buf = Vec::with_capacity(proto.encoded_len());
            proto.encode(&mut buf).unwrap();
            let file = FILE_CACHE
                .write_or_create(&list_path, Start(0))
                .unwrap()
                .upgrade()
                .unwrap();

            let mut lock = file.write().unwrap();
            lock.write_all(&buf).unwrap();
            lock.flush().unwrap();
            lock.sync_all().unwrap();

            proto
        };

        FolderBrowser {
            path,
            mapper: RwLock::new(proto),
        }
    }

    pub fn list_folders(&self) -> Result<Vec<PathBuf>, ReductError> {
        let mut folders = Vec::new();
        for item in &self.mapper.read()?.items {
            let folder_path = self.path.join(&item.folder_name);
            folders.push(folder_path);
        }
        Ok(folders)
    }

    pub fn add_folder(&self, folder_name: &str) -> Result<(), ReductError> {
        let folder_path = self.path.join(folder_name);
        FILE_CACHE.create_dir_all(&folder_path)?;
        {
            let mut mapper = self.mapper.write()?;

            if !mapper
                .items
                .iter()
                .any(|item| item.folder_name == folder_name)
            {
                mapper.items.push(Item {
                    name: folder_name.to_string(),
                    folder_name: folder_name.to_string(),
                });
            }
        }

        self.save()
    }

    pub fn remove_folder(&self, folder_name: &str) -> Result<(), ReductError> {
        let folder_path = self.path.join(folder_name);
        FILE_CACHE.remove_dir(&folder_path)?;
        {
            let mut mapper = self.mapper.write()?;
            mapper.items.retain(|item| item.folder_name != folder_name);
        }
        self.save()
    }

    fn save(&self) -> Result<(), ReductError> {
        let mapper = self.mapper.read()?;
        let mut buf = Vec::new();
        mapper
            .encode(&mut buf)
            .map_err(|e| internal_server_error!("Failed to encode folder map: {}", e))?;
        let list_path = self.path.join(".folder");
        let file = FILE_CACHE
            .write_or_create(&list_path, Start(0))?
            .upgrade()?;
        let mut lock = file.write()?;
        lock.write_all(&buf)?;
        lock.flush()?;
        Ok(())
    }
}
