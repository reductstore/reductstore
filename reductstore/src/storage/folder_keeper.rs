// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::cfg::{Cfg, InstanceRole};
use crate::core::file_cache::FILE_CACHE;
use crate::core::sync::RwLock;
use crate::storage::proto::folder_map::Item;
use crate::storage::proto::FolderMap;
use log::warn;
use prost::Message;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::io::SeekFrom::Start;
use std::io::{Read, Write};
use std::path::PathBuf;

/// A simple folder keeper that maintains a list of folders in a `.folder` file.
///
/// Mostly needed for S3 compatible storage backends that do not support listing folders natively.
pub(super) struct FolderKeeper {
    path: PathBuf,
    full_access: bool,
    map: RwLock<FolderMap>,
}

impl FolderKeeper {
    pub fn new(path: PathBuf, cfg: &Cfg) -> Self {
        let list_path = path.join(".folder");
        let full_access = cfg.role != InstanceRole::Replica;
        let proto = Self::read_or_build_map(&path, &list_path, full_access);

        FolderKeeper {
            path,
            full_access,
            map: RwLock::new(proto),
        }
    }

    fn read_or_build_map(path: &PathBuf, list_path: &PathBuf, save_on_change: bool) -> FolderMap {
        let proto = if FILE_CACHE.try_exists(&list_path).unwrap_or(false) {
            let read_and_encode = || {
                let file = FILE_CACHE.read(&list_path, Start(0))?.upgrade()?;
                let mut lock = file.write()?;
                let mut buf = Vec::new();
                lock.read_to_end(&mut buf)?;
                FolderMap::decode(&buf[..]).map_err(|err| internal_server_error!("{}", err))
            };

            match read_and_encode() {
                Ok(map) => map,
                Err(err) => {
                    warn!(
                        "Failed to decode folder map at {:?}: {}. Rebuilding cache.",
                        list_path, err
                    );
                    Self::build_from_fs(&path)
                }
            }
        } else {
            let proto = Self::build_from_fs(&path);
            if save_on_change {
                Self::save_static(&list_path, &proto).unwrap();
            }
            proto
        };
        proto
    }

    pub fn list_folders(&self) -> Result<Vec<PathBuf>, ReductError> {
        let mut folders = Vec::new();
        for item in &self.map.read()?.items {
            let folder_path = self.path.join(&item.folder_name);
            folders.push(folder_path);
        }
        Ok(folders)
    }

    pub fn add_folder(&self, folder_name: &str) -> Result<(), ReductError> {
        let folder_path = self.path.join(folder_name);
        FILE_CACHE.create_dir_all(&folder_path)?;
        {
            let mut map = self.map.write()?;

            if !map.items.iter().any(|item| item.folder_name == folder_name) {
                map.items.push(Item {
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
            let mut map = self.map.write()?;
            map.items.retain(|item| item.folder_name != folder_name);
        }
        self.save()
    }

    pub fn rename_folder(&self, old_name: &str, new_name: &str) -> Result<(), ReductError> {
        let old_path = self.path.join(old_name);
        let new_path = self.path.join(new_name);
        FILE_CACHE.rename(&old_path, &new_path)?;
        {
            let mut map = self.map.write()?;
            if let Some(item) = map
                .items
                .iter_mut()
                .find(|item| item.folder_name == old_name)
            {
                item.name = new_name.to_string();
                item.folder_name = new_name.to_string();
            }
        }
        self.save()
    }

    /// Reload the folder map from the filesystem, discarding any cached version.
    /// Used in ReadOnly mode to sync folder list from backend storage.
    pub fn reload(&self) -> Result<(), ReductError> {
        let file_path = self.path.join(".folder"); // remove cached file
        FILE_CACHE.discard_recursive(&file_path)?;
        let proto = Self::read_or_build_map(&self.path, &self.path.join(".folder"), false);
        let mut map = self.map.write()?;
        *map = proto;
        Ok(())
    }

    fn save(&self) -> Result<(), ReductError> {
        if !self.full_access {
            return Ok(());
        }

        let map = self.map.read()?;
        Self::save_static(&self.path.join(".folder"), &map)?;
        Ok(())
    }

    fn save_static(path: &PathBuf, map: &FolderMap) -> Result<(), ReductError> {
        let mut buf = Vec::new();
        map.encode(&mut buf)
            .map_err(|e| internal_server_error!("Failed to encode folder map: {}", e))?;
        let file = FILE_CACHE.write_or_create(path, Start(0))?.upgrade()?;
        let mut lock = file.write()?;
        lock.set_len(0)?; // truncate the file before writing
        lock.write_all(&buf)?;
        lock.sync_all()?;
        Ok(())
    }

    fn build_from_fs(path: &PathBuf) -> FolderMap {
        let mut proto = FolderMap { items: vec![] };
        for path in FILE_CACHE.read_dir(path).unwrap() {
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

        proto
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Backend;
    use crate::cfg::{Cfg, InstanceRole};
    use crate::core::file_cache::FILE_CACHE;
    use rstest::{fixture, rstest};
    use std::io::SeekFrom;
    use tempfile::tempdir;

    #[fixture]
    pub fn path() -> PathBuf {
        let path = tempdir().unwrap().keep();
        FILE_CACHE.set_storage_backend(
            Backend::builder()
                .local_data_path(path.clone())
                .try_build()
                .unwrap(),
        );
        path
    }

    #[rstest]
    fn ignores_invalid_folder_map(path: PathBuf) {
        let base_path = path.join("bucket");
        FILE_CACHE.create_dir_all(&base_path).unwrap();

        {
            let list_path = base_path.join(".folder");
            let file = FILE_CACHE
                .write_or_create(&list_path, SeekFrom::Start(0))
                .unwrap()
                .upgrade()
                .unwrap();
            let mut lock = file.write().unwrap();
            lock.write_all(b"invalid-folder-map").unwrap();
            lock.flush().unwrap()
        };

        let cfg = Cfg::default();
        let keeper = FolderKeeper::new(base_path.clone(), &cfg);
        let folders = keeper.list_folders().unwrap();
        assert!(folders.is_empty());
    }

    #[rstest]
    fn does_not_persist_folder_map_in_replica_mode(path: PathBuf) {
        let base_path = path.join("replica_bucket");
        FILE_CACHE.create_dir_all(&base_path).unwrap();

        let mut cfg = Cfg::default();
        cfg.role = InstanceRole::Replica;

        let _ = FolderKeeper::new(base_path.clone(), &cfg);

        let list_path = base_path.join(".folder");
        assert!(
            !FILE_CACHE.try_exists(&list_path).unwrap_or(false),
            ".folder should not be created in replica mode"
        );
    }
}
