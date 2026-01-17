// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::backend::BackendType;
use crate::cfg::{Cfg, InstanceRole};
use crate::core::file_cache::FILE_CACHE;
use crate::core::sync::AsyncRwLock;
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
    map: AsyncRwLock<FolderMap>,
}

impl FolderKeeper {
    pub async fn new(path: PathBuf, cfg: &Cfg) -> Self {
        let list_path = path.join(".folder");
        let full_access = cfg.role != InstanceRole::Replica;

        // for Filesystem backend, always rebuild from FS since it is cheap and reliable
        let proto = if cfg.cs_config.backend_type == BackendType::Filesystem {
            let proto = Self::build_from_fs(&path).await;
            if full_access {
                if let Err(err) =
                    Self::save_static(&list_path, &AsyncRwLock::new(proto.clone())).await
                {
                    warn!("Failed to persist folder map at {:?}: {}", list_path, err);
                }
            }
            proto
        } else {
            Self::read_or_build_map(&path, &list_path, full_access).await
        };

        FolderKeeper {
            path,
            full_access,
            map: AsyncRwLock::new(proto),
        }
    }

    async fn read_or_build_map(
        path: &PathBuf,
        list_path: &PathBuf,
        save_on_change: bool,
    ) -> FolderMap {
        if FILE_CACHE.try_exists(list_path).await.unwrap_or(false) {
            match Self::read_folder_map(list_path).await {
                Ok(map) => map,
                Err(err) => {
                    warn!(
                        "Failed to decode folder map at {:?}: {}. Rebuilding cache.",
                        list_path, err
                    );
                    Self::build_from_fs(path).await
                }
            }
        } else {
            let proto = Self::build_from_fs(path).await;
            if save_on_change {
                Self::save_static(list_path, &AsyncRwLock::new(proto.clone()))
                    .await
                    .expect("Failed to persist folder map");
            }
            proto
        }
    }

    async fn read_folder_map(list_path: &PathBuf) -> Result<FolderMap, ReductError> {
        let mut lock = FILE_CACHE.read(list_path, Start(0)).await?;
        let mut buf = Vec::new();
        lock.read_to_end(&mut buf)?;
        FolderMap::decode(&buf[..]).map_err(|err| internal_server_error!("{}", err))
    }

    pub async fn list_folders(&self) -> Result<Vec<PathBuf>, ReductError> {
        let mut folders = Vec::new();
        for item in &self.map.read().await?.items {
            let folder_path = self.path.join(&item.folder_name);
            folders.push(folder_path);
        }
        Ok(folders)
    }

    pub async fn add_folder(&self, folder_name: &str) -> Result<(), ReductError> {
        let folder_path = self.path.join(folder_name);
        FILE_CACHE.create_dir_all(&folder_path).await?;
        {
            let mut map = self.map.write().await?;

            if !map.items.iter().any(|item| item.folder_name == folder_name) {
                map.items.push(Item {
                    name: folder_name.to_string(),
                    folder_name: folder_name.to_string(),
                });
            }
        }

        self.save().await
    }

    pub async fn remove_folder(&self, folder_name: &str) -> Result<(), ReductError> {
        let folder_path = self.path.join(folder_name);
        FILE_CACHE.remove_dir(&folder_path).await?;
        {
            let mut map = self.map.write().await?;
            map.items.retain(|item| item.folder_name != folder_name);
        }
        self.save().await
    }

    pub async fn rename_folder(&self, old_name: &str, new_name: &str) -> Result<(), ReductError> {
        let old_path = self.path.join(old_name);
        let new_path = self.path.join(new_name);
        FILE_CACHE.rename(&old_path, &new_path).await?;
        {
            let mut map = self.map.write().await?;
            if let Some(item) = map
                .items
                .iter_mut()
                .find(|item| item.folder_name == old_name)
            {
                item.name = new_name.to_string();
                item.folder_name = new_name.to_string();
            }
        }
        self.save().await
    }

    /// Reload the folder map from the filesystem, discarding any cached version.
    /// Used in ReadOnly mode to sync folder list from backend storage.
    pub async fn reload(&self) -> Result<(), ReductError> {
        let file_path = self.path.join(".folder"); // remove cached file
        FILE_CACHE.discard_recursive(&file_path).await?;
        let proto = Self::read_or_build_map(&self.path, &self.path.join(".folder"), false).await;
        let mut map = self.map.write().await?;
        *map = proto;
        Ok(())
    }

    async fn save(&self) -> Result<(), ReductError> {
        if !self.full_access {
            return Ok(());
        }

        Self::save_static(&self.path.join(".folder"), &self.map).await?;
        Ok(())
    }

    async fn save_static(path: &PathBuf, map: &AsyncRwLock<FolderMap>) -> Result<(), ReductError> {
        let mut buf = Vec::new();
        map.read()
            .await?
            .encode(&mut buf)
            .map_err(|e| internal_server_error!("Failed to encode folder map: {}", e))?;
        let mut lock = FILE_CACHE.write_or_create(path, Start(0)).await?;
        lock.set_len(0)?; // truncate the file before writing
        lock.write_all(&buf)?;
        lock.sync_all().await?;
        Ok(())
    }

    async fn build_from_fs(path: &PathBuf) -> FolderMap {
        let mut proto = FolderMap { items: vec![] };
        for path in FILE_CACHE.read_dir(path).await.unwrap() {
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

    use crate::backend::BackendType;
    use crate::cfg::{Cfg, InstanceRole};
    use crate::core::file_cache::FILE_CACHE;
    use rstest::{fixture, rstest};
    use std::io::SeekFrom;
    use tempfile::tempdir;

    #[fixture]
    pub async fn path() -> PathBuf {
        let path = tempdir().unwrap().keep();
        path
    }

    #[rstest]
    #[tokio::test]
    async fn reads_folder_map_from_cache_for_non_filesystem_backend(#[future] path: PathBuf) {
        let path = path.await;
        let base_path = path.join("s3_bucket");
        FILE_CACHE.create_dir_all(&base_path).await.unwrap();

        // Create a folder on the filesystem
        FILE_CACHE
            .create_dir_all(&base_path.join("entry_1"))
            .await
            .unwrap();

        // Pre-create a .folder file with a different entry to verify it reads from cache
        let list_path = base_path.join(".folder");
        let cached_map = FolderMap {
            items: vec![Item {
                name: "cached_entry".to_string(),
                folder_name: "cached_entry".to_string(),
            }],
        };
        FolderKeeper::save_static(&list_path, &AsyncRwLock::new(cached_map))
            .await
            .unwrap();

        // Configure for S3 backend (non-filesystem)
        let mut cfg = Cfg::default();
        cfg.cs_config.backend_type = BackendType::S3;

        let keeper = FolderKeeper::new(base_path.clone(), &cfg).await;
        let folders = keeper.list_folders().await.unwrap();

        // Should read from the cached .folder file, not rebuild from filesystem
        assert!(
            folders.iter().any(|path| path.ends_with("cached_entry")),
            "Should read cached_entry from .folder file"
        );
        assert!(
            !folders.iter().any(|path| path.ends_with("entry_1")),
            "Should not include entry_1 from filesystem scan"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn builds_folder_map_when_cache_missing_for_non_filesystem_backend(
        #[future] path: PathBuf,
    ) {
        let path = path.await;
        let base_path = path.join("s3_bucket_no_cache");
        FILE_CACHE.create_dir_all(&base_path).await.unwrap();

        // Create folders on the filesystem but no .folder cache file
        FILE_CACHE
            .create_dir_all(&base_path.join("entry_from_fs"))
            .await
            .unwrap();

        // Configure for S3 backend (non-filesystem)
        let mut cfg = Cfg::default();
        cfg.cs_config.backend_type = BackendType::S3;

        let keeper = FolderKeeper::new(base_path.clone(), &cfg).await;
        let folders = keeper.list_folders().await.unwrap();

        // Should rebuild from filesystem since no cache exists
        assert!(
            folders.iter().any(|path| path.ends_with("entry_from_fs")),
            "Should rebuild from filesystem when cache is missing"
        );

        // Should persist the .folder file
        let list_path = base_path.join(".folder");
        assert!(
            FILE_CACHE.try_exists(&list_path).await.unwrap_or(false),
            ".folder should be created after rebuild"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn ignores_invalid_folder_map(#[future] path: PathBuf) {
        let path = path.await;
        let base_path = path.join("bucket");
        FILE_CACHE.create_dir_all(&base_path).await.unwrap();

        {
            let list_path = base_path.join(".folder");
            let mut lock = FILE_CACHE
                .write_or_create(&list_path, SeekFrom::Start(0))
                .await
                .unwrap();
            lock.write_all(b"invalid-folder-map").unwrap();
            lock.flush().unwrap()
        };

        let cfg = Cfg::default();
        let keeper = FolderKeeper::new(base_path.clone(), &cfg).await;
        let folders = keeper.list_folders().await.unwrap();
        assert!(folders.is_empty());
    }

    #[rstest]
    #[rstest]
    #[tokio::test]
    async fn does_not_persist_folder_map_in_replica_mode(#[future] path: PathBuf) {
        let path = path.await;
        let base_path = path.join("replica_bucket");
        FILE_CACHE.create_dir_all(&base_path).await.unwrap();

        let mut cfg = Cfg::default();
        cfg.role = InstanceRole::Replica;

        let _ = FolderKeeper::new(base_path.clone(), &cfg).await;

        let list_path = base_path.join(".folder");
        assert!(
            !FILE_CACHE.try_exists(&list_path).await.unwrap_or(false),
            ".folder should not be created in replica mode"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn rebuilds_folder_map_from_fs_for_filesystem_backend(#[future] path: PathBuf) {
        let path = path.await;
        let base_path = path.join("bucket");
        FILE_CACHE.create_dir_all(&base_path).await.unwrap();

        let list_path = base_path.join(".folder");
        let empty_map = FolderMap { items: vec![] };
        FolderKeeper::save_static(&list_path, &AsyncRwLock::new(empty_map))
            .await
            .unwrap();

        FILE_CACHE
            .create_dir_all(&base_path.join("entry_1"))
            .await
            .unwrap();

        let cfg = Cfg::default();
        let keeper = FolderKeeper::new(base_path.clone(), &cfg).await;
        let folders = keeper.list_folders().await.unwrap();
        assert!(folders.iter().any(|path| path.ends_with("entry_1")));
    }
}
