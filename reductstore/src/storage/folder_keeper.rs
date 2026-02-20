// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::backend::BackendType;
use crate::cfg::{Cfg, InstanceRole};
use crate::core::file_cache::FILE_CACHE;
use crate::core::sync::AsyncRwLock;
use crate::storage::block_manager::BLOCK_INDEX_FILE;
use crate::storage::proto::folder_map::Item;
use crate::storage::proto::FolderMap;
use log::warn;
use prost::Message;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::io::SeekFrom::Start;
use std::io::{Read, Write};
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DiscoveryDepth {
    FirstLevel,
    Recursive,
}

/// A simple folder keeper that maintains a list of folders in a `.folder` file.
///
/// Mostly needed for S3 compatible storage backends that do not support listing folders natively.
pub(super) struct FolderKeeper {
    path: PathBuf,
    full_access: bool,
    depth: DiscoveryDepth,
    map: AsyncRwLock<FolderMap>,
}

impl FolderKeeper {
    pub async fn new(path: PathBuf, cfg: &Cfg) -> Self {
        Self::new_with_depth(path, cfg, DiscoveryDepth::Recursive).await
    }

    pub async fn new_with_depth(path: PathBuf, cfg: &Cfg, depth: DiscoveryDepth) -> Self {
        let list_path = path.join(".folder");
        let full_access = cfg.role != InstanceRole::Replica;

        // for Filesystem backend, always rebuild from FS since it is cheap and reliable
        let proto = if cfg.cs_config.backend_type == BackendType::Filesystem {
            let proto = Self::build_from_fs(&path, depth).await;
            if full_access {
                if let Err(err) =
                    Self::save_static(&list_path, &AsyncRwLock::new(proto.clone())).await
                {
                    warn!("Failed to persist folder map at {:?}: {}", list_path, err);
                }
            }
            proto
        } else {
            Self::read_or_build_map(&path, &list_path, full_access, depth).await
        };

        FolderKeeper {
            path,
            full_access,
            depth,
            map: AsyncRwLock::new(proto),
        }
    }

    async fn read_or_build_map(
        path: &PathBuf,
        list_path: &PathBuf,
        save_on_change: bool,
        depth: DiscoveryDepth,
    ) -> FolderMap {
        if FILE_CACHE.try_exists(list_path).await.unwrap_or(false) {
            match Self::read_folder_map(list_path).await {
                Ok(map) => map,
                Err(err) => {
                    warn!(
                        "Failed to decode folder map at {:?}: {}. Rebuilding cache.",
                        list_path, err
                    );
                    Self::build_from_fs(path, depth).await
                }
            }
        } else {
            let proto = Self::build_from_fs(path, depth).await;
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
            if self.depth == DiscoveryDepth::FirstLevel
                && (item.folder_name.contains('/') || item.folder_name.contains('\\'))
            {
                continue;
            }
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
        FILE_CACHE.invalidate_local_cache_file(&file_path).await?;
        let proto =
            Self::read_or_build_map(&self.path, &self.path.join(".folder"), false, self.depth)
                .await;
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

    async fn build_from_fs(path: &PathBuf, depth: DiscoveryDepth) -> FolderMap {
        if depth == DiscoveryDepth::FirstLevel {
            let mut proto = FolderMap { items: vec![] };
            for item in FILE_CACHE.read_dir(path).await.unwrap_or_default() {
                if item.is_dir() {
                    let skip_dir = item
                        .file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|name| name.starts_with('.'));
                    if !skip_dir {
                        if let Some(name) = item.file_name().and_then(|n| n.to_str()) {
                            proto.items.push(Item {
                                name: name.to_string(),
                                folder_name: name.to_string(),
                            });
                        }
                    }
                }
            }
            proto
                .items
                .sort_by(|a, b| a.folder_name.cmp(&b.folder_name));
            return proto;
        }

        let mut proto = FolderMap { items: vec![] };
        let mut stack = vec![path.clone()];

        while let Some(current) = stack.pop() {
            let mut child_dirs = Vec::new();
            for item in FILE_CACHE.read_dir(&current).await.unwrap_or_default() {
                if item.is_dir() {
                    let skip_dir = item
                        .file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|name| name.starts_with('.') || name == "wal");
                    if !skip_dir {
                        child_dirs.push(item);
                    }
                }
            }

            let has_block_index = FILE_CACHE
                .try_exists(&current.join(BLOCK_INDEX_FILE))
                .await
                .unwrap_or(false);

            if current != *path && (has_block_index || child_dirs.is_empty()) {
                if let Ok(relative) = current.strip_prefix(path) {
                    let name = relative.to_string_lossy().replace('\\', "/");
                    proto.items.push(Item {
                        name: name.clone(),
                        folder_name: name,
                    });
                }
            }

            // Keep traversing even if current directory is already an entry.
            // This allows nested entries like `entry` and `entry/a` to coexist.
            stack.extend(child_dirs);
        }

        proto
            .items
            .sort_by(|a, b| a.folder_name.cmp(&b.folder_name));

        proto
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::backend::BackendType;
    use crate::cfg::{Cfg, InstanceRole};
    use crate::core::file_cache::FILE_CACHE;
    use crate::storage::block_manager::BLOCK_INDEX_FILE;
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
    async fn scans_nested_entry_paths_for_filesystem_backend(#[future] path: PathBuf) {
        let path = path.await;
        let base_path = path.join("fs_bucket_nested");
        FILE_CACHE.create_dir_all(&base_path).await.unwrap();
        FILE_CACHE
            .create_dir_all(&base_path.join("entry").join("a"))
            .await
            .unwrap();
        FILE_CACHE
            .write_or_create(
                &base_path.join("entry").join("a").join(BLOCK_INDEX_FILE),
                SeekFrom::Start(0),
            )
            .await
            .unwrap();

        let mut cfg = Cfg::default();
        cfg.cs_config.backend_type = BackendType::Filesystem;

        let keeper = FolderKeeper::new(base_path.clone(), &cfg).await;
        let folders = keeper.list_folders().await.unwrap();

        assert!(
            folders.iter().any(|path| path.ends_with("entry/a")),
            "Should include nested entry path"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn ignores_wal_directories_for_filesystem_backend(#[future] path: PathBuf) {
        let path = path.await;
        let base_path = path.join("fs_bucket_ignore_wal");
        FILE_CACHE.create_dir_all(&base_path).await.unwrap();
        FILE_CACHE
            .create_dir_all(&base_path.join("entry").join("wal"))
            .await
            .unwrap();

        let mut cfg = Cfg::default();
        cfg.cs_config.backend_type = BackendType::Filesystem;

        let keeper = FolderKeeper::new(base_path.clone(), &cfg).await;
        let folders = keeper.list_folders().await.unwrap();

        assert!(
            !folders.iter().any(|path| path.ends_with("entry/wal")),
            "Should ignore internal wal directory"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn scans_parent_and_nested_entries_when_both_have_index(#[future] path: PathBuf) {
        let path = path.await;
        let base_path = path.join("fs_bucket_parent_and_nested");
        FILE_CACHE.create_dir_all(&base_path).await.unwrap();
        FILE_CACHE
            .create_dir_all(&base_path.join("entry"))
            .await
            .unwrap();
        FILE_CACHE
            .write_or_create(
                &base_path.join("entry").join(BLOCK_INDEX_FILE),
                SeekFrom::Start(0),
            )
            .await
            .unwrap();
        FILE_CACHE
            .create_dir_all(&base_path.join("entry").join("a"))
            .await
            .unwrap();
        FILE_CACHE
            .write_or_create(
                &base_path.join("entry").join("a").join(BLOCK_INDEX_FILE),
                SeekFrom::Start(0),
            )
            .await
            .unwrap();

        let mut cfg = Cfg::default();
        cfg.cs_config.backend_type = BackendType::Filesystem;
        let keeper = FolderKeeper::new(base_path.clone(), &cfg).await;
        let folders = keeper.list_folders().await.unwrap();

        assert!(folders.iter().any(|p| p.ends_with("entry")));
        assert!(folders.iter().any(|p| p.ends_with("entry/a")));
    }

    #[rstest]
    #[tokio::test]
    async fn first_level_discovery_filters_nested_paths(#[future] path: PathBuf) {
        let path = path.await;
        let base_path = path.join("fs_first_level_only");
        FILE_CACHE.create_dir_all(&base_path).await.unwrap();
        FILE_CACHE
            .create_dir_all(&base_path.join("bucket-1"))
            .await
            .unwrap();
        FILE_CACHE
            .create_dir_all(&base_path.join("bucket-1").join("entry"))
            .await
            .unwrap();

        let mut cfg = Cfg::default();
        cfg.cs_config.backend_type = BackendType::Filesystem;
        let keeper =
            FolderKeeper::new_with_depth(base_path.clone(), &cfg, DiscoveryDepth::FirstLevel).await;
        let folders = keeper.list_folders().await.unwrap();

        assert!(folders.iter().any(|p| p.ends_with("bucket-1")));
        assert!(!folders.iter().any(|p| p.ends_with("bucket-1/entry")));
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
