// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::cfg::InstanceRole;
use crate::core::file_cache::FILE_CACHE;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::fmt::{Display, Formatter};
use std::io::{Read, SeekFrom, Write};
use std::path::Path;
use std::sync::LazyLock;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use uuid::Uuid;

const STORE_ID_FILE: &str = ".uuid";
static BOOT_NODE_ID: LazyLock<String> = LazyLock::new(|| format!("boot-{}", Uuid::new_v4()));

/// A stable identifier for a ReductStore dataset.
///
/// Store IDs identify the same dataset across instances and are stored in `.uuid`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct StoreId(Uuid);

impl StoreId {
    /// Load the store ID or initialize it for a primary or standalone instance.
    pub async fn load_or_create(
        data_path: &Path,
        role: InstanceRole,
        retry_interval: Duration,
        retry_timeout: Duration,
    ) -> Result<Self, ReductError> {
        let path = data_path.join(STORE_ID_FILE);
        let started_at = Instant::now();

        loop {
            if FILE_CACHE.try_exists(&path).await? {
                FILE_CACHE.invalidate_local_cache_file(&path).await?;
                return Self::read(&path).await;
            }

            if matches!(role, InstanceRole::Primary | InstanceRole::Standalone) {
                return Self::create(data_path, &path).await;
            }

            if !retry_timeout.is_zero() && started_at.elapsed() >= retry_timeout {
                return Err(internal_server_error!(
                    "Store ID file '{}' was not initialized by a primary or standalone instance",
                    path.display()
                ));
            }

            sleep(retry_interval).await;
        }
    }

    pub fn as_uuid(self) -> Uuid {
        self.0
    }

    async fn create(data_path: &Path, path: &std::path::PathBuf) -> Result<Self, ReductError> {
        FILE_CACHE.create_dir_all(&data_path.to_path_buf()).await?;

        let id = Self(Uuid::new_v4());
        let mut descriptor = FILE_CACHE.write_or_create(path, SeekFrom::Start(0)).await?;
        descriptor.set_len(0)?;
        descriptor.write_all(id.to_string().as_bytes())?;
        descriptor.sync_all().await?;
        Ok(id)
    }

    async fn read(path: &std::path::PathBuf) -> Result<Self, ReductError> {
        let mut descriptor = FILE_CACHE.read(path, SeekFrom::Start(0)).await?;
        let mut value = String::new();
        descriptor.read_to_string(&mut value)?;
        let id = Uuid::parse_str(&value).map_err(|_| {
            internal_server_error!(
                "Store ID file '{}' does not contain a canonical UUID",
                path.display()
            )
        })?;
        if id.to_string() != value {
            return Err(internal_server_error!(
                "Store ID file '{}' does not contain a canonical UUID",
                path.display()
            ));
        }
        Ok(Self(id))
    }
}

impl Display for StoreId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

/// An informational identifier for a ReductStore node.
///
/// Node IDs identify a running node for support diagnostics.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct NodeId(String);

impl NodeId {
    /// Resolve the node ID from the instance name, machine ID hash with boot identifier, or boot identifier.
    pub fn from_environment() -> Self {
        Self::from_values(
            std::env::var("RS_INSTANCE_NAME").ok().as_deref(),
            machine_id().as_deref(),
            BOOT_NODE_ID.as_str(),
        )
    }

    fn from_values(instance_name: Option<&str>, machine_id: Option<&str>, boot_id: &str) -> Self {
        let id = instance_name
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_string)
            .or_else(|| {
                machine_id
                    .map(str::trim)
                    .filter(|id| !id.is_empty())
                    .map(|id| {
                        format!(
                            "{}-{}",
                            hex::encode(ring::digest::digest(&ring::digest::SHA256, id.as_bytes())),
                            boot_id
                        )
                    })
            })
            .unwrap_or_else(|| boot_id.to_string());
        Self(id)
    }
}

impl Display for NodeId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

fn machine_id() -> Option<String> {
    std::fs::read_to_string("/etc/machine-id").ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Backend;
    use crate::cfg::InstanceRole;
    use crate::core::file_cache::FILE_CACHE;
    use std::fs;
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    async fn persists_store_id_for_primary_and_reuses_it_for_replica() {
        let directory = tempfile::tempdir().unwrap();
        configure_file_cache(directory.path()).await;

        let primary = StoreId::load_or_create(
            directory.path(),
            InstanceRole::Primary,
            Duration::from_millis(1),
            Duration::from_millis(10),
        )
        .await
        .unwrap();

        assert_eq!(
            fs::read_to_string(directory.path().join(".uuid")).unwrap(),
            primary.to_string()
        );

        let replica = StoreId::load_or_create(
            directory.path(),
            InstanceRole::Replica,
            Duration::from_millis(1),
            Duration::from_millis(10),
        )
        .await
        .unwrap();

        assert_eq!(replica, primary);
    }

    #[tokio::test]
    async fn replica_retries_until_primary_persists_store_id() {
        let directory = tempfile::tempdir().unwrap();
        configure_file_cache(directory.path()).await;

        let path = directory.path().to_path_buf();
        let replica = tokio::spawn(async move {
            StoreId::load_or_create(
                &path,
                InstanceRole::Replica,
                Duration::from_millis(1),
                Duration::from_millis(100),
            )
            .await
        });

        sleep(Duration::from_millis(5)).await;
        let primary = StoreId::load_or_create(
            directory.path(),
            InstanceRole::Primary,
            Duration::from_millis(1),
            Duration::from_millis(10),
        )
        .await
        .unwrap();

        assert_eq!(replica.await.unwrap().unwrap(), primary);
    }

    #[tokio::test]
    async fn replica_rejects_missing_uid_after_timeout() {
        let directory = tempfile::tempdir().unwrap();
        configure_file_cache(directory.path()).await;

        let error = StoreId::load_or_create(
            directory.path(),
            InstanceRole::Replica,
            Duration::from_millis(1),
            Duration::from_millis(5),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains(".uuid"));
    }

    #[tokio::test]
    async fn rejects_noncanonical_persisted_uid() {
        let directory = tempfile::tempdir().unwrap();
        configure_file_cache(directory.path()).await;
        fs::write(directory.path().join(".uuid"), "invalid").unwrap();

        let error = StoreId::load_or_create(
            directory.path(),
            InstanceRole::Primary,
            Duration::from_millis(1),
            Duration::from_millis(10),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("canonical UUID"));
    }

    #[test]
    fn node_id_prefers_instance_name() {
        assert_eq!(
            NodeId::from_values(Some("node-a"), Some("machine-id"), "boot-id").to_string(),
            "node-a"
        );
    }

    #[test]
    fn node_id_combines_machine_hash_with_boot_id() {
        assert_eq!(
            NodeId::from_values(None, Some("machine-id"), "boot-id").to_string(),
            "626a34be1bfdb1d11229f71a3a8098dc935a42f7fabe0c45d37f73d58224c559-boot-id"
        );
    }

    #[test]
    fn node_ids_differ_for_instances_on_same_machine() {
        let first = NodeId::from_values(None, Some("machine-id"), "boot-a");
        let second = NodeId::from_values(None, Some("machine-id"), "boot-b");

        assert_ne!(first, second);
    }

    #[test]
    fn node_id_uses_boot_id_without_machine_identity() {
        assert_eq!(
            NodeId::from_values(Some("  "), None, "boot-id").to_string(),
            "boot-id"
        );
    }

    async fn configure_file_cache(path: &std::path::Path) {
        let backend = Backend::builder()
            .local_data_path(path.to_path_buf())
            .try_build()
            .await
            .unwrap();
        FILE_CACHE.set_storage_backend(backend).await;
        FILE_CACHE.set_read_only(false);
    }
}
