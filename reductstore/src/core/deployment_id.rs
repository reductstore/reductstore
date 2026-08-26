// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::cfg::InstanceRole;
use crate::core::file_cache::FILE_CACHE;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::fmt::{Display, Formatter};
use std::io::{Read, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use uuid::Uuid;

const STORE_ID_FILE: &str = ".uuid";
static RUN_ID: LazyLock<String> = LazyLock::new(|| Uuid::new_v4().to_string());

/// A stable identifier for a ReductStore dataset.
///
/// Store IDs identify the same dataset across instances and are stored in `.uuid`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct StoreId(Uuid);

impl StoreId {
    pub fn builder(data_path: &Path, role: InstanceRole) -> StoreIdBuilder<'_> {
        StoreIdBuilder {
            data_path,
            role,
            retry_interval: Duration::from_secs(10),
            retry_timeout: Duration::from_secs(60),
        }
    }

    pub fn as_uuid(self) -> Uuid {
        self.0
    }

    async fn create(data_path: &Path, path: &PathBuf) -> Result<Self, ReductError> {
        FILE_CACHE.create_dir_all(&data_path.to_path_buf()).await?;

        let id = Self(Uuid::new_v4());
        let mut file = FILE_CACHE.write_or_create(path, SeekFrom::Start(0)).await?;
        file.set_len(0)?;
        write!(file, "{id}")?;
        file.sync_all().await?;
        Ok(id)
    }

    async fn read(path: &PathBuf) -> Result<Self, ReductError> {
        let mut file = FILE_CACHE.read(path, SeekFrom::Start(0)).await?;
        let mut value = String::new();
        file.read_to_string(&mut value)?;
        let id = Uuid::parse_str(&value).map_err(|_| Self::invalid_file(path))?;
        if id.to_string() == value {
            Ok(Self(id))
        } else {
            Err(Self::invalid_file(path))
        }
    }

    fn invalid_file(path: &Path) -> ReductError {
        internal_server_error!(
            "Store ID file '{}' does not contain a canonical UUID",
            path.display()
        )
    }
}

impl Display for StoreId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

pub(crate) struct StoreIdBuilder<'a> {
    data_path: &'a Path,
    role: InstanceRole,
    retry_interval: Duration,
    retry_timeout: Duration,
}

impl<'a> StoreIdBuilder<'a> {
    pub fn retry_interval(mut self, interval: Duration) -> Self {
        self.retry_interval = interval;
        self
    }

    pub fn retry_timeout(mut self, timeout: Duration) -> Self {
        self.retry_timeout = timeout;
        self
    }

    /// Load the store ID or initialize it for a primary or standalone instance.
    pub async fn load_or_create(self) -> Result<StoreId, ReductError> {
        let path = self.data_path.join(STORE_ID_FILE);
        let started_at = Instant::now();

        loop {
            if FILE_CACHE.try_exists(&path).await? {
                FILE_CACHE.invalidate_local_cache_file(&path).await?;
                return StoreId::read(&path).await;
            }

            if matches!(self.role, InstanceRole::Primary | InstanceRole::Standalone) {
                return StoreId::create(self.data_path, &path).await;
            }

            if !self.retry_timeout.is_zero() && started_at.elapsed() >= self.retry_timeout {
                return Err(internal_server_error!(
                    "Store ID file '{}' was not initialized by a primary or standalone instance",
                    path.display()
                ));
            }

            sleep(self.retry_interval).await;
        }
    }
}

/// An informational identifier for a ReductStore node.
///
/// Node IDs identify a running node for support diagnostics.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct NodeId(String);

impl NodeId {
    /// Build a node ID from the resolved instance name and the process run ID.
    pub fn from_instance_name(instance_name: &str) -> Self {
        Self(format!("instance:{instance_name}:run:{}", RUN_ID.as_str()))
    }
}

impl Display for NodeId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Backend;
    use crate::cfg::InstanceRole;
    use crate::core::file_cache::FILE_CACHE;
    use serial_test::serial;
    use std::fs;
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    #[serial]
    async fn persists_store_id_for_primary_and_reuses_it_for_replica() {
        let directory = tempfile::tempdir().unwrap();
        configure_file_cache(directory.path()).await;

        let primary = StoreId::builder(directory.path(), InstanceRole::Primary)
            .retry_interval(Duration::from_millis(1))
            .retry_timeout(Duration::from_millis(10))
            .load_or_create()
            .await
            .unwrap();

        assert_eq!(
            fs::read_to_string(directory.path().join(".uuid")).unwrap(),
            primary.to_string()
        );

        let replica = StoreId::builder(directory.path(), InstanceRole::Replica)
            .retry_interval(Duration::from_millis(1))
            .retry_timeout(Duration::from_millis(10))
            .load_or_create()
            .await
            .unwrap();

        assert_eq!(replica, primary);
    }

    #[tokio::test]
    #[serial]
    async fn replica_retries_until_primary_persists_store_id() {
        let directory = tempfile::tempdir().unwrap();
        configure_file_cache(directory.path()).await;

        let path = directory.path().to_path_buf();
        let replica = tokio::spawn(async move {
            StoreId::builder(&path, InstanceRole::Replica)
                .retry_interval(Duration::from_millis(1))
                .retry_timeout(Duration::from_millis(100))
                .load_or_create()
                .await
        });

        sleep(Duration::from_millis(5)).await;
        let primary = StoreId::builder(directory.path(), InstanceRole::Primary)
            .retry_interval(Duration::from_millis(1))
            .retry_timeout(Duration::from_millis(10))
            .load_or_create()
            .await
            .unwrap();

        assert_eq!(replica.await.unwrap().unwrap(), primary);
    }

    #[tokio::test]
    #[serial]
    async fn replica_rejects_missing_uuid_after_timeout() {
        let directory = tempfile::tempdir().unwrap();
        configure_file_cache(directory.path()).await;

        let error = StoreId::builder(directory.path(), InstanceRole::Replica)
            .retry_interval(Duration::from_millis(1))
            .retry_timeout(Duration::from_millis(5))
            .load_or_create()
            .await
            .unwrap_err();

        assert!(error.to_string().contains(".uuid"));
    }

    #[tokio::test]
    #[serial]
    async fn rejects_noncanonical_persisted_uuid() {
        let directory = tempfile::tempdir().unwrap();
        configure_file_cache(directory.path()).await;
        fs::write(directory.path().join(".uuid"), "invalid").unwrap();

        let error = StoreId::builder(directory.path(), InstanceRole::Primary)
            .retry_interval(Duration::from_millis(1))
            .retry_timeout(Duration::from_millis(10))
            .load_or_create()
            .await
            .unwrap_err();

        assert!(error.to_string().contains("canonical UUID"));
    }
    #[test]
    fn node_id_formats_instance_and_run_id() {
        let node_id = NodeId::from_instance_name("edge-pc-1").to_string();

        assert!(node_id.starts_with("instance:edge-pc-1:run:"));
    }

    #[test]
    fn node_ids_differ_per_process_run() {
        let node_id = NodeId::from_instance_name("edge-pc-1").to_string();
        let run_uuid = node_id.strip_prefix("instance:edge-pc-1:run:").unwrap();

        assert!(uuid::Uuid::parse_str(run_uuid).is_ok());
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
