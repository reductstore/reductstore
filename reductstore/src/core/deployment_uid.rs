// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::cfg::InstanceRole;
use crate::core::file_cache::FILE_CACHE;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::fmt::{Display, Formatter};
use std::io::{Read, SeekFrom, Write};
use std::path::Path;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use uuid::Uuid;

const DEPLOYMENT_UID_FILE: &str = ".uuid";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct DeploymentUid(Uuid);

impl DeploymentUid {
    pub async fn load_or_create(
        data_path: &Path,
        role: InstanceRole,
        retry_interval: Duration,
        retry_timeout: Duration,
    ) -> Result<Self, ReductError> {
        let path = data_path.join(DEPLOYMENT_UID_FILE);
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
                    "Deployment UUID file '{}' was not initialized by a primary or standalone instance",
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

        let uid = Self(Uuid::new_v4());
        let mut descriptor = FILE_CACHE.write_or_create(path, SeekFrom::Start(0)).await?;
        descriptor.set_len(0)?;
        descriptor.write_all(uid.to_string().as_bytes())?;
        descriptor.sync_all().await?;
        Ok(uid)
    }

    async fn read(path: &std::path::PathBuf) -> Result<Self, ReductError> {
        let mut descriptor = FILE_CACHE.read(path, SeekFrom::Start(0)).await?;
        let mut value = String::new();
        descriptor.read_to_string(&mut value)?;
        let uid = Uuid::parse_str(&value).map_err(|_| {
            internal_server_error!(
                "Deployment UUID file '{}' does not contain a canonical UUID",
                path.display()
            )
        })?;
        if uid.to_string() != value {
            return Err(internal_server_error!(
                "Deployment UUID file '{}' does not contain a canonical UUID",
                path.display()
            ));
        }
        Ok(Self(uid))
    }
}

impl Display for DeploymentUid {
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
    use std::fs;
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    async fn persists_uid_for_primary_and_reuses_it_for_replica() {
        let directory = tempfile::tempdir().unwrap();
        configure_file_cache(directory.path()).await;

        let primary = DeploymentUid::load_or_create(
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

        let replica = DeploymentUid::load_or_create(
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
    async fn replica_retries_until_primary_persists_uid() {
        let directory = tempfile::tempdir().unwrap();
        configure_file_cache(directory.path()).await;

        let path = directory.path().to_path_buf();
        let replica = tokio::spawn(async move {
            DeploymentUid::load_or_create(
                &path,
                InstanceRole::Replica,
                Duration::from_millis(1),
                Duration::from_millis(100),
            )
            .await
        });

        sleep(Duration::from_millis(5)).await;
        let primary = DeploymentUid::load_or_create(
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

        let error = DeploymentUid::load_or_create(
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

        let error = DeploymentUid::load_or_create(
            directory.path(),
            InstanceRole::Primary,
            Duration::from_millis(1),
            Duration::from_millis(10),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("canonical UUID"));
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
