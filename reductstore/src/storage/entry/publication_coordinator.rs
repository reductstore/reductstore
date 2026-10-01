// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use super::publication::{self, Publication};
use crate::core::file_cache::{BatchToken, FileBatch, FILE_CACHE};
use reduct_base::error::ReductError;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{Mutex, OwnedRwLockReadGuard, RwLock};

pub(super) struct PublicationCoordinator {
    admission: Arc<RwLock<()>>,
    batch: Mutex<Option<FileBatch>>,
    marker: Mutex<Option<Publication>>,
}

pub(super) struct MutationAdmission {
    pub(super) token: BatchToken,
    _guard: OwnedRwLockReadGuard<()>,
}

impl PublicationCoordinator {
    pub(super) fn new() -> Self {
        Self {
            admission: Arc::new(RwLock::new(())),
            batch: Mutex::new(None),
            marker: Mutex::new(None),
        }
    }

    pub(super) async fn begin_mutation(&self) -> Result<MutationAdmission, ReductError> {
        let guard = Arc::clone(&self.admission).read_owned().await;
        let mut batch = self.batch.lock().await;
        if batch.is_none() {
            *batch = Some(FILE_CACHE.begin_batch().await?);
        }
        Ok(MutationAdmission {
            token: batch.as_ref().expect("batch was initialized").token(),
            _guard: guard,
        })
    }

    pub(super) async fn try_publish(&self, path: &Path) -> Result<bool, ReductError> {
        let Some(_guard) = Arc::clone(&self.admission).try_write_owned().ok() else {
            return Ok(false);
        };
        self.publish_locked(path).await?;
        Ok(true)
    }

    pub(super) async fn publish(&self, path: &Path) -> Result<(), ReductError> {
        let _guard = Arc::clone(&self.admission).write_owned().await;
        self.publish_locked(path).await
    }

    async fn publish_locked(&self, path: &Path) -> Result<(), ReductError> {
        let mut stored_batch = self.batch.lock().await;
        let Some(batch) = stored_batch.as_mut() else {
            return Ok(());
        };

        let current = {
            let mut marker = self.marker.lock().await;
            if marker.is_none() {
                *marker = publication::load(path).await?;
            }
            marker.clone().unwrap_or_else(Publication::new)
        };

        let updating = if current.state == publication::PublicationState::Updating {
            current
        } else {
            current.updating()?
        };
        let marker_path =
            publication::write_local_in_batch(&batch.token(), path, &updating).await?;
        batch.sync_file(&marker_path).await?;
        *self.marker.lock().await = Some(updating.clone());

        batch.sync_all().await?;

        let ready = updating.ready()?;
        let marker_path = publication::write_local_in_batch(&batch.token(), path, &ready).await?;
        batch.sync_file(&marker_path).await?;
        *self.marker.lock().await = Some(ready);

        let batch = stored_batch.take().expect("active batch disappeared");
        batch.commit().await
    }
}
