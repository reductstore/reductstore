// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use super::publication::{self, Publication};
use crate::core::file_cache::{BatchToken, FileBatch, FILE_CACHE};
use crate::core::sync::AsyncRwLock;
use crate::storage::block_manager::BlockManager;
use log::error;
use reduct_base::error::ReductError;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, OwnedRwLockReadGuard, RwLock};

const IDLE_PUBLICATION_DELAY: Duration = Duration::from_millis(50);

pub(super) struct PublicationCoordinator {
    admission: Arc<RwLock<()>>,
    batch: Mutex<Option<FileBatch>>,
    marker: Mutex<Option<Publication>>,
    publication_revision: AtomicU64,
    publisher_running: AtomicBool,
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
            publication_revision: AtomicU64::new(0),
            publisher_running: AtomicBool::new(false),
        }
    }

    pub(super) async fn begin_mutation(&self) -> Result<MutationAdmission, ReductError> {
        self.publication_revision.fetch_add(1, Ordering::AcqRel);
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
        self.publish_locked(path).await.map(|_| ())
    }

    /// Publishes a completed mutation burst after a short idle period.
    ///
    /// Every completed mutation advances the revision. The single background
    /// publisher waits until the revision stops changing, so adjacent records
    /// and HTTP batches share one publication while an idle entry becomes
    /// visible to replicas without waiting for the periodic compaction tick.
    pub(super) fn schedule_publish(
        self: &Arc<Self>,
        path: PathBuf,
        block_manager: Arc<AsyncRwLock<BlockManager>>,
    ) {
        self.publication_revision.fetch_add(1, Ordering::AcqRel);
        if self
            .publisher_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let coordinator = Arc::clone(self);
        tokio::spawn(async move {
            coordinator
                .run_scheduled_publisher(path, block_manager)
                .await;
        });
    }

    async fn run_scheduled_publisher(
        self: Arc<Self>,
        path: PathBuf,
        block_manager: Arc<AsyncRwLock<BlockManager>>,
    ) {
        loop {
            let observed_revision = self.publication_revision.load(Ordering::Acquire);
            tokio::time::sleep(IDLE_PUBLICATION_DELAY).await;
            if self.publication_revision.load(Ordering::Acquire) != observed_revision {
                continue;
            }

            if let Err(err) = self.publish_scheduled_batch(&path, &block_manager).await {
                error!("Failed to publish entry {}: {}", path.display(), err);
            }

            self.publisher_running.store(false, Ordering::Release);
            if self.publication_revision.load(Ordering::Acquire) == observed_revision {
                break;
            }

            if self
                .publisher_running
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                break;
            }
        }
    }

    async fn publish_scheduled_batch(
        &self,
        path: &Path,
        block_manager: &AsyncRwLock<BlockManager>,
    ) -> Result<(), ReductError> {
        let _guard = Arc::clone(&self.admission).write_owned().await;
        if let Some(token) = self.publish_locked(path).await? {
            block_manager.write().await?.clear_mutation_batch_if(&token);
        }
        Ok(())
    }

    async fn publish_locked(&self, path: &Path) -> Result<Option<BatchToken>, ReductError> {
        let mut stored_batch = self.batch.lock().await;
        let Some(batch) = stored_batch.as_mut() else {
            return Ok(None);
        };
        let token = batch.token();

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
        batch.commit().await?;
        Ok(Some(token))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::entry::publication::{load, PublicationState};
    use std::sync::Arc;

    fn entry_path() -> std::path::PathBuf {
        let path = tempfile::tempdir().unwrap().keep().join("entry");
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[tokio::test]
    async fn blocks_try_publish_during_mutation_then_publishes_marker() {
        let coordinator = Arc::new(PublicationCoordinator::new());
        let path = entry_path();
        let admission = coordinator.begin_mutation().await.unwrap();

        assert!(!coordinator.try_publish(&path).await.unwrap());
        let publisher = tokio::spawn({
            let coordinator = Arc::clone(&coordinator);
            let path = path.clone();
            async move { coordinator.publish(&path).await }
        });
        tokio::task::yield_now().await;
        assert_eq!(load(&path).await.unwrap(), None);
        drop(admission);

        publisher.await.unwrap().unwrap();
        let marker = load(&path).await.unwrap().unwrap();
        assert_eq!(marker.generation, 2);
        assert_eq!(marker.state, PublicationState::Ready);
        assert!(!marker.incarnation.is_empty());
    }

    #[tokio::test]
    async fn try_publish_without_mutations_is_a_noop() {
        let coordinator = PublicationCoordinator::new();
        let path = entry_path();

        assert!(coordinator.try_publish(&path).await.unwrap());
        assert_eq!(load(&path).await.unwrap(), None);
    }

    #[tokio::test]
    async fn finishes_an_existing_updating_publication() {
        let path = entry_path();
        let updating = publication::Publication {
            incarnation: "test-incarnation".to_owned(),
            generation: 1,
            state: PublicationState::Updating,
        };
        let mut batch = FILE_CACHE.begin_batch().await.unwrap();
        let marker_path = publication::write_local_in_batch(&batch.token(), &path, &updating)
            .await
            .unwrap();
        batch.sync_file(&marker_path).await.unwrap();
        batch.commit().await.unwrap();

        let coordinator = PublicationCoordinator::new();
        drop(coordinator.begin_mutation().await.unwrap());
        coordinator.publish(&path).await.unwrap();

        assert_eq!(
            load(&path).await.unwrap().unwrap(),
            publication::Publication {
                incarnation: "test-incarnation".to_owned(),
                generation: 2,
                state: PublicationState::Ready,
            }
        );
    }
}
