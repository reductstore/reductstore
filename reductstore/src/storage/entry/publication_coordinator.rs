// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use super::publication::{self, Publication};
use crate::core::file_cache::{BatchToken, FileBatch, FileCache};
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
    batch: Mutex<Option<PendingPublication>>,
    publisher: Mutex<()>,
    marker: Mutex<Option<Publication>>,
    publication_revision: AtomicU64,
    publisher_running: AtomicBool,
    file_cache: Arc<FileCache>,
    #[cfg(test)]
    sync_pause: Mutex<Option<Arc<SyncPause>>>,
}

struct PendingPublication {
    token: BatchToken,
    // Temporarily taken by the publisher so mutations can keep using the token
    // without waiting for backend synchronization.
    file_batch: Option<FileBatch>,
}

#[cfg(test)]
struct SyncPause {
    started: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

pub(super) struct MutationAdmission {
    pub(super) token: BatchToken,
    _guard: OwnedRwLockReadGuard<()>,
}

impl PublicationCoordinator {
    pub(super) fn new_with_cache(file_cache: Arc<FileCache>) -> Self {
        Self {
            admission: Arc::new(RwLock::new(())),
            batch: Mutex::new(None),
            publisher: Mutex::new(()),
            marker: Mutex::new(None),
            publication_revision: AtomicU64::new(0),
            publisher_running: AtomicBool::new(false),
            file_cache,
            #[cfg(test)]
            sync_pause: Mutex::new(None),
        }
    }

    pub(super) async fn begin_mutation(
        &self,
        path: &Path,
    ) -> Result<MutationAdmission, ReductError> {
        self.publication_revision.fetch_add(1, Ordering::AcqRel);
        let guard = Arc::clone(&self.admission).read_owned().await;
        let mut batch = self.batch.lock().await;
        if batch.is_none() {
            let file_batch = self.file_cache.begin_batch_for_scope(path).await?;
            *batch = Some(PendingPublication {
                token: file_batch.token(),
                file_batch: Some(file_batch),
            });
        }
        Ok(MutationAdmission {
            token: batch.as_ref().expect("batch was initialized").token.clone(),
            _guard: guard,
        })
    }

    pub(super) async fn try_publish(&self, path: &Path) -> Result<bool, ReductError> {
        let Ok(_publisher) = self.publisher.try_lock() else {
            return Ok(false);
        };
        let Some(_guard) = Arc::clone(&self.admission).try_write_owned().ok() else {
            return Ok(false);
        };
        self.publish_exclusive(path).await?;
        Ok(true)
    }

    pub(super) async fn publish(&self, path: &Path) -> Result<(), ReductError> {
        let _publisher = self.publisher.lock().await;
        let _guard = Arc::clone(&self.admission).write_owned().await;
        self.publish_exclusive(path).await.map(|_| ())
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
        if let Some(token) = self.publish_scheduled(path).await? {
            block_manager.write().await?.clear_mutation_batch_if(&token);
        }
        Ok(())
    }

    async fn publish_scheduled(&self, path: &Path) -> Result<Option<BatchToken>, ReductError> {
        let _publisher = self.publisher.lock().await;
        let mut guard = Arc::clone(&self.admission).write_owned().await;
        let Some(token) = self.begin_publication(path).await? else {
            return Ok(None);
        };

        loop {
            let observed_revision = self.publication_revision.load(Ordering::Acquire);
            drop(guard);

            // Mutations may join this batch while its current dirty-file snapshot is synced.
            // A revision change below forces another pass before Ready is published.
            self.sync_pending_batch().await?;

            guard = Arc::clone(&self.admission).write_owned().await;
            if self.publication_revision.load(Ordering::Acquire) != observed_revision {
                continue;
            }

            self.finish_publication(path).await?;
            return Ok(Some(token));
        }
    }

    async fn publish_exclusive(&self, path: &Path) -> Result<Option<BatchToken>, ReductError> {
        let Some(token) = self.begin_publication(path).await? else {
            return Ok(None);
        };
        self.sync_pending_batch().await?;
        self.finish_publication(path).await?;
        Ok(Some(token))
    }

    async fn begin_publication(&self, path: &Path) -> Result<Option<BatchToken>, ReductError> {
        let Some(token) = self
            .batch
            .lock()
            .await
            .as_ref()
            .map(|batch| batch.token.clone())
        else {
            return Ok(None);
        };

        let current = {
            let mut marker = self.marker.lock().await;
            if marker.is_none() {
                *marker = publication::load(path, &self.file_cache).await?;
            }
            marker.clone().unwrap_or_else(Publication::new)
        };

        let updating = if current.state == publication::PublicationState::Updating {
            current
        } else {
            current.updating()?
        };
        let marker_path =
            publication::write_local_in_batch(&token, path, &updating, &self.file_cache).await?;
        self.sync_pending_file(&marker_path).await?;
        *self.marker.lock().await = Some(updating.clone());
        Ok(Some(token))
    }

    async fn finish_publication(&self, path: &Path) -> Result<(), ReductError> {
        let token = self
            .batch
            .lock()
            .await
            .as_ref()
            .expect("pending publication disappeared")
            .token
            .clone();
        let updating = self
            .marker
            .lock()
            .await
            .clone()
            .expect("updating publication marker disappeared");
        let ready = updating.ready()?;
        let marker_path =
            publication::write_local_in_batch(&token, path, &ready, &self.file_cache).await?;
        self.sync_pending_file(&marker_path).await?;
        *self.marker.lock().await = Some(ready);

        let mut pending = self
            .batch
            .lock()
            .await
            .take()
            .expect("pending publication disappeared");
        pending
            .file_batch
            .take()
            .expect("file batch is already being synchronized")
            .commit()
            .await
    }

    async fn sync_pending_file(&self, path: &Path) -> Result<(), ReductError> {
        let mut file_batch = self.take_file_batch().await?;
        let result = file_batch.sync_file(path).await;
        self.restore_file_batch(file_batch).await?;
        result
    }

    async fn sync_pending_batch(&self) -> Result<(), ReductError> {
        #[cfg(test)]
        if let Some(pause) = self.sync_pause.lock().await.take() {
            pause.started.notify_one();
            pause.release.notified().await;
        }
        let mut file_batch = self.take_file_batch().await?;
        let result = file_batch.sync_all().await;
        self.restore_file_batch(file_batch).await?;
        result
    }

    async fn take_file_batch(&self) -> Result<FileBatch, ReductError> {
        self.batch
            .lock()
            .await
            .as_mut()
            .and_then(|pending| pending.file_batch.take())
            .ok_or_else(|| {
                reduct_base::internal_server_error!("Pending publication file batch is unavailable")
            })
    }

    async fn restore_file_batch(&self, file_batch: FileBatch) -> Result<(), ReductError> {
        let mut batch = self.batch.lock().await;
        let pending = batch.as_mut().ok_or_else(|| {
            reduct_base::internal_server_error!("Pending publication disappeared during sync")
        })?;
        if pending.file_batch.is_some() {
            return Err(reduct_base::internal_server_error!(
                "Pending publication file batch was replaced during sync"
            ));
        }
        pending.file_batch = Some(file_batch);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::file_cache::build_test_file_cache;
    use crate::storage::entry::publication::{load, PublicationState};
    use std::io::{SeekFrom, Write};
    use std::sync::Arc;

    fn entry_path() -> std::path::PathBuf {
        let path = tempfile::tempdir().unwrap().keep().join("entry");
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[tokio::test]
    async fn blocks_try_publish_during_mutation_then_publishes_marker() {
        let file_cache = build_test_file_cache();
        let coordinator = Arc::new(PublicationCoordinator::new_with_cache(file_cache.clone()));
        let path = entry_path();
        let admission = coordinator.begin_mutation(&path).await.unwrap();

        assert!(!coordinator.try_publish(&path).await.unwrap());
        let publisher = tokio::spawn({
            let coordinator = Arc::clone(&coordinator);
            let path = path.clone();
            async move { coordinator.publish(&path).await }
        });
        tokio::task::yield_now().await;
        assert_eq!(load(&path, &file_cache).await.unwrap(), None);
        drop(admission);

        publisher.await.unwrap().unwrap();
        let marker = load(&path, &file_cache).await.unwrap().unwrap();
        assert_eq!(marker.generation, 2);
        assert_eq!(marker.state, PublicationState::Ready);
        assert!(!marker.incarnation.is_empty());
    }

    #[tokio::test]
    async fn try_publish_without_mutations_is_a_noop() {
        let file_cache = build_test_file_cache();
        let coordinator = PublicationCoordinator::new_with_cache(file_cache.clone());
        let path = entry_path();

        assert!(coordinator.try_publish(&path).await.unwrap());
        assert_eq!(load(&path, &file_cache).await.unwrap(), None);
    }

    #[tokio::test]
    async fn finishes_an_existing_updating_publication() {
        let path = entry_path();
        let updating = publication::Publication {
            incarnation: "test-incarnation".to_owned(),
            generation: 1,
            state: PublicationState::Updating,
        };
        let file_cache = build_test_file_cache();
        let mut batch = file_cache.begin_batch().await.unwrap();
        let marker_path =
            publication::write_local_in_batch(&batch.token(), &path, &updating, &file_cache)
                .await
                .unwrap();
        batch.sync_file(&marker_path).await.unwrap();
        batch.commit().await.unwrap();

        let coordinator = PublicationCoordinator::new_with_cache(file_cache.clone());
        drop(coordinator.begin_mutation(&path).await.unwrap());
        coordinator.publish(&path).await.unwrap();

        assert_eq!(
            load(&path, &file_cache).await.unwrap().unwrap(),
            publication::Publication {
                incarnation: "test-incarnation".to_owned(),
                generation: 2,
                state: PublicationState::Ready,
            }
        );
    }

    #[tokio::test]
    async fn scheduled_publication_admits_mutations_while_syncing() {
        let path = entry_path();
        let first_path = path.join("first.blk");
        let second_path = path.join("second.blk");
        let file_cache = build_test_file_cache();
        let coordinator = Arc::new(PublicationCoordinator::new_with_cache(Arc::clone(
            &file_cache,
        )));
        let pause = Arc::new(SyncPause {
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        *coordinator.sync_pause.lock().await = Some(Arc::clone(&pause));

        let admission = coordinator.begin_mutation(&path).await.unwrap();
        let first_token = admission.token.clone();
        let mut first = file_cache
            .write_or_create_in_batch(&first_token, &first_path, SeekFrom::Start(0))
            .await
            .unwrap();
        first.write_all(b"first").unwrap();
        drop(first);
        drop(admission);

        let publisher = tokio::spawn({
            let coordinator = Arc::clone(&coordinator);
            let path = path.clone();
            async move { coordinator.publish_scheduled(&path).await }
        });
        tokio::time::timeout(Duration::from_secs(2), pause.started.notified())
            .await
            .expect("publication did not start synchronizing");

        let admission = tokio::time::timeout(
            Duration::from_millis(100),
            coordinator.begin_mutation(&path),
        )
        .await
        .expect("mutation was blocked by publication sync")
        .unwrap();
        assert_eq!(admission.token, first_token);
        let mut second = file_cache
            .write_or_create_in_batch(&admission.token, &second_path, SeekFrom::Start(0))
            .await
            .unwrap();
        second.write_all(b"second").unwrap();
        drop(second);
        drop(admission);

        pause.release.notify_one();
        assert_eq!(publisher.await.unwrap().unwrap(), Some(first_token));
        let marker = load(&path, &file_cache).await.unwrap().unwrap();
        assert_eq!(marker.state, PublicationState::Ready);
        assert_eq!(marker.generation, 2);
    }
}
