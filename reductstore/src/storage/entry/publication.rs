// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use std::sync::Arc;
use tokio::sync::{Mutex, OwnedMutexGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

/// Coordinates a single entry's mutations with remote publication.
///
/// An object backend exposes each entry file independently. Publishing a
/// changed `blocks.idx` before its referenced block files would therefore let
/// readers observe an invalid entry. The coordinator closes that interval:
/// mutations hold shared admission permits, while `Entry::sync_fs` obtains an
/// exclusive permit and serializes the odd-index, object, and even-index
/// publication sequence.
///
/// The admission permit is intentionally held by the task that performs a
/// streamed write, not merely by the API wrapper that created it. Bucket entry
/// construction is serialized, so each live entry owns exactly one coordinator
/// without a process-global path registry.
pub(crate) struct PublicationCoordinator {
    serial: Arc<Mutex<()>>,
    admission: Arc<RwLock<()>>,
}

pub(crate) struct MutationGuard(#[allow(dead_code)] OwnedRwLockReadGuard<()>);

pub(crate) struct PublicationGuard {
    _serial: OwnedMutexGuard<()>,
    _admission: OwnedRwLockWriteGuard<()>,
}

impl PublicationCoordinator {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            serial: Arc::new(Mutex::new(())),
            admission: Arc::new(RwLock::new(())),
        })
    }

    pub(crate) async fn admit(self: &Arc<Self>) -> MutationGuard {
        MutationGuard(self.admission.clone().read_owned().await)
    }

    pub(crate) async fn begin_publication(self: &Arc<Self>) -> PublicationGuard {
        let serial = self.serial.clone().lock_owned().await;
        let admission = self.admission.clone().write_owned().await;
        PublicationGuard {
            _serial: serial,
            _admission: admission,
        }
    }
}
