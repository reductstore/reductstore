// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex as StdMutex, Weak};
use tokio::sync::{Mutex, OwnedMutexGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

/// Coordinates a single entry's mutations with remote publication.
///
/// The admission lock is intentionally held by the task that performs a
/// streamed write, not merely by the API wrapper that created it.
pub(crate) struct PublicationCoordinator {
    serial: Arc<Mutex<()>>,
    admission: Arc<RwLock<()>>,
}

static COORDINATORS: LazyLock<StdMutex<HashMap<PathBuf, Weak<PublicationCoordinator>>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

pub(crate) struct MutationGuard(#[allow(dead_code)] OwnedRwLockReadGuard<()>);

pub(crate) struct PublicationGuard {
    _serial: OwnedMutexGuard<()>,
    _admission: OwnedRwLockWriteGuard<()>,
}

impl PublicationCoordinator {
    pub(crate) fn for_path(path: PathBuf) -> Arc<Self> {
        let mut coordinators = COORDINATORS.lock().unwrap();
        if let Some(coordinator) = coordinators.get(&path).and_then(Weak::upgrade) {
            return coordinator;
        }

        let coordinator = Arc::new(Self {
            serial: Arc::new(Mutex::new(())),
            admission: Arc::new(RwLock::new(())),
        });
        coordinators.insert(path, Arc::downgrade(&coordinator));
        coordinator
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
