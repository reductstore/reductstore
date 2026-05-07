// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

mod read_only;
mod repo;

use crate::cfg::Cfg;
use crate::cfg::InstanceRole::Replica;
use crate::lifecycle::ManageLifecycles;
use crate::storage::engine::StorageEngine;
use read_only::ReadOnlyLifecycleRepository;
use repo::LifecycleRepository;
use std::sync::Arc;

pub(crate) struct LifecycleRepoBuilder {
    cfg: Cfg,
}

type BoxedLifecycleRepository = Box<dyn ManageLifecycles + Send + Sync>;

impl LifecycleRepoBuilder {
    pub fn new(cfg: Cfg) -> Self {
        LifecycleRepoBuilder { cfg }
    }

    pub async fn build(self, storage: Arc<StorageEngine>) -> BoxedLifecycleRepository {
        if self.cfg.role == Replica {
            Box::new(ReadOnlyLifecycleRepository::new())
        } else {
            Box::new(LifecycleRepository::load_or_create(storage, self.cfg).await)
        }
    }
}
