// Copyright 2023-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

mod read_only;
mod repo;

use crate::cfg::Cfg;
use crate::cfg::InstanceRole::Replica;
use crate::replication::ManageReplications;
use crate::storage::engine::StorageEngine;
use read_only::ReadOnlyReplicationRepository;
use repo::ReplicationRepository;
use std::sync::Arc;

pub(crate) struct ReplicationRepoBuilder {
    cfg: Cfg,
}

type BoxedReplicationRepository = Box<dyn ManageReplications + Send + Sync>;

impl ReplicationRepoBuilder {
    pub fn new(cfg: Cfg) -> Self {
        ReplicationRepoBuilder { cfg }
    }

    pub async fn build(self, storage: Arc<StorageEngine>) -> BoxedReplicationRepository {
        if self.cfg.role == Replica {
            Box::new(ReadOnlyReplicationRepository::new())
        } else {
            Box::new(ReplicationRepository::load_or_create(storage, self.cfg).await)
        }
    }
}
