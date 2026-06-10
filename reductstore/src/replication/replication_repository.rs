// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

mod read_only;
mod repo;

use crate::cfg::Cfg;
use crate::cfg::InstanceRole::Replica;
use crate::lifecycle::SystemEventSink;
use crate::replication::ManageReplications;
use crate::storage::engine::StorageEngine;
use read_only::ReadOnlyReplicationRepository;
use repo::ReplicationRepository;
use std::sync::Arc;

pub(crate) struct ReplicationRepoBuilder {
    cfg: Cfg,
    system_event_sink: Option<SystemEventSink>,
}

type BoxedReplicationRepository = Box<dyn ManageReplications + Send + Sync>;

impl ReplicationRepoBuilder {
    pub fn new(cfg: Cfg) -> Self {
        ReplicationRepoBuilder {
            cfg,
            system_event_sink: None,
        }
    }

    pub fn with_system_event_sink(mut self, system_event_sink: SystemEventSink) -> Self {
        self.system_event_sink = Some(system_event_sink);
        self
    }

    pub async fn build(self, storage: Arc<StorageEngine>) -> BoxedReplicationRepository {
        if self.cfg.role == Replica {
            Box::new(ReadOnlyReplicationRepository::new())
        } else {
            Box::new(
                ReplicationRepository::load_or_create(storage, self.cfg, self.system_event_sink)
                    .await,
            )
        }
    }
}
