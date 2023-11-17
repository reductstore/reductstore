// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::replication::agent::ReplicationAgentImpl;
use crate::replication::cfg::ReplicationCfg;
use crate::replication::{
    BuildReplicationAgent, ReplicationAgent, ReplicationEngine, ReplicationEvent,
};
use log::info;
use reduct_base::error::ReductError;
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;

#[derive(Debug)]
pub(super) struct ReplicationNotification {
    pub bucket: String,
    pub entry: String,
    pub event: ReplicationEvent,
}

pub(crate) struct ReplicationEngineImpl {
    replications: HashMap<String, ReplicationCfg>,
    tx: Sender<ReplicationNotification>,
}

impl BuildReplicationAgent for ReplicationEngineImpl {
    fn build_agent_for(&mut self, bucket: &str, entry: &str) -> ReplicationAgent {
        Box::new(ReplicationAgentImpl::new(bucket, entry, self.tx.clone()))
    }
}

impl ReplicationEngine for ReplicationEngineImpl {
    fn add_replication(&mut self, cfg: ReplicationCfg) -> Result<(), ReductError> {
        self.replications.insert(cfg.name.clone(), cfg);
        Ok(())
    }
}

impl ReplicationEngineImpl {
    pub(crate) fn new() -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        tokio::spawn(async move {
            while let Some(notification) = rx.recv().await {
                info!("Replication notification: {:?}", notification);
            }
        });
        Self {
            replications: HashMap::new(),
            tx,
        }
    }
}
