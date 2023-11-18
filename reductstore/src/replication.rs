// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use async_trait::async_trait;
use reduct_base::error::ReductError;
use std::sync::Arc;
use tokio::sync::RwLock;

mod cfg;
mod engine;

/// Replication event to be synchronized.
#[derive(Debug)]
pub enum ReplicationEvent {
    /// Write a record to a bucket (timestamp)
    WriteRecord(u64),
}

#[derive(Debug)]
pub struct ReplicationNotification {
    pub bucket: String,
    pub entry: String,
    pub event: ReplicationEvent,
}

#[async_trait]
pub trait ReplicationEngine {
    fn add_replication(&mut self, cfg: cfg::ReplicationCfg) -> Result<(), ReductError>;
    async fn notify(&self, notification: ReplicationNotification) -> Result<(), ReductError>;
}

pub(crate) fn create_replication_engine() -> Box<dyn ReplicationEngine + Send + Sync> {
    Box::new(engine::ReplicationEngineImpl::new())
}
