// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use async_trait::async_trait;
use reduct_base::error::ReductError;
use std::sync::Arc;
use tokio::sync::RwLock;

mod agent;
mod cfg;
mod engine;

/// Replication event to be synchronized.
#[derive(Debug)]
pub(crate) enum ReplicationEvent {
    /// Write a record to a bucket (timestamp)
    WriteRecord(u64),
}

pub(crate) type ReplicationAgent = Box<dyn NotifyReplicationEvent + Send + Sync>;

#[async_trait]
pub(crate) trait NotifyReplicationEvent {
    /// Filter and  notify the agent of events.
    ///
    /// # Arguments
    ///
    /// * `event` - The event to filter and notify
    async fn filter_and_notify(&self, event: ReplicationEvent) -> Result<(), ReductError>;
}

pub(crate) trait BuildReplicationAgent {
    /// Make a replication agent for a given bucket and entry name of event source.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket name
    /// * `entry` - The entry name
    ///
    /// # Returns
    ///
    /// A replication agent for event notification.
    fn build_agent_for(&mut self, bucket: &str, entry: &str) -> ReplicationAgent;
}

#[async_trait]
pub(crate) trait ReplicationEngine: BuildReplicationAgent {
    fn add_replication(&mut self, cfg: cfg::ReplicationCfg) -> Result<(), ReductError>;
}

pub(crate) fn create_replication_engine() -> impl ReplicationEngine {
    engine::ReplicationEngineImpl::new()
}
