// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use async_trait::async_trait;
use reduct_base::error::ReductError;

mod agent;
mod engine;

/// Replication event to be synchronized.
pub(crate) enum ReplicationEvent {
    WriteRecord(u64),
}

#[async_trait]
pub(crate) trait NotifyReplicationEvent {
    /// Filter and  notify the agent of events.
    ///
    /// # Arguments
    ///
    /// * `event` - The event to filter and notify
    async fn filter_and_notify(&self, event: ReplicationEvent) -> Result<(), ReductError>;
}

pub(crate) trait MakeReplicationAgent {
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
    fn make_replication_agent_for(
        &self,
        bucket: &str,
        entry: &str,
    ) -> Box<dyn NotifyReplicationEvent>;
}
