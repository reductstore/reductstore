// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::replication::engine::ReplicationNotification;
use crate::replication::{NotifyReplicationEvent, ReplicationEvent};
use crate::storage::proto::{ts_to_us, Record};
use async_trait::async_trait;
use reduct_base::error::ReductError;
use tokio::sync::mpsc::Sender;

pub(super) struct ReplicationAgentImpl {
    bucket: String,
    entry: String,
    tx: Sender<ReplicationNotification>,
}

#[async_trait]
impl NotifyReplicationEvent for ReplicationAgentImpl {
    async fn filter_and_notify(&self, event: ReplicationEvent) -> Result<(), ReductError> {
        self.tx
            .send(ReplicationNotification {
                bucket: self.bucket.clone(),
                entry: self.entry.clone(),
                event,
            })
            .await
            .map_err(|_| {
                ReductError::internal_server_error("Failed to send event to replication engine")
            })?;
        Ok(())
    }
}

impl ReplicationAgentImpl {
    pub(super) fn new(bucket: &str, entry: &str, tx: Sender<ReplicationNotification>) -> Self {
        Self {
            bucket: bucket.to_string(),
            entry: entry.to_string(),
            tx,
        }
    }
}
