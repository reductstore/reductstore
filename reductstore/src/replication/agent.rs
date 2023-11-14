// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::replication::{NotifyReplicationEvent, ReplicationEvent};
use crate::storage::proto::{ts_to_us, Record};
use async_trait::async_trait;
use reduct_base::error::ReductError;
use tokio::sync::mpsc::Sender;

struct ReplicationAgent {
    tx: Sender<ReplicationEvent>,
}

#[async_trait]
impl NotifyReplicationEvent for ReplicationAgent {
    async fn filter_and_notify(&self, event: ReplicationEvent) -> Result<(), ReductError> {
        // todo: filter
        self.tx.send(event).await.map_err(|_| {
            ReductError::internal_server_error("Failed to send event to replication engine")
        })?;
        Ok(())
    }
}

struct NoReplicationAgent {}

#[async_trait]
impl NotifyReplicationEvent for NoReplicationAgent {
    async fn filter_and_notify(&self, event: ReplicationEvent) -> Result<(), ReductError> {
        Ok(())
    }
}
