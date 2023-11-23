// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::replication::replication::Replication;
use async_trait::async_trait;
use reduct_base::error::ReductError;
use reduct_base::Labels;
use std::sync::Arc;
use tokio::sync::RwLock;

mod engine;
mod replication;
mod transaction_log;

/// Replication event to be synchronized.
#[derive(Debug, Clone, PartialEq)]
pub enum Transaction {
    /// Write a record to a bucket (timestamp)
    WriteRecord(u64),
}

impl Into<u8> for Transaction {
    fn into(self) -> u8 {
        match self {
            Transaction::WriteRecord(_) => 0,
        }
    }
}

impl Transaction {
    pub fn timestamp(&self) -> &u64 {
        match self {
            Transaction::WriteRecord(ts) => ts,
        }
    }
}

impl TryFrom<u8> for Transaction {
    type Error = ReductError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Transaction::WriteRecord(0)),
            _ => Err(ReductError::internal_server_error(
                "Invalid transaction type",
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReplicationNotification {
    pub bucket: String,
    pub entry: String,
    pub labels: Labels,
    pub event: Transaction,
}

#[async_trait]
pub trait ReplicationEngine {
    fn add_replication(&mut self, replication: Replication) -> Result<(), ReductError>;
    async fn notify(&self, notification: ReplicationNotification) -> Result<(), ReductError>;
}

pub(crate) fn create_replication_engine() -> Box<dyn ReplicationEngine + Send + Sync> {
    Box::new(engine::ReplicationEngineImpl::new())
}
