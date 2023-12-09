// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use async_trait::async_trait;
use reduct_base::error::ReductError;
use reduct_base::Labels;
use std::collections::HashMap;

mod engine;
mod remote_bucket;
mod replication;
mod transaction_filter;
mod transaction_log;

pub use replication::Replication;
pub use replication::ReplicationSettings;

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
pub struct TransactionNotification {
    pub bucket: String,
    pub entry: String,
    pub labels: Labels,
    pub event: Transaction,
}

#[async_trait]
pub trait ReplicationEngine {
    fn add_replication(&mut self, replication: Replication) -> Result<(), ReductError>;

    fn replications(&self) -> &HashMap<String, Replication>;

    async fn notify(&self, notification: TransactionNotification) -> Result<(), ReductError>;
}

pub(crate) fn create_replication_engine() -> Box<dyn ReplicationEngine + Send + Sync> {
    Box::new(engine::ReplicationEngineImpl::new())
}