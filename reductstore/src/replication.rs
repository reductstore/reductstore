// Copyright 2023-2024 ReductStore
// Licensed under the Business Source License 1.1

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use reduct_base::error::ReductError;
use reduct_base::msg::replication_api::{
    FullReplicationInfo, ReplicationInfo, ReplicationSettings,
};

use crate::replication::replication_task::ReplicationTask;
use crate::storage::proto::record::Label;
use crate::storage::storage::Storage;

mod diagnostics;
pub mod proto;
mod remote_bucket;
mod replication_repository;
mod replication_sender;
mod replication_task;
mod transaction_filter;
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
pub struct TransactionNotification {
    pub bucket: String,
    pub entry: String,
    pub labels: Vec<Label>,
    pub event: Transaction,
}

#[async_trait]
pub trait ManageReplications {
    /// Create a new replication.
    ///
    /// # Arguments
    /// * `name` - Replication name.
    /// * `settings` - Replication settings.
    ///
    /// # Errors
    ///
    /// * `ReductError::Conflict` - Replication already exists.
    /// * `ReductError::BadRequest` - Invalid destination host.
    /// * `ReductError::NotFound` - Source bucket does not exist.
    async fn create_replication(
        &mut self,
        name: &str,
        settings: ReplicationSettings,
    ) -> Result<(), ReductError>;

    async fn update_replication(
        &mut self,
        name: &str,
        settings: ReplicationSettings,
    ) -> Result<(), ReductError>;

    async fn replications(&self) -> Vec<ReplicationInfo>;

    async fn get_info(&self, name: &str) -> Result<FullReplicationInfo, ReductError>;

    fn get_replication(&self, name: &str) -> Result<&ReplicationTask, ReductError>;

    fn get_mut_replication(&mut self, name: &str) -> Result<&mut ReplicationTask, ReductError>;

    fn remove_replication(&mut self, name: &str) -> Result<(), ReductError>;

    async fn notify(&mut self, notification: TransactionNotification) -> Result<(), ReductError>;
}

pub(crate) async fn create_replication_engine(
    storage: Arc<RwLock<Storage>>,
) -> Box<dyn ManageReplications + Send + Sync> {
    Box::new(replication_repository::ReplicationRepository::load_or_create(storage).await)
}
