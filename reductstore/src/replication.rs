// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::replication::replication_task::ReplicationTask;
use async_trait::async_trait;
use reduct_base::error::ReductError;
use reduct_base::io::RecordMeta;
use reduct_base::msg::replication_api::{
    FullReplicationInfo, ReplicationInfo, ReplicationMode, ReplicationSettings,
};

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

    /// Update a record in a bucket (timestamp)
    UpdateRecord(u64),
}

impl Into<u8> for Transaction {
    fn into(self) -> u8 {
        match self {
            Transaction::WriteRecord(_) => 0,
            Transaction::UpdateRecord(_) => 1,
        }
    }
}

impl Transaction {
    pub fn timestamp(&self) -> &u64 {
        match self {
            Transaction::WriteRecord(ts) => ts,
            Transaction::UpdateRecord(ts) => ts,
        }
    }

    pub fn into_timestamp(self) -> u64 {
        match self {
            Transaction::WriteRecord(ts) => ts,
            Transaction::UpdateRecord(ts) => ts,
        }
    }
}

impl TryFrom<u8> for Transaction {
    type Error = ReductError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Transaction::WriteRecord(0)),
            1 => Ok(Transaction::UpdateRecord(0)),
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
    pub meta: RecordMeta,
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

    /// Update an existing replication.
    ///
    /// # Arguments
    ///
    /// * `name` - Replication name.
    /// * `settings` - Replication settings.
    ///
    /// # Errors
    ///
    /// A `ReductError` is returned if the update fails.
    async fn update_replication(
        &mut self,
        name: &str,
        settings: ReplicationSettings,
    ) -> Result<(), ReductError>;

    /// List all replications.
    async fn replications(&self) -> Result<Vec<ReplicationInfo>, ReductError>;

    /// Get replication information.
    async fn get_info(&self, name: &str) -> Result<FullReplicationInfo, ReductError>;

    /// Get replication task.
    fn get_replication(&self, name: &str) -> Result<&ReplicationTask, ReductError>;

    /// Get mutable replication task.
    fn get_mut_replication(&mut self, name: &str) -> Result<&mut ReplicationTask, ReductError>;

    /// Remove a replication task
    async fn remove_replication(&mut self, name: &str) -> Result<(), ReductError>;

    /// Update replication mode
    async fn set_mode(&mut self, name: &str, mode: ReplicationMode) -> Result<(), ReductError>;

    /// Notify replication task about a new transaction.
    ///
    /// # Arguments
    ///
    /// * `notification` - Transaction notification.
    ///
    /// # Errors
    ///
    /// A `ReductError` is returned if the notification fails.
    async fn notify(&mut self, notification: TransactionNotification) -> Result<(), ReductError>;

    /// Start background workers if they are not running yet.
    fn start(&mut self);

    /// Stop background workers and wait until they finish.
    async fn stop(&mut self);
}

pub(crate) use replication_repository::ReplicationRepoBuilder;
