// Copyright 2023-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::replication::replication_task::ReplicationTask;
use crate::replication::{ManageReplications, TransactionNotification};
use reduct_base::error::ReductError;
use reduct_base::forbidden;
use reduct_base::msg::replication_api::{
    FullReplicationInfo, ReplicationInfo, ReplicationSettings,
};

pub(super) struct ReadOnlyReplicationRepository;

impl ReadOnlyReplicationRepository {
    pub fn new() -> Self {
        ReadOnlyReplicationRepository {}
    }
}

impl ManageReplications for ReadOnlyReplicationRepository {
    fn create_replication(
        &mut self,
        name: &str,
        settings: ReplicationSettings,
    ) -> Result<(), ReductError> {
        Err(forbidden!("Cannot create replication in read-only mode"))
    }

    fn update_replication(
        &mut self,
        name: &str,
        settings: ReplicationSettings,
    ) -> Result<(), ReductError> {
        Err(forbidden!("Cannot update replication in read-only mode"))
    }

    fn replications(&self) -> Vec<ReplicationInfo> {
        vec![]
    }

    fn get_info(&self, name: &str) -> Result<FullReplicationInfo, ReductError> {
        Err(forbidden!("Cannot get replication info in read-only mode"))
    }

    fn get_replication(&self, name: &str) -> Result<&ReplicationTask, ReductError> {
        Err(forbidden!("Cannot get replication in read-only mode"))
    }

    fn get_mut_replication(&mut self, name: &str) -> Result<&mut ReplicationTask, ReductError> {
        Err(forbidden!("Cannot get replication in read-only mode"))
    }

    fn remove_replication(&mut self, name: &str) -> Result<(), ReductError> {
        Err(forbidden!("Cannot remove replication in read-only mode"))
    }

    fn notify(&mut self, notification: TransactionNotification) -> Result<(), ReductError> {
        Err(forbidden!("Cannot notify replication in read-only mode"))
    }
    // Implement required methods with no-op behavior
}
