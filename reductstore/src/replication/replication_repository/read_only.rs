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
        _name: &str,
        _settings: ReplicationSettings,
    ) -> Result<(), ReductError> {
        Err(forbidden!("Cannot create replication in read-only mode"))
    }

    fn update_replication(
        &mut self,
        _name: &str,
        _settings: ReplicationSettings,
    ) -> Result<(), ReductError> {
        Err(forbidden!("Cannot update replication in read-only mode"))
    }

    fn replications(&self) -> Vec<ReplicationInfo> {
        vec![]
    }

    fn get_info(&self, _name: &str) -> Result<FullReplicationInfo, ReductError> {
        Err(forbidden!("Cannot get replication info in read-only mode"))
    }

    fn get_replication(&self, _name: &str) -> Result<&ReplicationTask, ReductError> {
        Err(forbidden!("Cannot get replication in read-only mode"))
    }

    fn get_mut_replication(&mut self, _name: &str) -> Result<&mut ReplicationTask, ReductError> {
        Err(forbidden!("Cannot get replication in read-only mode"))
    }

    fn remove_replication(&mut self, _name: &str) -> Result<(), ReductError> {
        Err(forbidden!("Cannot remove replication in read-only mode"))
    }

    fn notify(&mut self, _notification: TransactionNotification) -> Result<(), ReductError> {
        Err(forbidden!("Cannot notify replication in read-only mode"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::TransactionNotification;
    use rstest::{fixture, rstest};

    #[fixture]
    fn repo() -> ReadOnlyReplicationRepository {
        ReadOnlyReplicationRepository::new()
    }

    mod create {
        use super::*;
        #[rstest]
        fn test_create_replication_forbidden(mut repo: ReadOnlyReplicationRepository) {
            let settings = ReplicationSettings::default();
            let result = repo.create_replication("test", settings);
            assert_eq!(
                result.err().unwrap(),
                forbidden!("Cannot create replication in read-only mode")
            );
        }
    }

    mod update {
        use super::*;
        #[rstest]
        fn test_update_replication_forbidden(mut repo: ReadOnlyReplicationRepository) {
            let settings = ReplicationSettings::default();
            let result = repo.update_replication("test", settings);
            assert_eq!(
                result.err().unwrap(),
                forbidden!("Cannot update replication in read-only mode")
            );
        }
    }

    mod replications {
        use super::*;
        #[rstest]
        fn test_replications_empty(repo: ReadOnlyReplicationRepository) {
            let reps = repo.replications();
            assert!(reps.is_empty());
        }
    }

    mod get_info {
        use super::*;
        #[rstest]
        fn test_get_info_forbidden(repo: ReadOnlyReplicationRepository) {
            let result = repo.get_info("test");
            assert_eq!(
                result.err().unwrap(),
                forbidden!("Cannot get replication info in read-only mode")
            );
        }
    }

    mod get_replication {
        use super::*;
        #[rstest]
        fn test_get_replication_forbidden(repo: ReadOnlyReplicationRepository) {
            let err = repo.get_replication("test").err().unwrap();
            assert_eq!(err, forbidden!("Cannot get replication in read-only mode"));
        }
    }

    mod get_mut_replication {
        use super::*;
        #[rstest]
        fn test_get_mut_replication_forbidden(mut repo: ReadOnlyReplicationRepository) {
            let err = repo.get_mut_replication("test").err().unwrap();
            assert_eq!(err, forbidden!("Cannot get replication in read-only mode"));
        }
    }

    mod notify {
        use super::*;
        use reduct_base::io::RecordMeta;
        #[rstest]
        fn test_notify_forbidden(mut repo: ReadOnlyReplicationRepository) {
            let notification = TransactionNotification {
                bucket: "bucket".to_string(),
                entry: "entry".to_string(),
                meta: RecordMeta::builder().timestamp(0).build(),
                event: crate::replication::Transaction::WriteRecord(0),
            };
            let err = repo.notify(notification).err().unwrap();
            assert_eq!(
                err,
                forbidden!("Cannot notify replication in read-only mode")
            );
        }
    }
}
