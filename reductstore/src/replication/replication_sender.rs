// Copyright 2023-2024 ReductStore
// Licensed under the Business Source License 1.1

use crate::replication::remote_bucket::RemoteBucket;

use crate::replication::transaction_log::TransactionLog;

use crate::storage::storage::Storage;
use std::cmp::PartialEq;

use log::{debug, error};
use reduct_base::error::{ErrorCode, ReductError};

use crate::replication::diagnostics::DiagnosticsCounter;

use reduct_base::msg::replication_api::ReplicationSettings;
use std::collections::HashMap;

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use tokio::time::sleep;

/// Internal worker for replication to process a sole iteration of the replication loop.
pub(super) struct ReplicationSender {
    replication_name: String,
    log_map: Arc<RwLock<HashMap<String, RwLock<TransactionLog>>>>,
    storage: Arc<RwLock<Storage>>,
    config: ReplicationSettings,
    hourly_diagnostics: Arc<RwLock<DiagnosticsCounter>>,
    bucket: Arc<RwLock<dyn RemoteBucket + Send + Sync>>,
}

#[derive(Debug, PartialEq)]
pub(super) enum SyncState {
    SyncedOrRemoved,
    NotAvailable,
    NoTransactions,
    BrokenLog(String),
}

impl ReplicationSender {
    pub fn new(
        replication_name: String,
        log_map: Arc<RwLock<HashMap<String, RwLock<TransactionLog>>>>,
        storage: Arc<RwLock<Storage>>,
        config: ReplicationSettings,
        hourly_diagnostics: Arc<RwLock<DiagnosticsCounter>>,
        bucket: Arc<RwLock<dyn RemoteBucket + Send + Sync>>,
    ) -> Self {
        Self {
            replication_name,
            log_map,
            storage,
            config,
            hourly_diagnostics,
            bucket,
        }
    }

    pub async fn run(&self) -> SyncState {
        let mut sync_something = false;
        for (entry_name, log) in self.log_map.read().await.iter() {
            let tr = log.write().await.front().await;
            let state = match tr {
                Ok(None) => SyncState::NoTransactions,
                Ok(Some(transaction)) => {
                    sync_something = true;
                    debug!("Replicating transaction {}/{:?}", entry_name, transaction);

                    let read_record_from_storage = async {
                        let mut atempts = 3;
                        loop {
                            let read_record = async {
                                self.storage
                                    .read()
                                    .await
                                    .get_bucket(&self.config.src_bucket)?
                                    .begin_read(&entry_name, *transaction.timestamp())
                                    .await
                            };
                            let record = read_record.await;
                            match record {
                                Err(ReductError {
                                    status: ErrorCode::TooEarly,
                                    ..
                                }) => {
                                    debug!("Transaction is too early, retrying later");
                                    sleep(Duration::from_millis(10)).await;
                                    atempts -= 1;
                                }

                                _ => {
                                    atempts = 0;
                                }
                            }

                            if atempts == 0 {
                                break record;
                            }
                        }
                    };

                    let record_to_sync = match read_record_from_storage.await {
                        Ok(record) => record,
                        Err(err) => {
                            log.write().await.pop_front().await.unwrap();
                            error!("Failed to read record: {}", err);
                            self.hourly_diagnostics.write().await.count(Err(err));
                            break;
                        }
                    };

                    let mut bucket = self.bucket.write().await;
                    match bucket.write_record(entry_name, record_to_sync).await {
                        Ok(_) => {
                            if bucket.is_active() {
                                self.hourly_diagnostics.write().await.count(Ok(()));
                            }
                        }
                        Err(err) => {
                            debug!("Failed to replicate transaction: {:?}", err);
                            self.hourly_diagnostics.write().await.count(Err(err));
                        }
                    }

                    if bucket.is_active() {
                        log.write().await.pop_front().await.unwrap();
                        SyncState::SyncedOrRemoved
                    } else {
                        SyncState::NotAvailable
                    }
                }

                Err(err) => {
                    error!("Failed to read transaction: {:?}", err);
                    return SyncState::BrokenLog(entry_name.clone());
                }
            };

            if state == SyncState::NotAvailable {
                // if the bucket is not active, we don't want to spin the CPU
                return state;
            }
        }

        if sync_something {
            SyncState::SyncedOrRemoved
        } else {
            SyncState::NoTransactions
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::Transaction;
    use crate::storage::bucket::RecordReader;
    use async_trait::async_trait;
    use bytes::Bytes;

    use mockall::mock;
    use reduct_base::error::ErrorCode;
    use reduct_base::msg::bucket_api::BucketSettings;
    use reduct_base::msg::diagnostics::{DiagnosticsError, DiagnosticsItem};
    use reduct_base::Labels;
    use rstest::*;

    mock! {
        RmBucket {}

        #[async_trait]
        impl RemoteBucket for RmBucket {
            async fn write_record(
                &mut self,
                entry_name: &str,
                record: RecordReader,
            ) -> Result<(), ReductError>;

            fn is_active(&self) -> bool;
        }

    }

    #[rstest]
    #[tokio::test]
    async fn test_replication_ok_not_active(
        mut remote_bucket: MockRmBucket,
        settings: ReplicationSettings,
    ) {
        remote_bucket.expect_write_record().returning(|_, _| Ok(()));
        remote_bucket.expect_is_active().return_const(false);
        let sender = build_sender(remote_bucket, settings).await;

        let transaction = Transaction::WriteRecord(10);
        imitate_write_record(&sender, &transaction).await;

        assert_eq!(sender.run().await, SyncState::NotAvailable);
        assert_eq!(
            sender
                .log_map
                .read()
                .await
                .get("test")
                .unwrap()
                .read()
                .await
                .front()
                .await,
            Ok(Some(transaction)),
        );

        assert_eq!(
            sender.hourly_diagnostics.read().await.diagnostics(),
            DiagnosticsItem {
                ok: 0,
                errored: 0,
                errors: HashMap::new(),
            },
            "should not count errors for non active replication"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_replication_comm_err(mut remote_bucket: MockRmBucket) {
        remote_bucket
            .expect_write_record()
            .returning(|_, _| Err(ReductError::new(ErrorCode::Timeout, "Timeout")));
        remote_bucket.expect_is_active().return_const(false);
        let sender = build_sender(remote_bucket, settings()).await;

        let transaction = Transaction::WriteRecord(10);
        imitate_write_record(&sender, &transaction).await;

        assert_eq!(sender.run().await, SyncState::NotAvailable);

        assert_eq!(
            sender
                .log_map
                .read()
                .await
                .get("test")
                .unwrap()
                .read()
                .await
                .front()
                .await,
            Ok(Some(transaction)),
        );

        let diagnostics = sender.hourly_diagnostics.read().await.diagnostics();
        assert_eq!(diagnostics.ok, 0);
        assert!(
            diagnostics.errored > 0,
            "We have at least one errored transaction",
        );
        assert!(
            diagnostics.errors[&-2].count > 0,
            "We have at least one timeout error",
        );
        assert!(
            diagnostics.errors[&-2].last_message.contains("Timeout"),
            "We have at least one timeout error",
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_replication_not_found(
        mut remote_bucket: MockRmBucket,
        settings: ReplicationSettings,
    ) {
        remote_bucket.expect_write_record().returning(|_, _| Ok(()));
        remote_bucket.expect_is_active().return_const(true);
        let sender = build_sender(remote_bucket, settings).await;

        let transaction = Transaction::WriteRecord(10);
        imitate_write_record(&sender, &transaction).await;
        sender
            .storage
            .write()
            .await
            .get_mut_bucket("src")
            .unwrap()
            .remove_entry("test")
            .unwrap();

        assert_eq!(sender.run().await, SyncState::SyncedOrRemoved);
        assert!(
            sender
                .log_map
                .read()
                .await
                .get("test")
                .unwrap()
                .read()
                .await
                .is_empty(),
            "We don't keep the transaction for a non existing record"
        );

        let diagnostics = sender.hourly_diagnostics.read().await.diagnostics();
        assert_eq!(
            diagnostics,
            DiagnosticsItem {
                ok: 0,
                errored: 60,
                errors: HashMap::from_iter(vec![(
                    404,
                    DiagnosticsError {
                        count: 1,
                        last_message: "Entry 'test' not found in bucket 'src'".to_string(),
                    }
                )]),
            }
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_replication_too_early_ok(
        mut remote_bucket: MockRmBucket,
        settings: ReplicationSettings,
    ) {
        remote_bucket.expect_write_record().returning(|_, _| Ok(()));
        remote_bucket.expect_is_active().return_const(true);
        let sender = build_sender(remote_bucket, settings).await;

        sender
            .log_map
            .read()
            .await
            .get("test")
            .unwrap()
            .write()
            .await
            .push_back(Transaction::WriteRecord(20))
            .await
            .unwrap();

        let tx = sender
            .storage
            .write()
            .await
            .create_bucket("src", BucketSettings::default())
            .unwrap()
            .write_record("test", 20, 4, "".to_string(), Labels::new())
            .await
            .unwrap();

        let hourly_diagnostics = sender.hourly_diagnostics.clone();

        tokio::spawn(async move {
            // we need to spawn a task to check the state in the attempt loop
            sender.run().await
        });

        tx.send(Ok(Some(Bytes::from("xxxx")))).await.unwrap();
        sleep(Duration::from_millis(15)).await;

        let diagnostics = hourly_diagnostics.read().await.diagnostics();
        assert_eq!(
            diagnostics,
            DiagnosticsItem {
                ok: 60,
                errored: 0,
                errors: HashMap::new(),
            }
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_replication_too_early_err(
        mut remote_bucket: MockRmBucket,
        settings: ReplicationSettings,
    ) {
        remote_bucket.expect_write_record().returning(|_, _| Ok(()));
        remote_bucket.expect_is_active().return_const(true);
        let sender = build_sender(remote_bucket, settings).await;

        sender
            .log_map
            .read()
            .await
            .get("test")
            .unwrap()
            .write()
            .await
            .push_back(Transaction::WriteRecord(20))
            .await
            .unwrap();

        let _tx = sender
            .storage
            .write()
            .await
            .create_bucket("src", BucketSettings::default())
            .unwrap()
            .write_record("test", 20, 4, "".to_string(), Labels::new())
            .await
            .unwrap();

        sender.run().await;

        let diagnostics = sender.hourly_diagnostics.read().await.diagnostics();
        assert_eq!(
            diagnostics,
            DiagnosticsItem {
                ok: 0,
                errored: 20,
                errors: HashMap::from_iter(vec![(
                    425,
                    DiagnosticsError {
                        count: 1,
                        last_message: "Record with timestamp 20 is still being written".to_string(),
                    }
                )]),
            }
        );
    }

    async fn imitate_write_record(sender: &ReplicationSender, transaction: &Transaction) {
        sender
            .log_map
            .read()
            .await
            .get("test")
            .unwrap()
            .write()
            .await
            .push_back(transaction.clone())
            .await
            .unwrap();
        let tx = sender
            .storage
            .write()
            .await
            .create_bucket("src", BucketSettings::default())
            .unwrap()
            .write_record(
                "test",
                transaction.timestamp().clone(),
                4,
                "text/plain".to_string(),
                Labels::new(),
            )
            .await
            .unwrap();
        tx.send(Ok(Some(Bytes::from("xxxx")))).await.unwrap();
    }

    async fn build_sender(
        remote_bucket: MockRmBucket,
        settings: ReplicationSettings,
    ) -> ReplicationSender {
        let tmp_dir = tempfile::tempdir().unwrap().into_path();

        let storage = Arc::new(RwLock::new(Storage::load(tmp_dir, None).await));

        let log_map = Arc::new(RwLock::new(HashMap::new()));
        let log = RwLock::new(
            TransactionLog::try_load_or_create(
                storage.read().await.data_path().join("test.log"),
                1000,
            )
            .await
            .unwrap(),
        );

        log_map.write().await.insert("test".to_string(), log);

        ReplicationSender {
            replication_name: "test".to_string(),
            log_map,
            storage,
            config: settings,
            hourly_diagnostics: Arc::new(RwLock::new(DiagnosticsCounter::new(
                Duration::from_secs(1),
            ))),
            bucket: Arc::new(RwLock::new(remote_bucket)),
        }
    }

    #[fixture]
    fn remote_bucket() -> MockRmBucket {
        let bucket = MockRmBucket::new();
        bucket
    }

    #[fixture]
    fn notification() -> TransactionNotification {
        TransactionNotification {
            bucket: "src".to_string(),
            entry: "test".to_string(),
            labels: Labels::new(),
            event: Transaction::WriteRecord(10),
        }
    }

    #[fixture]
    fn settings() -> ReplicationSettings {
        ReplicationSettings {
            src_bucket: "src".to_string(),
            dst_bucket: "remote".to_string(),
            dst_host: "http://localhost:8383".to_string(),
            dst_token: "token".to_string(),
            entries: vec!["test".to_string()],
            include: Labels::new(),
            exclude: Labels::new(),
        }
    }
}
