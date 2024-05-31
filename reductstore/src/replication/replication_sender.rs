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

use crate::replication::Transaction;
use crate::storage::bucket::RecordReader;
use tokio::sync::RwLock;
use tokio::time::sleep;

/// Internal worker for replication to process a sole iteration of the replication loop.
pub(super) struct ReplicationSender {
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

const MAX_PAYLOAD_SIZE: u64 = 16_000_000;
const MAX_BATCH_SIZE: usize = 100;

impl ReplicationSender {
    pub fn new(
        log_map: Arc<RwLock<HashMap<String, RwLock<TransactionLog>>>>,
        storage: Arc<RwLock<Storage>>,
        config: ReplicationSettings,
        hourly_diagnostics: Arc<RwLock<DiagnosticsCounter>>,
        bucket: Arc<RwLock<dyn RemoteBucket + Send + Sync>>,
    ) -> Self {
        Self {
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
            let tr = log.write().await.front(MAX_BATCH_SIZE).await;
            let state = match tr {
                Ok(vec) => {
                    if vec.is_empty() {
                        SyncState::NoTransactions
                    } else {
                        sync_something = true;
                        let mut batch = Vec::new();
                        let mut total_size = 0;
                        let mut processed_transactions = 0;
                        for transaction in vec {
                            debug!(
                                "Replicating transaction {}/{}/{:?}",
                                self.config.src_bucket, entry_name, transaction
                            );

                            let record_to_sync = self.read_record(entry_name, &transaction).await;
                            if let Some(record_to_sync) = record_to_sync {
                                let record_size = record_to_sync.content_length();
                                if total_size + record_size > MAX_PAYLOAD_SIZE {
                                    break;
                                }
                                total_size += record_size;
                                batch.push(record_to_sync);
                                processed_transactions += 1;
                            } else {
                                processed_transactions += 1; // we count to remove errored transactions from log
                            }
                        }

                        let mut bucket = self.bucket.write().await;
                        let batch_size = batch.len() as u64;
                        match bucket.write_batch(entry_name, batch).await {
                            Ok(map) => {
                                if bucket.is_active() {
                                    self.hourly_diagnostics
                                        .write()
                                        .await
                                        .count(Ok(()), batch_size - map.len() as u64);
                                    for (timestamp, err) in map {
                                        debug!(
                                            "Failed to replicate record {}/{}/{}: {:?}",
                                            self.config.src_bucket, entry_name, timestamp, err
                                        );
                                        self.hourly_diagnostics.write().await.count(Err(err), 1);
                                    }
                                }
                            }
                            Err(err) => {
                                debug!(
                                    "Failed to replicate batch of records from {}/{} {:?}",
                                    self.config.src_bucket, entry_name, err
                                );
                                self.hourly_diagnostics
                                    .write()
                                    .await
                                    .count(Err(err), batch_size);
                            }
                        }

                        if bucket.is_active() {
                            if let Err(err) =
                                log.write().await.pop_front(processed_transactions).await
                            {
                                error!("Failed to remove transaction: {:?}", err);
                            }

                            SyncState::SyncedOrRemoved
                        } else {
                            SyncState::NotAvailable
                        }
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

    async fn read_record(
        &self,
        entry_name: &str,
        transaction: &Transaction,
    ) -> Option<RecordReader> {
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

        match read_record_from_storage.await {
            Ok(record) => Some(record),
            Err(err) => {
                error!("Failed to read record: {}", err);
                self.hourly_diagnostics.write().await.count(Err(err), 1);
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::remote_bucket::ErrorRecordMap;
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
            async fn write_batch(
                &mut self,
                entry_name: &str,
                record: Vec<RecordReader>,
            ) -> Result<ErrorRecordMap, ReductError>;

            fn is_active(&self) -> bool;
        }

    }

    #[rstest]
    #[tokio::test]
    async fn test_replication_ok_not_active(
        mut remote_bucket: MockRmBucket,
        settings: ReplicationSettings,
    ) {
        remote_bucket
            .expect_write_batch()
            .returning(|_, _| Ok(ErrorRecordMap::new()));
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
                .front(1)
                .await,
            Ok(vec![transaction]),
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
            .expect_write_batch()
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
                .front(1)
                .await,
            Ok(vec![transaction]),
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
        remote_bucket
            .expect_write_batch()
            .returning(|_, _| Ok(ErrorRecordMap::new()));
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
        remote_bucket
            .expect_write_batch()
            .returning(|_, _| Ok(ErrorRecordMap::new()));
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
        remote_bucket
            .expect_write_batch()
            .returning(|_, _| Ok(ErrorRecordMap::new()));
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

    #[rstest]
    #[tokio::test]
    async fn test_replication_not_all_records_ok(
        mut remote_bucket: MockRmBucket,
        settings: ReplicationSettings,
    ) {
        remote_bucket.expect_write_batch().returning(|_, _| {
            Ok(ErrorRecordMap::from_iter(vec![(
                10,
                ReductError::new(ErrorCode::Conflict, "AlreadyExists"),
            )]))
        });
        remote_bucket.expect_is_active().return_const(true);
        let sender = build_sender(remote_bucket, settings).await;

        let transaction = Transaction::WriteRecord(10);
        imitate_write_record(&sender, &transaction).await;

        let transaction = Transaction::WriteRecord(20);
        imitate_write_record(&sender, &transaction).await;

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
            "We remove all errored transactions"
        );

        let diagnostics = sender.hourly_diagnostics.read().await.diagnostics();
        assert_eq!(
            diagnostics,
            DiagnosticsItem {
                ok: 60,
                errored: 60,
                errors: HashMap::from_iter(vec![(
                    409,
                    DiagnosticsError {
                        count: 1,
                        last_message: "AlreadyExists".to_string(),
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

        let mut storage = sender.storage.write().await;
        let mut bucket = match storage.create_bucket("src", BucketSettings::default()) {
            Ok(bucket) => bucket,
            Err(err) => storage.get_mut_bucket("src").unwrap(),
        };

        let tx = bucket
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
