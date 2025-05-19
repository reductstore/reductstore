// Copyright 2023-2024 ReductSoftware UG
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

use crate::replication::Transaction;
use crate::storage::entry::RecordReader;
use reduct_base::io::{ReadRecord, RecordMeta};
use std::sync::{Arc, RwLock};
use std::thread::sleep;
use std::time::Duration;

/// Internal worker for replication to process a sole iteration of the replication loop.
pub(super) struct ReplicationSender {
    log_map: Arc<RwLock<HashMap<String, RwLock<TransactionLog>>>>,
    storage: Arc<Storage>,
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

const MAX_PAYLOAD_SIZE: u64 = 4_000_000;
const MAX_BATCH_SIZE: usize = 80;

impl ReplicationSender {
    pub fn new(
        log_map: Arc<RwLock<HashMap<String, RwLock<TransactionLog>>>>,
        storage: Arc<Storage>,
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

    pub fn run(&self) -> SyncState {
        let mut sync_something = false;
        let entries = self
            .log_map
            .read()
            .unwrap()
            .keys()
            .cloned()
            .collect::<Vec<_>>();

        for entry_name in entries.iter() {
            let transactions = {
                // we can't hold the lock while we read the log
                if let Some(log) = self.log_map.read().unwrap().get(entry_name) {
                    log.write().unwrap().front(MAX_BATCH_SIZE)
                } else {
                    // log might be removed
                    continue;
                }
            };
            let state = match transactions {
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

                            let record_to_sync = self.read_record(entry_name, &transaction);
                            processed_transactions += 1;

                            if let Some(record_to_sync) = record_to_sync {
                                let record_size = record_to_sync.content_length();
                                total_size += record_size;
                                batch.push((record_to_sync, transaction));

                                if total_size >= MAX_PAYLOAD_SIZE {
                                    break;
                                }
                            }
                        }

                        let mut bucket = self.bucket.write().unwrap();
                        let batch_size = batch.len() as u64;
                        match bucket.write_batch(entry_name, batch) {
                            Ok(map) => {
                                if bucket.is_active() {
                                    self.hourly_diagnostics
                                        .write()
                                        .unwrap()
                                        .count(Ok(()), batch_size - map.len() as u64);
                                    for (timestamp, err) in map {
                                        debug!(
                                            "Failed to replicate record {}/{}/{}: {:?}",
                                            self.config.src_bucket, entry_name, timestamp, err
                                        );
                                        self.hourly_diagnostics.write().unwrap().count(Err(err), 1);
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
                                    .unwrap()
                                    .count(Err(err), batch_size);
                            }
                        }

                        if bucket.is_active() {
                            if let Err(err) = self
                                .log_map
                                .read()
                                .unwrap()
                                .get(entry_name)
                                .and_then(|log| {
                                    Some(log.write().unwrap().pop_front(processed_transactions))
                                })
                                .unwrap_or(Ok(0))
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

    fn read_record(&self, entry_name: &str, transaction: &Transaction) -> Option<RecordReader> {
        let read_record_from_storage = || {
            let mut atempts = 3;
            loop {
                let read_record = || {
                    self.storage
                        .get_bucket(&self.config.src_bucket)?
                        .upgrade()?
                        .get_entry(&entry_name)?
                        .upgrade()?
                        .begin_read(*transaction.timestamp())
                        .wait()
                };
                let record = read_record();
                match record {
                    Err(ReductError {
                        status: ErrorCode::TooEarly,
                        ..
                    }) => {
                        debug!("Transaction is too early, retrying later");
                        sleep(Duration::from_millis(10));
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

        match read_record_from_storage() {
            Ok(record) => Some(record),
            Err(err) => {
                error!("Failed to read record: {}", err);
                self.hourly_diagnostics.write().unwrap().count(Err(err), 1);
                None
            }
        }
    }
}

#[cfg(test)]
#[cfg(target_os = "linux")] // we need precise timing
mod tests {
    use super::*;

    use crate::replication::remote_bucket::ErrorRecordMap;
    use crate::replication::Transaction;
    use crate::storage::storage::{CHANNEL_BUFFER_SIZE, MAX_IO_BUFFER_SIZE};

    use bytes::Bytes;
    use mockall::mock;
    use reduct_base::error::ErrorCode;

    use reduct_base::msg::bucket_api::BucketSettings;
    use reduct_base::msg::diagnostics::{DiagnosticsError, DiagnosticsItem};
    use reduct_base::Labels;
    use rstest::*;
    use std::thread::spawn;

    mock! {
        RmBucket {}

        impl RemoteBucket for RmBucket {
            fn write_batch(
                &mut self,
                entry_name: &str,
                record: Vec<(RecordReader, Transaction)>,
            ) -> Result<ErrorRecordMap, ReductError>;

            fn is_active(&self) -> bool;
        }

    }

    #[rstest]
    fn test_replication_ok_not_active(
        mut remote_bucket: MockRmBucket,
        settings: ReplicationSettings,
    ) {
        remote_bucket
            .expect_write_batch()
            .returning(|_, _| Ok(ErrorRecordMap::new()));
        remote_bucket.expect_is_active().return_const(false);
        let sender = build_sender(remote_bucket, settings);

        let transaction = Transaction::WriteRecord(10);
        imitate_write_record(&sender, &transaction, 5);

        assert_eq!(sender.run(), SyncState::NotAvailable);
        assert_eq!(
            sender
                .log_map
                .read()
                .unwrap()
                .get("test")
                .unwrap()
                .read()
                .unwrap()
                .front(1),
            Ok(vec![transaction]),
        );

        assert_eq!(
            sender.hourly_diagnostics.read().unwrap().diagnostics(),
            DiagnosticsItem {
                ok: 0,
                errored: 0,
                errors: HashMap::new(),
            },
            "should not count errors for non active replication"
        );
    }

    #[rstest]
    fn test_replication_comm_err(mut remote_bucket: MockRmBucket) {
        remote_bucket
            .expect_write_batch()
            .returning(|_, _| Err(ReductError::new(ErrorCode::Timeout, "Timeout")));
        remote_bucket.expect_is_active().return_const(false);
        let sender = build_sender(remote_bucket, settings());

        let transaction = Transaction::WriteRecord(10);
        imitate_write_record(&sender, &transaction, 5);

        assert_eq!(sender.run(), SyncState::NotAvailable);

        assert_eq!(
            sender
                .log_map
                .read()
                .unwrap()
                .get("test")
                .unwrap()
                .read()
                .unwrap()
                .front(1),
            Ok(vec![transaction]),
        );

        let diagnostics = sender.hourly_diagnostics.read().unwrap().diagnostics();
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
    fn test_replication_not_found(mut remote_bucket: MockRmBucket, settings: ReplicationSettings) {
        remote_bucket
            .expect_write_batch()
            .returning(|_, _| Ok(ErrorRecordMap::new()));
        remote_bucket.expect_is_active().return_const(true);
        let sender = build_sender(remote_bucket, settings);

        let transaction = Transaction::WriteRecord(10);
        imitate_write_record(&sender, &transaction, 5);
        sender
            .storage
            .get_bucket("src")
            .unwrap()
            .upgrade_and_unwrap()
            .remove_entry("test")
            .wait()
            .unwrap();

        assert_eq!(sender.run(), SyncState::SyncedOrRemoved);
        assert!(
            sender
                .log_map
                .read()
                .unwrap()
                .get("test")
                .unwrap()
                .read()
                .unwrap()
                .is_empty(),
            "We don't keep the transaction for a non existing record"
        );

        let diagnostics = sender.hourly_diagnostics.read().unwrap().diagnostics();
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
    fn test_replication_too_early_ok(
        mut remote_bucket: MockRmBucket,
        settings: ReplicationSettings,
    ) {
        remote_bucket
            .expect_write_batch()
            .returning(|_, _| Ok(ErrorRecordMap::new()));
        remote_bucket.expect_is_active().return_const(true);
        let sender = build_sender(remote_bucket, settings);

        sender
            .log_map
            .read()
            .unwrap()
            .get("test")
            .unwrap()
            .write()
            .unwrap()
            .push_back(Transaction::WriteRecord(20))
            .unwrap();

        let mut writer = sender
            .storage
            .create_bucket("src", BucketSettings::default())
            .unwrap()
            .upgrade_and_unwrap()
            .begin_write("test", 20, 4, "".to_string(), Labels::new())
            .wait()
            .unwrap();

        let hourly_diagnostics = sender.hourly_diagnostics.clone();

        spawn(move || {
            // we need to spawn a task to check the state in the attempt loop
            sender.run()
        });

        writer.blocking_send(Ok(Some(Bytes::from("xxxx")))).unwrap();
        writer.blocking_send(Ok(None)).unwrap_or(());
        sleep(Duration::from_millis(100));

        let diagnostics = hourly_diagnostics.read().unwrap().diagnostics();
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
    fn test_replication_too_early_err(
        mut remote_bucket: MockRmBucket,
        settings: ReplicationSettings,
    ) {
        remote_bucket
            .expect_write_batch()
            .returning(|_, _| Ok(ErrorRecordMap::new()));
        remote_bucket.expect_is_active().return_const(true);
        let sender = build_sender(remote_bucket, settings);

        sender
            .log_map
            .read()
            .unwrap()
            .get("test")
            .unwrap()
            .write()
            .unwrap()
            .push_back(Transaction::WriteRecord(20))
            .unwrap();

        let _tx = sender
            .storage
            .create_bucket("src", BucketSettings::default())
            .unwrap()
            .upgrade_and_unwrap()
            .begin_write(
                "test",
                20,
                MAX_IO_BUFFER_SIZE * CHANNEL_BUFFER_SIZE + 1,
                "".to_string(),
                Labels::new(),
            )
            .wait()
            .unwrap();

        sender.run();

        let diagnostics = sender.hourly_diagnostics.read().unwrap().diagnostics();
        assert_eq!(
            diagnostics,
            DiagnosticsItem {
                ok: 0,
                errored: 60,
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
    fn test_replication_not_all_records_ok(
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
        let sender = build_sender(remote_bucket, settings);

        let transaction = Transaction::WriteRecord(10);
        imitate_write_record(&sender, &transaction, 5);

        let transaction = Transaction::WriteRecord(20);
        imitate_write_record(&sender, &transaction, 5);

        assert_eq!(sender.run(), SyncState::SyncedOrRemoved);
        assert!(
            sender
                .log_map
                .read()
                .unwrap()
                .get("test")
                .unwrap()
                .read()
                .unwrap()
                .is_empty(),
            "We remove all errored transactions"
        );

        let diagnostics = sender.hourly_diagnostics.read().unwrap().diagnostics();
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

    #[rstest]
    fn test_replication_record_large_payload(
        mut remote_bucket: MockRmBucket,
        settings: ReplicationSettings,
    ) {
        remote_bucket
            .expect_write_batch()
            .returning(|_, _| Ok(ErrorRecordMap::new()));
        remote_bucket.expect_is_active().return_const(true);
        let sender = build_sender(remote_bucket, settings);

        let transaction = Transaction::WriteRecord(10);
        imitate_write_record(&sender, &transaction, (MAX_PAYLOAD_SIZE + 1) as usize);

        assert_eq!(sender.run(), SyncState::SyncedOrRemoved);
        assert!(
            sender
                .log_map
                .read()
                .unwrap()
                .get("test")
                .unwrap()
                .read()
                .unwrap()
                .is_empty(),
            "We remove all errored transactions"
        );

        let diagnostics = sender.hourly_diagnostics.read().unwrap().diagnostics();
        assert!(diagnostics.ok > 0, "records were replicated");
        assert_eq!(diagnostics.errored, 0, "no errors happened");
    }

    fn imitate_write_record(sender: &ReplicationSender, transaction: &Transaction, size: usize) {
        sender
            .log_map
            .read()
            .unwrap()
            .get("test")
            .unwrap()
            .write()
            .unwrap()
            .push_back(transaction.clone())
            .unwrap();

        let bucket = match sender
            .storage
            .create_bucket("src", BucketSettings::default())
        {
            Ok(bucket) => bucket,
            Err(_err) => sender.storage.get_bucket("src").unwrap(),
        };

        let mut writer = bucket
            .upgrade_and_unwrap()
            .begin_write(
                "test",
                transaction.timestamp().clone(),
                size,
                "text/plain".to_string(),
                Labels::new(),
            )
            .wait()
            .unwrap();
        writer
            .blocking_send(Ok(Some(Bytes::from(
                (0..size).map(|_| 'x').collect::<String>(),
            ))))
            .unwrap();
        writer.blocking_send(Ok(None)).unwrap_or(());
    }

    fn build_sender(
        remote_bucket: MockRmBucket,
        settings: ReplicationSettings,
    ) -> ReplicationSender {
        let tmp_dir = tempfile::tempdir().unwrap().keep();

        let storage = Arc::new(Storage::load(tmp_dir, None));

        let log_map = Arc::new(RwLock::new(HashMap::new()));
        let log = RwLock::new(
            TransactionLog::try_load_or_create(storage.data_path().join("test.log"), 1000).unwrap(),
        );

        log_map.write().unwrap().insert("test".to_string(), log);

        ReplicationSender {
            log_map,
            storage,
            config: settings,
            hourly_diagnostics: Arc::new(RwLock::new(DiagnosticsCounter::new(
                Duration::from_secs(3600),
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
            each_n: None,
            each_s: None,
            when: None,
        }
    }
}
