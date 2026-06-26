// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::cfg::io::IoConfig;
use crate::core::sync::AsyncRwLock;
use crate::replication::remote_bucket::RemoteBucket;
use crate::replication::transaction_log::{TransactionLogMap, TransactionLogRef};
use crate::replication::Transaction;
use crate::storage::engine::StorageEngine;
use log::{debug, error};
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::io::BoxedReadRecord;
use reduct_base::msg::replication_api::ReplicationSettings;
use std::cmp::PartialEq;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

/// Internal worker for replication to process a sole iteration of the replication loop.
///
/// Sends are pipelined with depth 1: the current entry's batch is sent by a
/// spawned task while the next entry's batch is prepared. The remote bucket
/// never leaves the sender: it stays behind a lock that the send task holds
/// for the duration of the send, so its state survives even a panicked send.
pub(super) struct ReplicationSender {
    log_map: TransactionLogMap,
    storage: Arc<StorageEngine>,
    settings: ReplicationSettings,
    io_config: IoConfig,
    bucket: Arc<AsyncRwLock<Box<dyn RemoteBucket + Send + Sync>>>,
}

/// Outcome of replicating a group of records: the result, the number of
/// records it covers, and the number of bytes successfully replicated
/// (non-zero only for successful writes).
type ResultResult = (Result<(), ReductError>, u64, u64);

#[derive(Debug, PartialEq)]
pub(super) enum SyncState {
    SyncedOrRemoved(Vec<ResultResult>),
    NotAvailable(Vec<ResultResult>),
    NoTransactions,
    BrokenLog(String),
}

/// Result of a spawned send task: the accounting entries for the processed
/// batch and the bucket availability after the send.
struct SendOutcome {
    counter: Vec<ResultResult>,
    bucket_active: bool,
}

/// One entry's batch prepared for sending, together with everything needed to
/// account for it and to acknowledge the transaction log on success.
struct PreparedBatch {
    entry_name: String,
    dst_entry_name: String,
    batch: Vec<(BoxedReadRecord, Transaction)>,
    batch_sizes: BTreeMap<u64, u64>,
    counter: Vec<ResultResult>,
    processed_transactions: usize,
    log: TransactionLogRef,
}

/// Why a batch could not be prepared for an entry.
enum PrepareError {
    /// The transaction log could not be read and must be recreated.
    BrokenLog,
    Internal(ReductError),
}

impl From<ReductError> for PrepareError {
    fn from(err: ReductError) -> Self {
        PrepareError::Internal(err)
    }
}

impl ReplicationSender {
    pub fn new(
        log_map: TransactionLogMap,
        storage: Arc<StorageEngine>,
        config: ReplicationSettings,
        io_config: IoConfig,
        bucket: Box<dyn RemoteBucket + Send + Sync>,
    ) -> Self {
        Self {
            log_map,
            storage,
            settings: config,
            io_config,
            bucket: Arc::new(AsyncRwLock::new(bucket)),
        }
    }

    pub async fn probe_availability(&mut self) -> bool {
        let mut bucket = match self.bucket.write().await {
            Ok(bucket) => bucket,
            Err(err) => {
                error!("Failed to lock remote bucket to probe it: {:?}", err);
                return false;
            }
        };
        bucket.probe_availability().await;
        bucket.is_active()
    }

    pub async fn run(&mut self) -> Result<SyncState, ReductError> {
        let mut entries = self
            .log_map
            .read()
            .await?
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        entries.sort();

        let mut counter = Vec::new();
        let mut send_task: Option<JoinHandle<SendOutcome>> = None;

        for entry_name in entries.iter() {
            // Depth-1 pipelining: prepare this entry's batch while the
            // previous batch is sent by `send_task` in the background.
            let prepare_result = Self::prepare_batch(
                &self.log_map,
                &self.storage,
                &self.settings,
                &self.io_config,
                entry_name,
            )
            .await;

            if let Some(task) = send_task.take() {
                if !Self::join_send_task(task, &mut counter).await {
                    // The remote bucket is not available: stop the pass, the
                    // remaining transactions (including the freshly prepared
                    // batch) stay in their logs.
                    return Ok(Self::make_state(counter, false));
                }
            }

            match prepare_result {
                Ok(Some(prepared)) => {
                    send_task = Some(tokio::spawn(Self::send_batch(
                        Arc::clone(&self.bucket),
                        prepared,
                        self.settings.src_bucket.clone(),
                    )));
                }
                Ok(None) => {}
                Err(PrepareError::BrokenLog) => {
                    return Ok(SyncState::BrokenLog(entry_name.clone()))
                }
                Err(PrepareError::Internal(err)) => return Err(err),
            }
        }

        // wait for the last send
        let mut bucket_active = true;
        if let Some(task) = send_task.take() {
            bucket_active = Self::join_send_task(task, &mut counter).await;
        }

        Ok(Self::make_state(counter, bucket_active))
    }

    /// Wait for a spawned send task and merge its accounting into `counter`.
    /// Returns whether the remote bucket is still available.
    async fn join_send_task(
        send_task: JoinHandle<SendOutcome>,
        counter: &mut Vec<ResultResult>,
    ) -> bool {
        match send_task.await {
            Ok(outcome) => {
                counter.extend(outcome.counter);
                outcome.bucket_active
            }
            Err(err) => {
                // The bucket stays in the sender behind its lock, so a
                // panicked send only loses this batch's accounting.
                error!("Replication send task failed: {:?}", err);
                true
            }
        }
    }

    fn make_state(counter: Vec<ResultResult>, bucket_active: bool) -> SyncState {
        if !counter.is_empty() {
            if bucket_active {
                SyncState::SyncedOrRemoved(counter)
            } else {
                SyncState::NotAvailable(counter)
            }
        } else {
            SyncState::NoTransactions
        }
    }

    /// Read the next batch of transactions of the entry from its log and the
    /// records to send from the local storage. Read failures are accounted in
    /// the batch's counter.
    async fn prepare_batch(
        log_map: &TransactionLogMap,
        storage: &Arc<StorageEngine>,
        settings: &ReplicationSettings,
        io_config: &IoConfig,
        entry_name: &str,
    ) -> Result<Option<PreparedBatch>, PrepareError> {
        let log = {
            // Take only the handle, drop the map lock before touching the log itself.
            let map = log_map.read().await?;
            match map.get(entry_name) {
                Some(log) => Arc::clone(log),
                None => return Ok(None), // log might be removed
            }
        };

        let transactions = {
            let log = log.write().await?;
            log.front(io_config.batch_max_records).await
        };
        let vec = match transactions {
            Ok(vec) => vec,
            Err(err) => {
                error!("Failed to read transaction: {:?}", err);
                return Err(PrepareError::BrokenLog);
            }
        };

        if vec.is_empty() {
            return Ok(None);
        }

        let mut batch = Vec::with_capacity(vec.len());
        let mut batch_sizes: BTreeMap<u64, u64> = BTreeMap::new();
        let mut counter = Vec::new();
        let mut total_size = 0;
        let mut processed_transactions = 0;
        for transaction in vec {
            debug!(
                "Replicating transaction {}/{}/{:?}",
                settings.src_bucket, entry_name, transaction
            );

            let record_to_sync =
                Self::read_record(storage, settings, entry_name, &transaction).await;
            processed_transactions += 1;

            match record_to_sync {
                Ok(record_to_sync) => {
                    let record_size = record_to_sync.meta().content_length();
                    total_size += record_size;
                    batch_sizes.insert(*transaction.timestamp(), record_size);
                    batch.push((record_to_sync, transaction));

                    if total_size >= io_config.batch_max_size {
                        break;
                    }
                }
                Err(err) => {
                    error!(
                        "Failed to read record {}/{}/{}: {:?}",
                        settings.src_bucket,
                        entry_name,
                        transaction.timestamp(),
                        err
                    );
                    counter.push((Err(err), 1, 0));
                }
            }
        }

        Ok(Some(PreparedBatch {
            entry_name: entry_name.to_string(),
            dst_entry_name: Self::destination_entry_name(&settings.dst_prefix, entry_name),
            batch,
            batch_sizes,
            counter,
            processed_transactions,
            log,
        }))
    }

    /// Send a prepared batch to the remote bucket and remove the processed
    /// transactions from the entry's log. Returns the accounting entries of
    /// the batch and whether the remote bucket is still available; if it is
    /// not, the log is kept intact so the transactions are resent.
    async fn send_batch(
        bucket: Arc<AsyncRwLock<Box<dyn RemoteBucket + Send + Sync>>>,
        prepared: PreparedBatch,
        src_bucket: String,
    ) -> SendOutcome {
        let PreparedBatch {
            entry_name,
            dst_entry_name,
            batch,
            batch_sizes,
            mut counter,
            processed_transactions,
            log,
        } = prepared;

        let mut bucket = match bucket.write().await {
            Ok(bucket) => bucket,
            Err(err) => {
                error!("Failed to lock remote bucket for sending: {:?}", err);
                return SendOutcome {
                    counter,
                    bucket_active: false,
                };
            }
        };

        let batch_size = batch.len() as u64;
        match bucket.write_batch(&dst_entry_name, batch).await {
            Ok(map) => {
                // Bytes successfully replicated are those whose
                // timestamp is not reported as failed.
                let written_size: u64 = batch_sizes
                    .iter()
                    .filter(|(timestamp, _)| !map.contains_key(*timestamp))
                    .map(|(_, size)| *size)
                    .sum();
                counter.push((Ok(()), batch_size - map.len() as u64, written_size));
                for (timestamp, err) in map.into_iter() {
                    debug!(
                        "Failed to replicate record {}/{}/{}: {:?}",
                        src_bucket, entry_name, timestamp, err
                    );
                    counter.push((Err(err), 1, 0));
                }
            }
            Err(err) => {
                debug!(
                    "Failed to replicate batch of records from {}/{} {:?}",
                    src_bucket, entry_name, err
                );

                counter.push((Err(err), batch_size, 0));
            }
        }

        let bucket_active = bucket.is_active();
        if bucket_active {
            // remove processed transactions from the log
            match log.write().await {
                Ok(mut log) => {
                    if let Err(err) = log.pop_front(processed_transactions).await {
                        error!("Failed to remove transaction: {:?}", err);
                    }
                }
                Err(err) => error!(
                    "Failed to lock transaction log to remove transactions: {:?}",
                    err
                ),
            }
        }

        SendOutcome {
            counter,
            bucket_active,
        }
    }

    async fn read_record(
        storage: &Arc<StorageEngine>,
        settings: &ReplicationSettings,
        entry_name: &str,
        transaction: &Transaction,
    ) -> Result<BoxedReadRecord, ReductError> {
        let read_record_from_storage = async || {
            let mut attempts = 3;
            loop {
                let read_record = async || {
                    storage
                        .get_bucket(&settings.src_bucket)
                        .await?
                        .upgrade()?
                        .get_entry(entry_name)
                        .await?
                        .upgrade()?
                        .begin_read(*transaction.timestamp())
                        .await
                };
                let record = read_record().await;
                match record {
                    Err(ReductError {
                        status: ErrorCode::TooEarly,
                        ..
                    }) => {
                        debug!("Transaction is too early, retrying later");
                        sleep(Duration::from_millis(10)).await;
                        attempts -= 1;
                    }

                    _ => {
                        attempts = 0;
                    }
                }

                if attempts == 0 {
                    break record;
                }
            }
        };

        match read_record_from_storage().await {
            Ok(record) => Ok(Box::new(record)),
            Err(err) => Err(err),
        }
    }

    fn destination_entry_name(prefix: &str, entry_name: &str) -> String {
        let prefix = prefix.trim_matches('/');
        if prefix.is_empty() {
            entry_name.to_string()
        } else {
            format!("{}/{}", prefix, entry_name.trim_start_matches('/'))
        }
    }
}

#[cfg(test)]
mod destination_entry_name_tests {
    use super::ReplicationSender;

    #[test]
    fn keeps_entry_name_when_prefix_is_empty() {
        assert_eq!(
            ReplicationSender::destination_entry_name("", "camera/front"),
            "camera/front"
        );
    }

    #[test]
    fn prepends_destination_prefix() {
        assert_eq!(
            ReplicationSender::destination_entry_name("robot-1", "camera/front"),
            "robot-1/camera/front"
        );
    }

    #[test]
    fn avoids_duplicate_slashes() {
        assert_eq!(
            ReplicationSender::destination_entry_name("/robot-1/", "/camera/front"),
            "robot-1/camera/front"
        );
    }
}

#[cfg(test)]
#[cfg(target_os = "linux")] // we need precise timing
mod tests {
    use super::*;

    use crate::cfg::Cfg;

    use crate::core::sync::AsyncRwLock;
    use crate::replication::remote_bucket::ErrorRecordMap;
    use crate::replication::transaction_log::TransactionLog;
    use crate::replication::transaction_log::TransactionLogRef;
    use crate::replication::Transaction;
    use crate::storage::engine::{CHANNEL_BUFFER_SIZE, MAX_IO_BUFFER_SIZE};
    use async_trait::async_trait;
    use bytes::Bytes;
    use mockall::{mock, predicate};
    use reduct_base::error::ErrorCode;
    use reduct_base::error::ReductError;
    use reduct_base::msg::bucket_api::BucketSettings;
    use reduct_base::msg::replication_api::ReplicationMode;
    use reduct_base::{conflict, not_found, timeout, too_early, Labels};
    use rstest::*;
    use std::collections::HashMap;
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::task::JoinHandle;
    use tokio::time::{sleep, Duration};

    mock! {
        RmBucket {}

        #[async_trait]
        impl RemoteBucket for RmBucket {
            async fn write_batch(
                &mut self,
                entry_name: &str,
                record: Vec<(BoxedReadRecord, Transaction)>,
            ) -> Result<ErrorRecordMap, ReductError>;

            async fn probe_availability(&mut self);

            fn is_active(&self) -> bool;
        }

    }

    #[rstest]
    #[tokio::test]
    async fn test_replication_ok(mut remote_bucket: MockRmBucket, settings: ReplicationSettings) {
        remote_bucket
            .expect_write_batch()
            .returning(|_, _| Ok(ErrorRecordMap::new()));
        remote_bucket.expect_is_active().return_const(true);
        let mut sender = build_sender(remote_bucket, settings).await;

        let transaction = Transaction::WriteRecord(10);
        imitate_write_record(&sender, &transaction, 5).await;

        assert_eq!(
            sender.run().await.unwrap(),
            SyncState::SyncedOrRemoved(vec![(Ok(()), 1, 5)])
        );
        assert_eq!(
            sender
                .log_map
                .read()
                .await
                .unwrap()
                .get("test")
                .unwrap()
                .read()
                .await
                .unwrap()
                .front(1)
                .await,
            Ok(vec![]),
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_replication_applies_destination_prefix(
        mut remote_bucket: MockRmBucket,
        mut settings: ReplicationSettings,
    ) {
        settings.dst_prefix = "robot-1".to_string();
        remote_bucket
            .expect_write_batch()
            .with(predicate::eq("robot-1/test"), predicate::always())
            .returning(|_, _| Ok(ErrorRecordMap::new()));
        remote_bucket.expect_is_active().return_const(true);
        let mut sender = build_sender(remote_bucket, settings).await;

        let transaction = Transaction::WriteRecord(10);
        imitate_write_record(&sender, &transaction, 5).await;

        assert_eq!(
            sender.run().await.unwrap(),
            SyncState::SyncedOrRemoved(vec![(Ok(()), 1, 5)])
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_replication_comm_err(mut remote_bucket: MockRmBucket) {
        remote_bucket
            .expect_write_batch()
            .returning(|_, _| Err(ReductError::new(ErrorCode::Timeout, "Timeout")));
        remote_bucket.expect_is_active().return_const(false);
        let mut sender = build_sender(remote_bucket, settings()).await;

        let transaction = Transaction::WriteRecord(10);
        imitate_write_record(&sender, &transaction, 5).await;

        assert_eq!(
            sender.run().await.unwrap(),
            SyncState::NotAvailable(vec![(Err(timeout!("Timeout")), 1, 0)])
        );

        assert_eq!(
            sender
                .log_map
                .read()
                .await
                .unwrap()
                .get("test")
                .unwrap()
                .read()
                .await
                .unwrap()
                .front(1)
                .await,
            Ok(vec![transaction]),
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_replication_429_keeps_transactions(mut remote_bucket: MockRmBucket) {
        remote_bucket
            .expect_write_batch()
            .returning(|_, _| Err(ReductError::new(ErrorCode::TooManyRequests, "slow down")));
        remote_bucket.expect_is_active().return_const(false);
        let mut sender = build_sender(remote_bucket, settings()).await;

        let transaction = Transaction::WriteRecord(10);
        imitate_write_record(&sender, &transaction, 5).await;

        assert_eq!(
            sender.run().await.unwrap(),
            SyncState::NotAvailable(vec![(
                Err(ReductError::new(ErrorCode::TooManyRequests, "slow down")),
                1,
                0,
            )])
        );

        assert_eq!(
            sender
                .log_map
                .read()
                .await
                .unwrap()
                .get("test")
                .unwrap()
                .read()
                .await
                .unwrap()
                .front(1)
                .await,
            Ok(vec![transaction]),
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
        let mut sender = build_sender(remote_bucket, settings).await;

        let transaction = Transaction::WriteRecord(10);
        imitate_write_record(&sender, &transaction, 5).await;
        sender
            .storage
            .get_bucket("src")
            .await
            .unwrap()
            .upgrade_and_unwrap()
            .remove_entry("test")
            .await
            .unwrap();

        sleep(Duration::from_millis(50)).await; // ensure the deletion is fully processed

        assert_eq!(
            sender.run().await.unwrap(),
            SyncState::SyncedOrRemoved(vec![
                (
                    Err(not_found!("Entry 'test' not found in bucket 'src'")),
                    1,
                    0
                ),
                (Ok(()), 0, 0)
            ]),
        );
        assert!(
            sender
                .log_map
                .read()
                .await
                .unwrap()
                .get("test")
                .unwrap()
                .read()
                .await
                .unwrap()
                .is_empty(),
            "We don't keep the transaction for a non existing record"
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
        let mut sender = build_sender(remote_bucket, settings).await;

        {
            let map = sender.log_map.write().await.unwrap();
            let log = map.get("test").unwrap().clone();
            log.write()
                .await
                .unwrap()
                .push_back(Transaction::WriteRecord(20))
                .await
                .unwrap();
        }

        let bucket = sender
            .storage
            .create_bucket("src", BucketSettings::default())
            .await
            .unwrap()
            .upgrade_and_unwrap();
        let mut writer = bucket
            .begin_write("test", 20, 4, "".to_string(), Labels::new())
            .await
            .unwrap();

        let handle: JoinHandle<Result<SyncState, ReductError>> =
            tokio::spawn(async move { sender.run().await });

        writer.send(Ok(Some(Bytes::from("xxxx")))).await.unwrap();
        writer.send(Ok(None)).await.unwrap();
        assert_eq!(
            handle.await.unwrap().unwrap(),
            SyncState::SyncedOrRemoved(vec![(Ok(()), 1, 4)])
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
        let mut sender = build_sender(remote_bucket, settings).await;

        {
            let map = sender.log_map.write().await.unwrap();
            let log = map.get("test").unwrap().clone();
            log.write()
                .await
                .unwrap()
                .push_back(Transaction::WriteRecord(20))
                .await
                .unwrap();
        }

        let _tx = sender
            .storage
            .create_bucket("src", BucketSettings::default())
            .await
            .unwrap()
            .upgrade_and_unwrap()
            .begin_write(
                "test",
                20,
                (MAX_IO_BUFFER_SIZE * CHANNEL_BUFFER_SIZE + 1) as u64,
                "".to_string(),
                Labels::new(),
            )
            .await
            .unwrap();

        assert_eq!(
            sender.run().await.unwrap(),
            SyncState::SyncedOrRemoved(vec![
                (
                    Err(too_early!(
                        "Record with timestamp 20 in src/test is still being written"
                    )),
                    1,
                    0
                ),
                (Ok(()), 0, 0)
            ])
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
        let mut sender = build_sender(remote_bucket, settings).await;

        let transaction = Transaction::WriteRecord(10);
        imitate_write_record(&sender, &transaction, 5).await;

        let transaction = Transaction::WriteRecord(20);
        imitate_write_record(&sender, &transaction, 5).await;

        assert_eq!(
            sender.run().await.unwrap(),
            SyncState::SyncedOrRemoved(vec![
                (Ok(()), 1, 5),
                (Err(conflict!("AlreadyExists")), 1, 0)
            ])
        );
        assert!(
            sender
                .log_map
                .read()
                .await
                .unwrap()
                .get("test")
                .unwrap()
                .read()
                .await
                .unwrap()
                .is_empty(),
            "We remove all errored transactions"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_replication_record_large_payload(
        mut remote_bucket: MockRmBucket,
        settings: ReplicationSettings,
    ) {
        remote_bucket
            .expect_write_batch()
            .returning(|_, _| Ok(ErrorRecordMap::new()));
        remote_bucket.expect_is_active().return_const(true);
        let mut sender = build_sender(remote_bucket, settings).await;

        let transaction = Transaction::WriteRecord(10);
        imitate_write_record(
            &sender,
            &transaction,
            IoConfig::default().batch_max_size + 1,
        )
        .await;

        assert_eq!(
            sender.run().await.unwrap(),
            SyncState::SyncedOrRemoved(vec![(Ok(()), 1, IoConfig::default().batch_max_size + 1)])
        );
        assert!(
            sender
                .log_map
                .read()
                .await
                .unwrap()
                .get("test")
                .unwrap()
                .read()
                .await
                .unwrap()
                .is_empty(),
            "We remove all errored transactions"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_skips_removed_log_entry(
        remote_bucket: MockRmBucket,
        settings: ReplicationSettings,
    ) {
        let cfg = Cfg {
            data_path: tempfile::tempdir().unwrap().keep(),
            ..Default::default()
        };

        let storage = Arc::new(
            StorageEngine::builder()
                .with_data_path(cfg.data_path.clone())
                .with_cfg(cfg)
                .build()
                .await,
        );

        let log_map: TransactionLogMap = Arc::new(AsyncRwLock::new(HashMap::new()));
        log_map.write().await.unwrap().insert(
            "gone".to_string(),
            Arc::new(AsyncRwLock::new(
                TransactionLog::try_load_or_create(&storage.data_path().join("gone.log"), 10)
                    .await
                    .unwrap(),
            )),
        );
        log_map.write().await.unwrap().remove("gone");

        let mut sender = ReplicationSender::new(
            log_map,
            storage,
            settings,
            IoConfig::default(),
            Box::new(remote_bucket),
        );

        assert_eq!(sender.run().await.unwrap(), SyncState::NoTransactions);
    }

    #[rstest]
    #[tokio::test]
    async fn test_pipelined_multi_entry_drains_final_send(
        mut remote_bucket: MockRmBucket,
        settings: ReplicationSettings,
    ) {
        remote_bucket
            .expect_write_batch()
            .times(2)
            .returning(|_, _| Ok(ErrorRecordMap::new()));
        remote_bucket.expect_is_active().return_const(true);
        let mut sender =
            build_sender_with_entries(remote_bucket, settings, &["test", "test2"]).await;

        imitate_write_record_to(&sender, "test", &Transaction::WriteRecord(10), 5).await;
        imitate_write_record_to(&sender, "test2", &Transaction::WriteRecord(10), 5).await;

        assert_eq!(
            sender.run().await.unwrap(),
            SyncState::SyncedOrRemoved(vec![(Ok(()), 1, 5), (Ok(()), 1, 5)]),
            "the last pending send must be drained after the loop"
        );
        for entry in ["test", "test2"] {
            assert!(
                log_front(&sender, entry).await.is_empty(),
                "transaction log of '{}' is acked",
                entry
            );
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_pipelined_ack_is_per_entry(
        mut remote_bucket: MockRmBucket,
        settings: ReplicationSettings,
    ) {
        let active = Arc::new(AtomicBool::new(true));
        let active_writer = Arc::clone(&active);
        remote_bucket
            .expect_write_batch()
            .returning(move |entry, _| {
                if entry == "b_fail" {
                    active_writer.store(false, Ordering::SeqCst);
                    Err(ReductError::new(ErrorCode::TooManyRequests, "slow down"))
                } else {
                    Ok(ErrorRecordMap::new())
                }
            });
        let active_reader = Arc::clone(&active);
        remote_bucket
            .expect_is_active()
            .returning(move || active_reader.load(Ordering::SeqCst));

        let mut sender =
            build_sender_with_entries(remote_bucket, settings, &["a_ok", "b_fail"]).await;
        imitate_write_record_to(&sender, "a_ok", &Transaction::WriteRecord(10), 5).await;
        imitate_write_record_to(&sender, "b_fail", &Transaction::WriteRecord(10), 5).await;

        assert_eq!(
            sender.run().await.unwrap(),
            SyncState::NotAvailable(vec![
                (Ok(()), 1, 5),
                (
                    Err(ReductError::new(ErrorCode::TooManyRequests, "slow down")),
                    1,
                    0
                ),
            ])
        );
        assert!(
            log_front(&sender, "a_ok").await.is_empty(),
            "the succeeding entry is acked"
        );
        assert_eq!(
            log_front(&sender, "b_fail").await,
            vec![Transaction::WriteRecord(10)],
            "the failing entry keeps its transactions"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_broken_log_drains_pending_send(
        mut remote_bucket: MockRmBucket,
        settings: ReplicationSettings,
    ) {
        remote_bucket
            .expect_write_batch()
            .returning(|_, _| Ok(ErrorRecordMap::new()));
        remote_bucket.expect_is_active().return_const(true);
        let mut sender =
            build_sender_with_entries(remote_bucket, settings, &["a_ok", "b_bad"]).await;

        imitate_write_record_to(&sender, "a_ok", &Transaction::WriteRecord(10), 5).await;
        // Push a transaction to "b_bad", then corrupt its type byte on disk
        // so that front() fails with a broken log.
        {
            let log = {
                let map = sender.log_map.read().await.unwrap();
                Arc::clone(map.get("b_bad").unwrap())
            };
            log.write()
                .await
                .unwrap()
                .push_back(Transaction::WriteRecord(10))
                .await
                .unwrap();

            let path = sender.storage.data_path().join("b_bad.log");
            let mut file = OpenOptions::new().write(true).open(path).unwrap();
            file.seek(SeekFrom::Start(16)).unwrap(); // type byte of the first entry
            file.write_all(&[99]).unwrap();
            file.sync_all().unwrap();
        }

        // "a_ok" is still in flight when "b_bad" turns out broken; the
        // pending send must be drained so the bucket survives the early return.
        assert_eq!(
            sender.run().await.unwrap(),
            SyncState::BrokenLog("b_bad".to_string())
        );
        assert!(
            log_front(&sender, "a_ok").await.is_empty(),
            "the in-flight entry is still acked"
        );

        // The bucket survived: replication keeps working after the broken log is dropped.
        sender.log_map.write().await.unwrap().remove("b_bad");
        imitate_write_record_to(&sender, "a_ok", &Transaction::WriteRecord(20), 5).await;
        assert_eq!(
            sender.run().await.unwrap(),
            SyncState::SyncedOrRemoved(vec![(Ok(()), 1, 5)])
        );
    }

    async fn imitate_write_record(
        sender: &ReplicationSender,
        transaction: &Transaction,
        size: u64,
    ) {
        imitate_write_record_to(sender, "test", transaction, size).await;
    }

    async fn imitate_write_record_to(
        sender: &ReplicationSender,
        entry: &str,
        transaction: &Transaction,
        size: u64,
    ) {
        let log = {
            let map = sender.log_map.write().await.unwrap();
            map.get(entry).unwrap().clone()
        };

        log.write()
            .await
            .unwrap()
            .push_back(transaction.clone())
            .await
            .unwrap();

        let bucket = match sender
            .storage
            .create_bucket("src", BucketSettings::default())
            .await
        {
            Ok(bucket) => bucket,
            Err(_err) => sender.storage.get_bucket("src").await.unwrap(),
        };

        let mut writer = bucket
            .upgrade_and_unwrap()
            .begin_write(
                entry,
                *transaction.timestamp(),
                size,
                "text/plain".to_string(),
                Labels::new(),
            )
            .await
            .unwrap();
        writer
            .send(Ok(Some(Bytes::from(
                (0..size).map(|_| 'x').collect::<String>(),
            ))))
            .await
            .unwrap();
        writer.send(Ok(None)).await.unwrap();
    }

    async fn build_sender(
        remote_bucket: MockRmBucket,
        settings: ReplicationSettings,
    ) -> ReplicationSender {
        build_sender_with_entries(remote_bucket, settings, &["test"]).await
    }

    async fn build_sender_with_entries(
        remote_bucket: MockRmBucket,
        settings: ReplicationSettings,
        entries: &[&str],
    ) -> ReplicationSender {
        let cfg = Cfg {
            data_path: tempfile::tempdir().unwrap().keep(),
            ..Default::default()
        };

        let storage = Arc::new(
            StorageEngine::builder()
                .with_data_path(cfg.data_path.clone())
                .with_cfg(cfg)
                .build()
                .await,
        );

        let log_map: TransactionLogMap = Arc::new(AsyncRwLock::new(HashMap::new()));
        for entry in entries {
            let log: TransactionLogRef = Arc::new(AsyncRwLock::new(
                TransactionLog::try_load_or_create(
                    &storage.data_path().join(format!("{}.log", entry)),
                    1000,
                )
                .await
                .unwrap(),
            ));

            log_map
                .write()
                .await
                .unwrap()
                .insert(entry.to_string(), log);
        }

        ReplicationSender {
            log_map,
            storage,
            settings,
            io_config: IoConfig::default(),
            bucket: Arc::new(AsyncRwLock::new(Box::new(remote_bucket))),
        }
    }

    async fn log_front(sender: &ReplicationSender, entry: &str) -> Vec<Transaction> {
        sender
            .log_map
            .read()
            .await
            .unwrap()
            .get(entry)
            .unwrap()
            .read()
            .await
            .unwrap()
            .front(10)
            .await
            .unwrap()
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
            dst_token: Some("token".to_string()),
            entries: vec!["test".to_string()],
            dst_prefix: String::new(),
            exclude: Labels::new(),
            each_n: None,
            when: None,
            mode: ReplicationMode::Enabled,
            compression: Default::default(),
        }
    }
}
