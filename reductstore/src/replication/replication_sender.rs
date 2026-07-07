// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::cfg::io::IoConfig;
use crate::replication::remote_bucket::{BucketFactory, RemoteBucket};
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
/// Sends are pipelined with depth 1: while a batch is being sent in a spawned
/// task (which owns the remote bucket for the duration of the send), the next
/// entry's batch is prepared. The bucket is `None` only while a send is in
/// flight or after a panicked send that could not be rebuilt from the factory.
pub(super) struct ReplicationSender {
    log_map: TransactionLogMap,
    storage: Arc<StorageEngine>,
    settings: ReplicationSettings,
    io_config: IoConfig,
    bucket: Option<Box<dyn RemoteBucket + Send + Sync>>,
    bucket_factory: BucketFactory,
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

/// Result of a spawned send task: the remote bucket handed back to the
/// sender, the accounting entries for the processed batch and the bucket
/// availability after the send.
struct PendingSend {
    bucket: Box<dyn RemoteBucket + Send + Sync>,
    counter: Vec<ResultResult>,
    bucket_active: bool,
}

/// One entry's batch together with everything the spawned send task needs to
/// account for it and to acknowledge the transaction log on success.
struct SendBatch {
    bucket: Box<dyn RemoteBucket + Send + Sync>,
    src_bucket: String,
    entry_name: String,
    dst_entry_name: String,
    batch: Vec<(BoxedReadRecord, Transaction)>,
    batch_sizes: BTreeMap<u64, u64>,
    counter: Vec<ResultResult>,
    processed_transactions: usize,
    log: TransactionLogRef,
}

impl SendBatch {
    async fn send(self) -> PendingSend {
        let SendBatch {
            mut bucket,
            src_bucket,
            entry_name,
            dst_entry_name,
            batch,
            batch_sizes,
            mut counter,
            processed_transactions,
            log,
        } = self;

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

        PendingSend {
            bucket,
            counter,
            bucket_active,
        }
    }
}

impl ReplicationSender {
    pub fn new(
        log_map: TransactionLogMap,
        storage: Arc<StorageEngine>,
        config: ReplicationSettings,
        io_config: IoConfig,
        bucket: Box<dyn RemoteBucket + Send + Sync>,
        bucket_factory: BucketFactory,
    ) -> Self {
        Self {
            log_map,
            storage,
            settings: config,
            io_config,
            bucket: Some(bucket),
            bucket_factory,
        }
    }

    pub async fn probe_availability(&mut self) -> bool {
        let Some(bucket) = self.ensure_bucket() else {
            return false;
        };
        bucket.probe_availability().await;
        bucket.is_active()
    }

    pub async fn run(&mut self) -> Result<SyncState, ReductError> {
        let mut pending_send: Option<JoinHandle<PendingSend>> = None;
        let mut counter = Vec::new();

        let result = self.run_loop(&mut pending_send, &mut counter).await;

        // Single drain point: every exit from the loop (completion, broken
        // log or error) must join the in-flight send to get the bucket back.
        if let Some(handle) = pending_send.take() {
            self.drain_pending(handle, &mut counter).await;
        }

        if let Some(broken_entry) = result? {
            return Ok(SyncState::BrokenLog(broken_entry));
        }

        Ok(if !counter.is_empty() {
            let bucket_active = self
                .bucket
                .as_ref()
                .map(|bucket| bucket.is_active())
                .unwrap_or(false);
            if bucket_active {
                SyncState::SyncedOrRemoved(counter)
            } else {
                SyncState::NotAvailable(counter)
            }
        } else {
            SyncState::NoTransactions
        })
    }

    /// Walk the entries pipelining the sends: prepare the next entry's batch
    /// while the previous one is being sent, joining the previous send before
    /// spawning the next. Returns the entry name of a broken transaction log,
    /// if any; the caller drains a still-pending send on every exit path.
    async fn run_loop(
        &mut self,
        pending_send: &mut Option<JoinHandle<PendingSend>>,
        counter: &mut Vec<ResultResult>,
    ) -> Result<Option<String>, ReductError> {
        let mut entries = self
            .log_map
            .read()
            .await?
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        entries.sort();

        for entry_name in entries.iter() {
            let log = {
                // Take only the handle, drop the map lock before touching the log itself.
                let map = self.log_map.read().await?;
                match map.get(entry_name) {
                    Some(log) => Arc::clone(log),
                    None => continue, // log might be removed
                }
            };

            let transactions = {
                let log = log.write().await?;
                log.front(self.io_config.batch_max_records).await
            };
            match transactions {
                Ok(vec) => {
                    if vec.is_empty() {
                        continue;
                    }
                    let mut batch = Vec::with_capacity(vec.len());
                    let mut batch_sizes: BTreeMap<u64, u64> = BTreeMap::new();
                    let mut entry_counter = Vec::new();
                    let mut total_size = 0;
                    let mut processed_transactions = 0;
                    for transaction in vec {
                        debug!(
                            "Replicating transaction {}/{}/{:?}",
                            self.settings.src_bucket, entry_name, transaction
                        );

                        let record_to_sync = self.read_record(entry_name, &transaction).await;
                        processed_transactions += 1;

                        match record_to_sync {
                            Ok(record_to_sync) => {
                                let record_size = record_to_sync.meta().content_length();
                                total_size += record_size;
                                batch_sizes.insert(*transaction.timestamp(), record_size);
                                batch.push((record_to_sync, transaction));

                                if total_size >= self.io_config.batch_max_size {
                                    break;
                                }
                            }
                            Err(err) => {
                                error!(
                                    "Failed to read record {}/{}/{}: {:?}",
                                    self.settings.src_bucket,
                                    entry_name,
                                    transaction.timestamp(),
                                    err
                                );
                                entry_counter.push((Err(err), 1, 0));
                            }
                        }
                    }

                    // The batch above was prepared while the previous entry's
                    // send was in flight; join it before spawning the next one.
                    if let Some(handle) = pending_send.take() {
                        if !self.drain_pending(handle, counter).await {
                            return Ok(None);
                        }
                    }

                    if self.ensure_bucket().is_none() {
                        // The remote bucket could not be rebuilt; treated as
                        // unavailable, the transactions stay in the log.
                        return Ok(None);
                    }

                    *pending_send = Some(tokio::spawn(
                        SendBatch {
                            bucket: self.bucket.take().unwrap(),
                            src_bucket: self.settings.src_bucket.clone(),
                            entry_name: entry_name.clone(),
                            dst_entry_name: Self::destination_entry_name(
                                &self.settings.dst_prefix,
                                entry_name,
                            ),
                            batch,
                            batch_sizes,
                            counter: entry_counter,
                            processed_transactions,
                            log,
                        }
                        .send(),
                    ));
                }

                Err(err) => {
                    error!("Failed to read transaction: {:?}", err);
                    return Ok(Some(entry_name.clone()));
                }
            };
        }

        Ok(None)
    }

    /// Join the in-flight send task, merge its accounting into `counter` and
    /// take the remote bucket back. A panicked task loses the bucket, so it
    /// is rebuilt from the factory. Returns whether the bucket is still
    /// active after the send.
    async fn drain_pending(
        &mut self,
        handle: JoinHandle<PendingSend>,
        counter: &mut Vec<ResultResult>,
    ) -> bool {
        match handle.await {
            Ok(PendingSend {
                bucket,
                counter: send_counter,
                bucket_active,
            }) => {
                counter.extend(send_counter);
                self.bucket = Some(bucket);
                bucket_active
            }
            Err(err) => {
                error!("Replication send task failed: {:?}", err);
                match (self.bucket_factory)() {
                    Ok(bucket) => self.bucket = Some(bucket),
                    Err(err) => error!("Failed to rebuild remote bucket: {:?}", err),
                }
                false
            }
        }
    }

    fn ensure_bucket(&mut self) -> Option<&mut Box<dyn RemoteBucket + Send + Sync>> {
        if self.bucket.is_none() {
            match (self.bucket_factory)() {
                Ok(bucket) => self.bucket = Some(bucket),
                Err(err) => error!("Failed to rebuild remote bucket: {:?}", err),
            }
        }
        self.bucket.as_mut()
    }

    async fn read_record(
        &self,
        entry_name: &str,
        transaction: &Transaction,
    ) -> Result<BoxedReadRecord, ReductError> {
        let read_record_from_storage = async || {
            let mut attempts = 3;
            loop {
                let read_record = async || {
                    self.storage
                        .get_bucket(&self.settings.src_bucket)
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
    use std::sync::Mutex;
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
            test_bucket_factory(),
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
        let mut sender = build_sender_with_entries(
            remote_bucket,
            settings,
            &["test", "test2"],
            test_bucket_factory(),
        )
        .await;

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

        let mut sender = build_sender_with_entries(
            remote_bucket,
            settings,
            &["a_ok", "b_fail"],
            test_bucket_factory(),
        )
        .await;
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
    async fn test_send_task_panic_recovers_bucket(settings: ReplicationSettings) {
        let mut panicking_bucket = MockRmBucket::new();
        panicking_bucket
            .expect_write_batch()
            .returning(|_, _| panic!("internal bug in send task"));

        let mut replacement = MockRmBucket::new();
        replacement
            .expect_write_batch()
            .returning(|_, _| Ok(ErrorRecordMap::new()));
        replacement.expect_is_active().return_const(true);

        let replacement = Mutex::new(Some(
            Box::new(replacement) as Box<dyn RemoteBucket + Send + Sync>
        ));
        let factory: BucketFactory = Arc::new(move || {
            replacement
                .lock()
                .unwrap()
                .take()
                .ok_or_else(|| ReductError::internal_server_error("factory already used"))
        });

        let mut sender =
            build_sender_with_entries(panicking_bucket, settings, &["test"], factory).await;
        imitate_write_record_to(&sender, "test", &Transaction::WriteRecord(10), 5).await;

        // The panicked send yields no accounting and must not crash run().
        assert_eq!(sender.run().await.unwrap(), SyncState::NoTransactions);
        assert_eq!(
            log_front(&sender, "test").await,
            vec![Transaction::WriteRecord(10)],
            "transactions are kept when the send task dies"
        );

        // The bucket was rebuilt from the factory, the next pass replicates.
        assert_eq!(
            sender.run().await.unwrap(),
            SyncState::SyncedOrRemoved(vec![(Ok(()), 1, 5)])
        );
        assert!(log_front(&sender, "test").await.is_empty());
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
        let mut sender = build_sender_with_entries(
            remote_bucket,
            settings,
            &["a_ok", "b_bad"],
            test_bucket_factory(),
        )
        .await;

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
        build_sender_with_entries(remote_bucket, settings, &["test"], test_bucket_factory()).await
    }

    async fn build_sender_with_entries(
        remote_bucket: MockRmBucket,
        settings: ReplicationSettings,
        entries: &[&str],
        bucket_factory: BucketFactory,
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
            bucket: Some(Box::new(remote_bucket)),
            bucket_factory,
        }
    }

    fn test_bucket_factory() -> BucketFactory {
        Arc::new(|| {
            Err(ReductError::internal_server_error(
                "no bucket rebuild in tests",
            ))
        })
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
            include: Labels::new(),
            exclude: Labels::new(),
            each_n: None,
            when: None,
            mode: ReplicationMode::Enabled,
        }
    }
}
