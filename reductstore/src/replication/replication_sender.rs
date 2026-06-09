// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::cfg::io::IoConfig;
use crate::replication::remote_bucket::RemoteBucket;
use crate::replication::transaction_log::TransactionLogMap;
use crate::storage::engine::StorageEngine;
use futures_util::{TryStreamExt, StreamExt, future, stream};
use log::{debug, error};
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::io::{BoxedReadRecord, ReadRecord};
use reduct_base::msg::replication_api::ReplicationSettings;
use tokio_retry::RetryIf;
use tokio_retry::strategy::ExponentialBackoff;
use either::Either;
use std::cmp::PartialEq;
use std::sync::Arc;

/// TODO just a guess, someone knowing the context should tune this, also foreshadowing this with global parameter would be nice
pub const DEFAULT_CONCURRENCY_LIMIT: usize = 300;

/// Internal worker for replication to process a sole iteration of the replication loop.
pub(super) struct ReplicationSender {
    log_map: TransactionLogMap,
    storage: Arc<StorageEngine>,
    settings: ReplicationSettings,
    io_config: IoConfig,
    bucket: Box<dyn RemoteBucket + Send + Sync>,
}

type ResultResult = (Result<(), ReductError>, u64);

#[derive(Debug, PartialEq)]
pub(super) enum SyncState {
    SyncedOrRemoved(Vec<ResultResult>),
    NotAvailable(Vec<ResultResult>),
    NoTransactions,
    BrokenLog(String),
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
            bucket,
        }
    }

    pub async fn probe_availability(&mut self) -> bool {
        self.bucket.probe_availability().await;
        self.bucket.is_active()
    }

    pub async fn run(&mut self) -> Result<SyncState, ReductError> {
        let batch_max_size = self.io_config.batch_max_size;
        let batch_max_records = self.io_config.batch_max_records;
        
        let log_map_lock = self
        .log_map
        .read()
        .await?;
        let log_map = log_map_lock.clone();
        drop(log_map_lock);

        let mut counter_aggregated = Vec::with_capacity(log_map.len()); // can be estimated slightly better
        
        let mut buffered = stream::iter(log_map)
        .map(|(entry_name, log)| async move {
            let transactions = log.read().await.map_err(either::Either::Left)?.front(batch_max_records).await.map_err(|err| {
                error!("Failed to read transaction: {err:?}");
                either::Either::Right(SyncState::BrokenLog(entry_name.clone()))
            });
            transactions.map(|transactions| (entry_name, log, transactions))
        }).buffer_unordered(DEFAULT_CONCURRENCY_LIMIT)
        .try_filter(|item| future::ready(!item.2.is_empty()))
        .map_ok(|(entry_name, log, transactions)| {
            let src_bucket = self.settings.src_bucket.clone();
            let storage = self.storage.clone();
            async move {
                let mut counter = Vec::with_capacity(1); // it will have at least `Ok()`
                let entry_name_ref = entry_name.as_str();
                    
                let mut batch = Vec::new();
                let mut total_size = 0;
                let mut processed_transactions = 0;
                for transaction in transactions {
                    debug!(
                        "Replicating transaction {}/{}/{:?}",
                        src_bucket, entry_name_ref, transaction
                    );

                    const STRATEGY: ExponentialBackoff = ExponentialBackoff::from_millis(5).factor(2); // https://github.com/reductstore/reductstore/pull/1419#pullrequestreview-4447002753
                    let record_to_sync = RetryIf::start(
                        STRATEGY.map(tokio_retry::strategy::jitter).take(7), 
                        async || {
                            storage
                            .get_bucket(&src_bucket)
                            .await?
                            .upgrade()?
                            .get_entry(entry_name_ref)
                            .await?
                            .upgrade()?
                            .begin_read(transaction.timestamp()).await
                        }, 
                        |error: &ReductError| error.status == ErrorCode::TooEarly
                    ).await;
                    processed_transactions += 1;

                    match record_to_sync {
                        Ok(record_to_sync) => {
                            let record_size = record_to_sync.meta().content_length();
                            total_size += record_size;
                            batch.push((Box::new(
                                record_to_sync
                            ) as BoxedReadRecord, transaction));

                            if total_size >= batch_max_size {
                                break;
                            }
                        }
                        Err(err) => {
                            error!(
                                "Failed to read record {}/{}/{}: {:?}",
                                src_bucket,
                                entry_name_ref,
                                transaction.timestamp(),
                                err
                            );
                            counter.push((Err(err), 1));
                        }
                    }
                }

                Ok((entry_name, log, batch.len() as u64, batch, counter, processed_transactions))
            }
        }).try_buffer_unordered(DEFAULT_CONCURRENCY_LIMIT);
        while let Some(item) = buffered.next().await  { 
            match item {
                Err(Either::Left(e)) => return Err(e),
                Err(Either::Right(ok)) => return Ok(ok),
                Ok((
                    entry_name, 
                    log, 
                    batch_size,
                    batch,
                    mut counter, 
                    processed_transactions
                )) => {
                    match self.bucket.write_batch(entry_name.as_str(), batch).await {
                        Ok(map) => {
                            counter.push((Ok(()), batch_size - map.len() as u64));
                            for (timestamp, err) in map.into_iter() {
                                debug!(
                                    "Failed to replicate record {}/{}/{}: {:?}",
                                    self.settings.src_bucket, entry_name, timestamp, err
                                );
                                counter.push((Err(err), 1));
                            }
                        }
                        Err(err) => {
                            debug!(
                                "Failed to replicate batch of records from {}/{} {:?}",
                                self.settings.src_bucket, entry_name, err
                            );

                            counter.push((Err(err), batch_size));
                        }
                    }

                    if self.bucket.is_active() {
                        // remove processed transactions from the log
                        if let Err(err) = 
                            log.write().await?.pop_front(processed_transactions).await {
                                error!("Failed to remove transaction: {err:?}");
                            }
                    }
                    
                    counter_aggregated.append(counter.as_mut());
                }
            }
        }

        Ok(if counter_aggregated.is_empty() {
            SyncState::NoTransactions
        } else if self.bucket.is_active() {
            SyncState::SyncedOrRemoved(counter_aggregated)
        } else {
            SyncState::NotAvailable(counter_aggregated)
        })
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
    use mockall::mock;
    use reduct_base::error::ErrorCode;
    use reduct_base::error::ReductError;
    use reduct_base::msg::bucket_api::BucketSettings;
    use reduct_base::msg::replication_api::ReplicationMode;
    use reduct_base::{conflict, not_found, timeout, too_early, Labels};
    use rstest::*;
    use std::collections::HashMap;
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
            SyncState::SyncedOrRemoved(vec![(Ok(()), 1)])
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
            SyncState::NotAvailable(vec![(Err(timeout!("Timeout")), 1)])
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
                (Err(not_found!("Entry 'test' not found in bucket 'src'")), 1),
                (Ok(()), 0)
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
            SyncState::SyncedOrRemoved(vec![(Ok(()), 1)])
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
                    1
                ),
                (Ok(()), 0)
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
            SyncState::SyncedOrRemoved(vec![(Ok(()), 1), (Err(conflict!("AlreadyExists")), 1)])
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
            SyncState::SyncedOrRemoved(vec![(Ok(()), 1)])
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

    async fn imitate_write_record(
        sender: &ReplicationSender,
        transaction: &Transaction,
        size: u64,
    ) {
        let log = {
            let map = sender.log_map.write().await.unwrap();
            map.get("test").unwrap().clone()
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
                "test",
                transaction.timestamp(),
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
        let log: TransactionLogRef = Arc::new(AsyncRwLock::new(
            TransactionLog::try_load_or_create(&storage.data_path().join("test.log"), 1000)
                .await
                .unwrap(),
        ));

        log_map
            .write()
            .await
            .unwrap()
            .insert("test".to_string(), log);

        ReplicationSender {
            log_map,
            storage,
            settings,
            io_config: IoConfig::default(),
            bucket: Box::new(remote_bucket),
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
            dst_token: Some("token".to_string()),
            entries: vec!["test".to_string()],
            include: Labels::new(),
            exclude: Labels::new(),
            each_n: None,
            each_s: None,
            when: None,
            mode: ReplicationMode::Enabled,
        }
    }
}
