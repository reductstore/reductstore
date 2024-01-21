// Copyright 2023-2024 ReductStore
// Licensed under the Business Source License 1.1

use crate::replication::remote_bucket::{create_remote_bucket, RemoteBucket};
use crate::replication::transaction_filter::TransactionFilter;
use crate::replication::transaction_log::TransactionLog;
use crate::replication::TransactionNotification;
use crate::storage::storage::Storage;

use log::{debug, error, info};
use reduct_base::error::ReductError;

use crate::replication::diagnostics::DiagnosticsCounter;
use reduct_base::msg::diagnostics::Diagnostics;
use reduct_base::msg::replication_api::{ReplicationInfo, ReplicationSettings};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use tokio::sync::RwLock;
use tokio::time::sleep;

pub struct Replication {
    name: String,
    is_provisioned: bool,
    settings: ReplicationSettings,
    filter: TransactionFilter,
    log_map: Arc<RwLock<HashMap<String, RwLock<TransactionLog>>>>,
    storage: Arc<RwLock<Storage>>,
    remote_bucket: Arc<RwLock<dyn RemoteBucket + Send + Sync>>,
    hourly_diagnostics: Arc<RwLock<DiagnosticsCounter>>,
}

const TRANSACTION_LOG_SIZE: usize = 1000_000;

impl Replication {
    pub(super) fn new(
        name: String,
        settings: ReplicationSettings,
        storage: Arc<RwLock<Storage>>,
    ) -> Self {
        let ReplicationSettings {
            src_bucket,
            dst_bucket: remote_bucket,
            dst_host: remote_host,
            dst_token: remote_token,
            entries,
            include,
            exclude,
        } = settings.clone();

        let remote_bucket = create_remote_bucket(
            remote_host.as_str(),
            remote_bucket.as_str(),
            remote_token.as_str(),
        );

        let filter = TransactionFilter::new(src_bucket, entries, include, exclude);

        Self::build(name, settings, remote_bucket, filter, storage)
    }

    fn build(
        name: String,
        settings: ReplicationSettings,
        remote_bucket: Arc<RwLock<dyn RemoteBucket + Send + Sync>>,
        filter: TransactionFilter,
        storage: Arc<RwLock<Storage>>,
    ) -> Self {
        let log_map = Arc::new(RwLock::new(HashMap::<String, RwLock<TransactionLog>>::new()));
        let hourly_diagnostics = Arc::new(RwLock::new(DiagnosticsCounter::new(
            Duration::from_secs(3600),
        )));

        let config = settings.clone();
        let replication_name = name.clone();
        let thr_log_map = Arc::clone(&log_map);
        let thr_bucket = Arc::clone(&remote_bucket);
        let thr_storage = Arc::clone(&storage);
        let thr_hourly_diagnostics = Arc::clone(&hourly_diagnostics);

        tokio::spawn(async move {
            loop {
                let mut no_transactions = true;
                for (entry_name, log) in thr_log_map.read().await.iter() {
                    let tr = log.write().await.front().await;
                    match tr {
                        Ok(None) => {}
                        Ok(Some(transaction)) => {
                            no_transactions = false;

                            debug!("Replicating transaction {}/{:?}", entry_name, transaction);

                            let get_record = async {
                                thr_storage
                                    .read()
                                    .await
                                    .get_bucket(&config.src_bucket)?
                                    .begin_read(&entry_name, *transaction.timestamp())
                                    .await
                            };

                            let read_record = match get_record.await {
                                Ok(record) => record,
                                Err(err) => {
                                    log.write().await.pop_front().await.unwrap();
                                    error!("Failed to read record: {}", err);
                                    thr_hourly_diagnostics.write().await.count(Err(err));
                                    break;
                                }
                            };

                            let mut bucket = thr_bucket.write().await;
                            match bucket.write_record(entry_name, read_record).await {
                                Ok(_) => {
                                    if bucket.is_active() {
                                        thr_hourly_diagnostics.write().await.count(Ok(()));
                                    }
                                }
                                Err(err) => {
                                    debug!("Failed to replicate transaction: {:?}", err);
                                    thr_hourly_diagnostics.write().await.count(Err(err));
                                }
                            }

                            if bucket.is_active() {
                                // We keep transactions if the destination bucket is not available
                                log.write().await.pop_front().await.unwrap();
                            } else {
                                drop(bucket); // drop lock
                                sleep(Duration::from_secs(5)).await;
                            }
                        }

                        Err(err) => {
                            error!("Failed to read transaction: {:?}", err);
                            info!("Transaction log is corrupted, dropping the whole log");
                            let mut log = log.write().await;

                            let path = Self::build_path_to_transaction_log(
                                thr_storage.read().await.data_path(),
                                &config.src_bucket,
                                &entry_name,
                                &replication_name,
                            );
                            if let Err(err) = fs::remove_file(&path).await {
                                error!("Failed to remove transaction log: {:?}", err);
                            }

                            info!("Creating a new transaction log");
                            *log = TransactionLog::try_load_or_create(path, TRANSACTION_LOG_SIZE)
                                .await
                                .unwrap();
                        }
                    }
                }

                // NOTE: we don't want to spin the CPU when there is nothing to do
                if no_transactions {
                    sleep(Duration::from_millis(100)).await;
                }
            }
        });

        Self {
            name,
            is_provisioned: false,
            settings,
            storage,
            filter,
            log_map,
            remote_bucket,
            hourly_diagnostics,
        }
    }

    pub async fn notify(&self, notification: TransactionNotification) -> Result<(), ReductError> {
        if !self.filter.filter(&notification) {
            return Ok(());
        }

        // NOTE: very important not to lock the log_map for too long
        // because it is used by the replication thread
        if !self.log_map.read().await.contains_key(&notification.entry) {
            let mut map = self.log_map.write().await;
            map.insert(
                notification.entry.clone(),
                RwLock::new(
                    TransactionLog::try_load_or_create(
                        Self::build_path_to_transaction_log(
                            self.storage.read().await.data_path(),
                            &self.settings.src_bucket,
                            &notification.entry,
                            &self.name,
                        ),
                        TRANSACTION_LOG_SIZE,
                    )
                    .await?,
                ),
            );
        };

        let log_map = self.log_map.read().await;
        let log = log_map.get(&notification.entry).unwrap();

        if let Some(_) = log
            .write()
            .await
            .push_back(notification.event.clone())
            .await?
        {
            error!("Transaction log is full, dropping the oldest transaction without replication");
        }

        Ok(())
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    /// Get the replication settings with the destination token masked.
    pub fn masked_settings(&self) -> ReplicationSettings {
        ReplicationSettings {
            dst_token: "***".to_string(),
            ..self.settings.clone()
        }
    }

    pub fn settings(&self) -> &ReplicationSettings {
        &self.settings
    }

    pub fn is_provisioned(&self) -> bool {
        self.is_provisioned
    }

    pub fn set_provisioned(&mut self, provisioned: bool) {
        self.is_provisioned = provisioned;
    }

    pub async fn info(&self) -> ReplicationInfo {
        let mut pending_records = 0;
        for (_, log) in self.log_map.read().await.iter() {
            pending_records += log.read().await.len() as u64;
        }
        ReplicationInfo {
            name: self.name.clone(),
            is_active: self.remote_bucket.read().await.is_active(),
            is_provisioned: self.is_provisioned,
            pending_records,
        }
    }

    pub async fn diagnostics(&self) -> Diagnostics {
        Diagnostics {
            hourly: self.hourly_diagnostics.read().await.diagnostics(),
        }
    }

    fn build_path_to_transaction_log(
        storage_path: &PathBuf,
        bucket: &str,
        entry: &str,
        name: &str,
    ) -> PathBuf {
        storage_path.join(format!("{}/{}/{}.log", bucket, entry, name))
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
    async fn test_replication_ok_active(
        mut remote_bucket: MockRmBucket,
        notification: TransactionNotification,
    ) {
        remote_bucket.expect_write_record().returning(|_, _| Ok(()));
        remote_bucket.expect_is_active().return_const(true);
        let replication = build_replication(remote_bucket).await;

        replication.notify(notification).await.unwrap();
        assert!(transaction_log_is_empty(&replication).await);
        assert_eq!(
            replication.info().await,
            ReplicationInfo {
                name: "test".to_string(),
                is_active: true,
                is_provisioned: false,
                pending_records: 0,
            }
        );
        assert_eq!(
            replication.diagnostics().await,
            Diagnostics {
                hourly: DiagnosticsItem {
                    ok: 60,
                    errored: 0,
                    errors: HashMap::new(),
                }
            }
        )
    }

    #[rstest]
    #[tokio::test]
    async fn test_replication_ok_not_active(
        mut remote_bucket: MockRmBucket,
        notification: TransactionNotification,
    ) {
        remote_bucket.expect_write_record().returning(|_, _| Ok(()));
        remote_bucket.expect_is_active().return_const(false);
        let replication = build_replication(remote_bucket).await;

        replication.notify(notification).await.unwrap();
        assert!(!transaction_log_is_empty(&replication).await);
        assert_eq!(
            replication.info().await,
            ReplicationInfo {
                name: "test".to_string(),
                is_active: false,
                is_provisioned: false,
                pending_records: 1,
            }
        );
        assert_eq!(
            replication.diagnostics().await,
            Diagnostics {
                hourly: DiagnosticsItem {
                    ok: 0,
                    errored: 0,
                    errors: HashMap::new(),
                }
            },
            "should not count errors for non active replication"
        )
    }

    #[rstest]
    #[tokio::test]
    async fn test_replication_comm_err(
        mut remote_bucket: MockRmBucket,
        notification: TransactionNotification,
    ) {
        remote_bucket
            .expect_write_record()
            .returning(|_, _| Err(ReductError::new(ErrorCode::Timeout, "Timeout")));
        remote_bucket.expect_is_active().return_const(false);
        let replication = build_replication(remote_bucket).await;

        replication.notify(notification).await.unwrap();
        assert!(
            !transaction_log_is_empty(&replication).await,
            "We keep the transaction in the log to sync later"
        );
        assert_eq!(
            replication.info().await,
            ReplicationInfo {
                name: "test".to_string(),
                is_active: false,
                is_provisioned: false,
                pending_records: 1,
            }
        );
        let diagnostics = replication.diagnostics().await.hourly;
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
        mut notification: TransactionNotification,
    ) {
        remote_bucket.expect_write_record().returning(|_, _| Ok(()));
        remote_bucket.expect_is_active().return_const(true);
        let replication = build_replication(remote_bucket).await;

        notification.event = Transaction::WriteRecord(100);
        replication.notify(notification).await.unwrap();
        assert!(
            transaction_log_is_empty(&replication).await,
            "We don't keep the transaction for a non existing record"
        );
        assert_eq!(
            replication.info().await,
            ReplicationInfo {
                name: "test".to_string(),
                is_active: true,
                is_provisioned: false,
                pending_records: 0,
            }
        );
        assert_eq!(
            replication.diagnostics().await,
            Diagnostics {
                hourly: DiagnosticsItem {
                    ok: 0,
                    errored: 60,
                    errors: HashMap::from_iter(vec![(
                        404,
                        DiagnosticsError {
                            count: 1,
                            last_message: "No record with timestamp 100".to_string(),
                        }
                    )]),
                }
            }
        )
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

    async fn build_replication(remote_bucket: MockRmBucket) -> Replication {
        let tmp_dir = tempfile::tempdir().unwrap().into_path();
        let settings = ReplicationSettings {
            src_bucket: "src".to_string(),
            dst_bucket: "remote".to_string(),
            dst_host: "http://localhost:8383".to_string(),
            dst_token: "token".to_string(),
            entries: vec![],
            include: Labels::new(),
            exclude: Labels::new(),
        };

        let filter = TransactionFilter::new(
            settings.src_bucket.clone(),
            settings.entries.clone(),
            settings.include.clone(),
            settings.exclude.clone(),
        );

        let storage = Arc::new(RwLock::new(Storage::new(tmp_dir)));

        {
            let mut lock = storage.write().await;

            let bucket = lock
                .create_bucket("src", BucketSettings::default())
                .unwrap();

            let tx = bucket
                .write_record("test", 10, 4, "text/plain".to_string(), Labels::new())
                .await
                .unwrap();
            tx.send(Ok(Some(Bytes::from("test")))).await.unwrap();
            tx.send(Ok(None)).await.unwrap();
        }

        Replication::build(
            "test".to_string(),
            settings,
            Arc::new(RwLock::new(remote_bucket)),
            filter,
            storage,
        )
    }

    async fn transaction_log_is_empty(replication: &Replication) -> bool {
        sleep(Duration::from_millis(50)).await;
        sleep(Duration::from_millis(50)).await;

        replication
            .log_map
            .read()
            .await
            .get("test")
            .unwrap()
            .read()
            .await
            .is_empty()
    }
}
