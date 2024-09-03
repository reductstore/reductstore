// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use log::{error, info};
use tokio::fs;
use tokio::sync::RwLock;
use tokio::time::sleep;

use reduct_base::error::ReductError;
use reduct_base::msg::diagnostics::Diagnostics;
use reduct_base::msg::replication_api::{ReplicationInfo, ReplicationSettings};

use crate::replication::diagnostics::DiagnosticsCounter;
use crate::replication::remote_bucket::{create_remote_bucket, RemoteBucket};
use crate::replication::replication_sender::{ReplicationSender, SyncState};
use crate::replication::transaction_filter::TransactionFilter;
use crate::replication::transaction_log::TransactionLog;
use crate::replication::TransactionNotification;
use crate::storage::storage::Storage;

pub struct ReplicationTask {
    name: String,
    is_provisioned: bool,
    settings: ReplicationSettings,
    filter_map: Arc<RwLock<HashMap<String, TransactionFilter>>>,
    log_map: Arc<RwLock<HashMap<String, RwLock<TransactionLog>>>>,
    storage: Arc<RwLock<Storage>>,
    remote_bucket: Arc<RwLock<dyn RemoteBucket + Send + Sync>>,
    hourly_diagnostics: Arc<RwLock<DiagnosticsCounter>>,
}

const TRANSACTION_LOG_SIZE: usize = 1000_000;

impl ReplicationTask {
    /// Create a new replication task.
    pub(super) fn new(
        name: String,
        settings: ReplicationSettings,
        storage: Arc<RwLock<Storage>>,
    ) -> Self {
        let ReplicationSettings {
            dst_bucket: remote_bucket,
            dst_host: remote_host,
            dst_token: remote_token,
            ..
        } = settings.clone();

        let remote_bucket = create_remote_bucket(
            remote_host.as_str(),
            remote_bucket.as_str(),
            remote_token.as_str(),
        );

        Self::build(
            name,
            settings,
            remote_bucket,
            Arc::new(RwLock::new(HashMap::new())),
            storage,
        )
    }

    fn build(
        name: String,
        settings: ReplicationSettings,
        remote_bucket: Arc<RwLock<dyn RemoteBucket + Send + Sync>>,
        filter: Arc<RwLock<HashMap<String, TransactionFilter>>>,
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
            let init_transaction_logs = async {
                let mut logs = thr_log_map.write().await;
                let storage = thr_storage.read().await;
                for entry in storage
                    .get_bucket(&config.src_bucket)?
                    .info()
                    .await?
                    .entries
                {
                    let path = Self::build_path_to_transaction_log(
                        storage.data_path(),
                        &config.src_bucket,
                        &entry.name,
                        &replication_name,
                    );
                    let log = TransactionLog::try_load_or_create(path, TRANSACTION_LOG_SIZE)
                        .await
                        .unwrap();
                    logs.insert(entry.name, RwLock::new(log));
                }

                Ok::<(), ReductError>(())
            };

            if let Err(err) = init_transaction_logs.await {
                error!("Failed to initialize transaction logs: {:?}", err);
            }

            let sender = ReplicationSender::new(
                thr_log_map.clone(),
                thr_storage.clone(),
                config.clone(),
                thr_hourly_diagnostics,
                thr_bucket,
            );

            loop {
                match sender.run().await {
                    SyncState::SyncedOrRemoved => {}
                    SyncState::NotAvailable => {
                        sleep(Duration::from_secs(5)).await;
                    }
                    SyncState::NoTransactions => {
                        // NOTE: we don't want to spin the CPU when there is nothing to do or the bucket is not available
                        sleep(Duration::from_millis(250)).await;
                    }
                    SyncState::BrokenLog(entry_name) => {
                        info!("Transaction log is corrupted, dropping the whole log");
                        let path = ReplicationTask::build_path_to_transaction_log(
                            thr_storage.read().await.data_path(),
                            &config.src_bucket,
                            &entry_name,
                            &replication_name,
                        );
                        if let Err(err) = fs::remove_file(&path).await {
                            error!("Failed to remove transaction log: {:?}", err);
                        }

                        info!("Creating a new transaction log");
                        thr_log_map.write().await.insert(
                            entry_name,
                            RwLock::new(
                                TransactionLog::try_load_or_create(path, TRANSACTION_LOG_SIZE)
                                    .await
                                    .unwrap(),
                            ),
                        );
                    }
                }
            }
        });

        Self {
            name,
            is_provisioned: false,
            settings,
            storage,
            filter_map: filter,
            log_map,
            remote_bucket,
            hourly_diagnostics,
        }
    }

    pub async fn notify(
        &mut self,
        notification: TransactionNotification,
    ) -> Result<(), ReductError> {
        // We need to have a filter for each entry
        {
            let mut lock = self.filter_map.write().await;
            if !lock.contains_key(&notification.entry) {
                lock.insert(
                    notification.entry.clone(),
                    TransactionFilter::new(self.settings.clone()),
                );
            }

            let filter = lock.get_mut(&notification.entry).unwrap();
            if !filter.filter(&notification) {
                return Ok(());
            }
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

#[cfg(target_os = "linux")]
#[cfg(test)]
mod tests {
    use super::*;

    use async_trait::async_trait;

    use bytes::Bytes;

    use mockall::mock;
    use rstest::*;

    use crate::replication::remote_bucket::ErrorRecordMap;
    use crate::storage::entry::io::record_reader::RecordReader;
    use reduct_base::msg::bucket_api::BucketSettings;
    use reduct_base::msg::diagnostics::DiagnosticsItem;
    use reduct_base::Labels;

    use crate::replication::Transaction;

    mock! {
        RmBucket {}

        #[async_trait]
        impl RemoteBucket for RmBucket {
            async fn write_batch(
                &mut self,
                entry_name: &str,
                record: Vec<(RecordReader, Transaction)>,
            ) -> Result<ErrorRecordMap, ReductError>;

            fn is_active(&self) -> bool;
        }

    }

    #[rstest]
    #[tokio::test]
    async fn test_transaction_log_init(remote_bucket: MockRmBucket, settings: ReplicationSettings) {
        let replication = build_replication(remote_bucket, settings).await;
        assert_eq!(replication.log_map.read().await.len(), 2);
        assert!(
            replication.log_map.read().await.contains_key("test1"),
            "Transaction log is initialized"
        );
        assert!(
            replication.log_map.read().await.contains_key("test2"),
            "Transaction log is initialized"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_replication_ok_active(
        mut remote_bucket: MockRmBucket,
        notification: TransactionNotification,
        settings: ReplicationSettings,
    ) {
        remote_bucket
            .expect_write_batch()
            .returning(|_, _| Ok(ErrorRecordMap::new()));
        remote_bucket.expect_is_active().return_const(true);
        let mut replication = build_replication(remote_bucket, settings).await;

        replication.notify(notification).await.unwrap();
        sleep(Duration::from_millis(250)).await;
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
    async fn test_replication_filter_each_entry(
        mut notification: TransactionNotification,
        mut remote_bucket: MockRmBucket,
        settings: ReplicationSettings,
    ) {
        remote_bucket
            .expect_write_batch()
            .return_const(Ok(ErrorRecordMap::new()));

        let settings = ReplicationSettings {
            each_n: Some(2),
            ..settings
        };

        let mut replication = build_replication(MockRmBucket::new(), settings.clone()).await;

        let mut time = 10;
        for entry in &["test1", "test2"] {
            for _ in 0..3 {
                notification.entry = entry.to_string();
                notification.event = Transaction::WriteRecord(time.clone());
                replication.notify(notification.clone()).await.unwrap();
                time += 10;
            }
        }

        assert_eq!(replication.log_map.read().await.len(), 2);
        assert_eq!(
            get_entries_from_transaction_log(&mut replication, "test1").await,
            vec![Transaction::WriteRecord(10), Transaction::WriteRecord(30)]
        );
        assert_eq!(
            get_entries_from_transaction_log(&mut replication, "test2").await,
            vec![Transaction::WriteRecord(40), Transaction::WriteRecord(60)]
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_broken_transaction_log(
        mut remote_bucket: MockRmBucket,
        notification: TransactionNotification,
        settings: ReplicationSettings,
    ) {
        remote_bucket
            .expect_write_batch()
            .return_const(Ok(ErrorRecordMap::new()));
        remote_bucket.expect_is_active().return_const(true);
        let mut replication = build_replication(remote_bucket, settings.clone()).await;

        replication.notify(notification.clone()).await.unwrap();
        assert!(!transaction_log_is_empty(&replication).await);

        let path = ReplicationTask::build_path_to_transaction_log(
            replication.storage.read().await.data_path(),
            &settings.src_bucket,
            &notification.entry,
            &replication.name,
        );
        fs::write(path.clone(), "broken").await.unwrap();
        sleep(Duration::from_millis(500)).await;

        assert_eq!(
            get_entries_from_transaction_log(&mut replication, "test1").await,
            vec![],
            "Transaction log is empty"
        );
    }

    async fn build_replication(
        remote_bucket: MockRmBucket,
        settings: ReplicationSettings,
    ) -> ReplicationTask {
        let tmp_dir = tempfile::tempdir().unwrap().into_path();

        let storage = Arc::new(RwLock::new(Storage::load(tmp_dir, None).await));

        {
            let mut lock = storage.write().await;

            let bucket = lock
                .create_bucket("src", BucketSettings::default())
                .unwrap();

            let mut time = 10;
            for entry in ["test1", "test2"] {
                for _ in 0..3 {
                    let mut writer = bucket
                        .write_record(entry, time, 4, "text/plain".to_string(), Labels::new())
                        .await
                        .unwrap();
                    writer
                        .tx()
                        .send(Ok(Some(Bytes::from("test"))))
                        .await
                        .unwrap();
                    writer.tx().send(Ok(None)).await.unwrap_or(());
                    writer.tx().closed().await;
                    time += 10;
                }

                time += 10;
            }
        }

        let repl = ReplicationTask::build(
            "test".to_string(),
            settings,
            Arc::new(RwLock::new(remote_bucket)),
            Arc::new(RwLock::new(HashMap::new())),
            storage,
        );

        sleep(Duration::from_millis(10)).await; // wait for the transaction log to be initialized in worker
        repl
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
            entry: "test1".to_string(),
            labels: Vec::new(),
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
            entries: vec!["test1".to_string(), "test2".to_string()],
            include: Labels::new(),
            exclude: Labels::new(),
            each_n: None,
            each_s: None,
        }
    }

    async fn transaction_log_is_empty(replication: &ReplicationTask) -> bool {
        sleep(Duration::from_millis(50)).await;
        sleep(Duration::from_millis(50)).await;

        replication
            .log_map
            .read()
            .await
            .get("test1")
            .unwrap()
            .read()
            .await
            .is_empty()
    }

    async fn get_entries_from_transaction_log(
        replication: &mut ReplicationTask,
        entry: &str,
    ) -> Vec<Transaction> {
        replication
            .log_map
            .read()
            .await
            .get(entry)
            .unwrap()
            .read()
            .await
            .front(10)
            .await
            .unwrap()
    }
}
