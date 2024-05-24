// Copyright 2023-2024 ReductStore
// Licensed under the Business Source License 1.1

use crate::replication::remote_bucket::{create_remote_bucket, RemoteBucket};
use crate::replication::transaction_filter::TransactionFilter;
use crate::replication::transaction_log::TransactionLog;
use crate::replication::{Transaction, TransactionNotification};
use crate::storage::storage::Storage;

use log::{debug, error, info};
use reduct_base::error::{ErrorCode, ReductError};

use crate::replication::diagnostics::DiagnosticsCounter;
use futures_util::stream::Concat;
use reduct_base::msg::diagnostics::Diagnostics;
use reduct_base::msg::replication_api::{ReplicationInfo, ReplicationSettings};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use tokio::sync::RwLock;
use tokio::time::sleep;

pub struct ReplicationTask {
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

impl ReplicationTask {
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
            let init_transaction_logs = async {
                let mut logs = thr_log_map.write().await;
                for entry in thr_storage
                    .read()
                    .await
                    .get_bucket(&config.src_bucket)?
                    .info()
                    .await?
                    .entries
                {
                    let path = Self::build_path_to_transaction_log(
                        thr_storage.read().await.data_path(),
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

            let sender = ReplicationSender {
                replication_name,
                log_map: thr_log_map,
                storage: thr_storage,
                config,
                hourly_diagnostics: thr_hourly_diagnostics,
                bucket: thr_bucket,
                remote_bucket_timeout: Duration::from_secs(5),
            };

            loop {
                // NOTE: we don't want to spin the CPU when there is nothing to do
                if sender.run().await {
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

/// Internal worker for replication to process a sole iteration of the replication loop.
struct ReplicationSender {
    replication_name: String,
    log_map: Arc<RwLock<HashMap<String, RwLock<TransactionLog>>>>,
    storage: Arc<RwLock<Storage>>,
    config: ReplicationSettings,
    hourly_diagnostics: Arc<RwLock<DiagnosticsCounter>>,
    bucket: Arc<RwLock<dyn RemoteBucket + Send + Sync>>,
    remote_bucket_timeout: Duration,
}

impl ReplicationSender {
    pub async fn run(&self) -> bool {
        let mut no_transactions = true;
        for (entry_name, log) in self.log_map.read().await.iter() {
            let tr = log.write().await.front().await;
            match tr {
                Ok(None) => {}
                Ok(Some(transaction)) => {
                    no_transactions = false;

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
                    } else {
                        // We keep transactions if the destination bucket is not available
                        drop(bucket); // drop lock
                        sleep(self.remote_bucket_timeout).await; // FIXME: this is a temporary solution
                    }
                }

                Err(err) => {
                    error!("Failed to read transaction: {:?}", err);
                    info!("Transaction log is corrupted, dropping the whole log");
                    let mut log = log.write().await;

                    let path = ReplicationTask::build_path_to_transaction_log(
                        self.storage.read().await.data_path(),
                        &self.config.src_bucket,
                        &entry_name,
                        &self.replication_name,
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

        return no_transactions;
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

    #[cfg(target_os = "linux")]
    mod replication_task {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_transaction_log_init(
            remote_bucket: MockRmBucket,
            settings: ReplicationSettings,
        ) {
            let replication = build_replication(remote_bucket, settings).await;
            assert_eq!(replication.log_map.read().await.len(), 1);
            assert!(
                replication.log_map.read().await.contains_key("test"),
                "Transaction log is initialized"
            );
        }

        /// TODO check notify method

        async fn build_replication(
            remote_bucket: MockRmBucket,
            settings: ReplicationSettings,
        ) -> ReplicationTask {
            let tmp_dir = tempfile::tempdir().unwrap().into_path();

            let filter = TransactionFilter::new(
                settings.src_bucket.clone(),
                settings.entries.clone(),
                settings.include.clone(),
                settings.exclude.clone(),
            );

            let storage = Arc::new(RwLock::new(Storage::load(tmp_dir, None).await));

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
                tx.send(Ok(None)).await.unwrap_or(());
            }

            let repl = ReplicationTask::build(
                "test".to_string(),
                settings,
                Arc::new(RwLock::new(remote_bucket)),
                filter,
                storage,
            );

            sleep(Duration::from_millis(5)).await; // wait for the transaction log to be initialized in worker
            repl
        }

        #[rstest]
        #[tokio::test]
        async fn test_replication_ok_active(
            mut remote_bucket: MockRmBucket,
            notification: TransactionNotification,
            settings: ReplicationSettings,
        ) {
            remote_bucket.expect_write_record().returning(|_, _| Ok(()));
            remote_bucket.expect_is_active().return_const(true);
            let replication = build_replication(remote_bucket, settings).await;

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
    }

    mod test_replication_sender {
        use super::*;

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

            assert!(!sender.run().await);
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

            assert!(!sender.run().await);

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

            assert!(!sender.run().await);
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
                            last_message: "Record with timestamp 20 is still being written"
                                .to_string()
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
                    TRANSACTION_LOG_SIZE,
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
                remote_bucket_timeout: Duration::from_millis(10),
            }
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

    async fn transaction_log_is_empty(replication: &ReplicationTask) -> bool {
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
