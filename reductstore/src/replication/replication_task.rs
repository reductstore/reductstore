// Copyright 2023-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::{sleep, spawn};
use std::time::Duration;

use log::{error, info};

use crate::cfg::io::IoConfig;
use crate::cfg::replication::ReplicationConfig;
use crate::cfg::Cfg;
use crate::core::file_cache::FILE_CACHE;
use crate::replication::diagnostics::DiagnosticsCounter;
use crate::replication::remote_bucket::{create_remote_bucket, RemoteBucket};
use crate::replication::replication_sender::{ReplicationSender, SyncState};
use crate::replication::transaction_filter::TransactionFilter;
use crate::replication::transaction_log::TransactionLog;
use crate::replication::TransactionNotification;
use crate::storage::query::filters::WhenFilter;
use crate::storage::storage::Storage;
use reduct_base::error::ReductError;
use reduct_base::msg::diagnostics::Diagnostics;
use reduct_base::msg::replication_api::{ReplicationInfo, ReplicationSettings};

#[derive(Clone)]
struct ReplicationSystemOptions {
    transaction_log_size: usize, // in records
    remote_bucket_unavailable_timeout: Duration,
    next_transaction_timeout: Duration,
    log_recovery_timeout: Duration,
}

pub struct ReplicationTask {
    name: String,
    is_provisioned: bool,
    settings: ReplicationSettings,
    system_options: ReplicationSystemOptions,
    io_config: IoConfig,
    filter_map: Arc<RwLock<HashMap<String, TransactionFilter>>>,
    log_map: Arc<RwLock<HashMap<String, RwLock<TransactionLog>>>>,
    storage: Arc<Storage>,
    hourly_diagnostics: Arc<RwLock<DiagnosticsCounter>>,
    stop_flag: Arc<AtomicBool>,
    is_active: Arc<AtomicBool>,
}

impl Default for ReplicationSystemOptions {
    fn default() -> Self {
        Self {
            transaction_log_size: 1000_000,
            remote_bucket_unavailable_timeout: Duration::from_secs(5),
            next_transaction_timeout: Duration::from_millis(250),
            log_recovery_timeout: Duration::from_secs(10),
        }
    }
}

impl ReplicationTask {
    /// Create a new replication task.
    pub(super) fn new(
        name: String,
        settings: ReplicationSettings,
        config: Cfg,
        storage: Arc<Storage>,
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

        let system_options = ReplicationSystemOptions {
            transaction_log_size: config.replication_conf.replication_log_size,
            remote_bucket_unavailable_timeout: config.replication_conf.connection_timeout.clone(),
            ..Default::default()
        };

        Self::build(
            name,
            settings,
            system_options,
            config.io_conf,
            remote_bucket,
            Arc::new(RwLock::new(HashMap::new())),
            storage,
        )
    }

    fn build(
        name: String,
        settings: ReplicationSettings,
        system_options: ReplicationSystemOptions,
        io_config: IoConfig,
        remote_bucket: Arc<RwLock<dyn RemoteBucket + Send + Sync>>,
        filter: Arc<RwLock<HashMap<String, TransactionFilter>>>,
        storage: Arc<Storage>,
    ) -> Self {
        let log_map = Arc::new(RwLock::new(HashMap::<String, RwLock<TransactionLog>>::new()));
        let hourly_diagnostics = Arc::new(RwLock::new(DiagnosticsCounter::new(
            Duration::from_secs(3600),
        )));
        let stop_flag = Arc::new(AtomicBool::new(false));
        let is_active = Arc::new(AtomicBool::new(true));

        let replication_name = name.clone();
        let thr_settings = settings.clone();
        let thr_io_config = io_config.clone();
        let thr_log_map = Arc::clone(&log_map);
        let thr_storage = Arc::clone(&storage);
        let thr_hourly_diagnostics = Arc::clone(&hourly_diagnostics);
        let thr_system_options = system_options.clone();
        let thr_stop_flag = Arc::clone(&stop_flag);
        let thr_is_active = Arc::clone(&is_active);

        spawn(move || {
            let init_transaction_logs = || {
                let mut logs = thr_log_map.write()?;
                for entry in thr_storage
                    .get_bucket(&thr_settings.src_bucket)?
                    .upgrade()?
                    .info()?
                    .entries
                {
                    let path = Self::build_path_to_transaction_log(
                        thr_storage.data_path(),
                        &thr_settings.src_bucket,
                        &entry.name,
                        &replication_name,
                    );
                    let log = match TransactionLog::try_load_or_create(
                        &path,
                        thr_system_options.transaction_log_size,
                    ) {
                        Ok(log) => log,
                        Err(err) => {
                            error!(
                                "Failed to load transaction log for entry '{}': {:?}",
                                entry.name, err
                            );
                            info!("Creating a new transaction log for entry '{}'", entry.name);
                            FILE_CACHE.remove(&path)?;
                            TransactionLog::try_load_or_create(
                                &path,
                                thr_system_options.transaction_log_size,
                            )?
                        }
                    };

                    logs.insert(entry.name, RwLock::new(log));
                }

                Ok::<(), ReductError>(())
            };

            if let Err(err) = init_transaction_logs() {
                error!("Failed to initialize transaction logs: {:?}", err);
            }

            let sender = ReplicationSender::new(
                thr_log_map.clone(),
                thr_storage.clone(),
                thr_settings.clone(),
                thr_io_config.clone(),
                thr_hourly_diagnostics,
                remote_bucket,
            );

            while !thr_stop_flag.load(Ordering::Relaxed) {
                match sender.run() {
                    SyncState::SyncedOrRemoved => {
                        thr_is_active.store(true, Ordering::Relaxed);
                    }
                    SyncState::NotAvailable => {
                        thr_is_active.store(false, Ordering::Relaxed);
                        sleep(thr_system_options.remote_bucket_unavailable_timeout);
                    }
                    SyncState::NoTransactions => {
                        // NOTE: we don't want to spin the CPU when there is nothing to do or the bucket is not available
                        thr_is_active.store(true, Ordering::Relaxed);
                        sleep(thr_system_options.next_transaction_timeout);
                    }
                    SyncState::BrokenLog(entry_name) => {
                        thr_is_active.store(false, Ordering::Relaxed);

                        info!("Transaction log is corrupted, dropping the whole log");
                        let path = ReplicationTask::build_path_to_transaction_log(
                            thr_storage.data_path(),
                            &thr_settings.src_bucket,
                            &entry_name,
                            &replication_name,
                        );
                        if let Err(err) = FILE_CACHE.remove(&path) {
                            error!("Failed to remove transaction log: {:?}", err);
                        }

                        info!("Creating a new transaction log: {:?}", path);
                        match TransactionLog::try_load_or_create(
                            &path,
                            thr_system_options.transaction_log_size,
                        ) {
                            Ok(log) => {
                                thr_log_map
                                    .write()
                                    .unwrap()
                                    .insert(entry_name, RwLock::new(log));
                            }

                            Err(err) => {
                                error!("Failed to create transaction log: {:?}", err);
                                sleep(thr_system_options.log_recovery_timeout);
                            }
                        }
                    }
                }
            }
        });

        Self {
            name,
            is_provisioned: false,
            settings,
            system_options,
            io_config,
            storage,
            filter_map: filter,
            log_map,
            hourly_diagnostics,
            stop_flag,
            is_active,
        }
    }

    pub fn notify(&mut self, notification: TransactionNotification) -> Result<(), ReductError> {
        // We need to have a filter for each entry
        let entry_name = notification.entry.clone();
        let notifications = {
            let mut lock = self.filter_map.write()?;
            if !lock.contains_key(&notification.entry) {
                lock.insert(
                    notification.entry.clone(),
                    TransactionFilter::try_new(
                        self.name(),
                        self.settings.clone(),
                        self.io_config.clone(),
                    )?,
                );
            }

            let filter = lock.get_mut(&entry_name).unwrap();
            filter.filter(notification)
        };

        // NOTE: very important not to lock the log_map for too long
        // because it is used by the replication thread
        if !self.log_map.read()?.contains_key(&entry_name) {
            let mut map = self.log_map.write()?;
            map.insert(
                entry_name.clone(),
                RwLock::new(TransactionLog::try_load_or_create(
                    &Self::build_path_to_transaction_log(
                        self.storage.data_path(),
                        &self.settings.src_bucket,
                        &entry_name,
                        &self.name,
                    ),
                    self.system_options.transaction_log_size,
                )?),
            );
        };

        let log_map = self.log_map.read()?;
        let log = log_map.get(&entry_name).unwrap();

        for notification in notifications.into_iter() {
            if let Some(_) = log.write()?.push_back(notification.event)? {
                error!(
                    "Transaction log is full, dropping the oldest transaction without replication"
                );
            }
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

    pub fn info(&self) -> ReplicationInfo {
        let mut pending_records = 0;
        for (_, log) in self.log_map.read().unwrap().iter() {
            pending_records += log.read().unwrap().len() as u64;
        }

        ReplicationInfo {
            name: self.name.clone(),
            is_active: self.is_active.load(Ordering::Relaxed),
            is_provisioned: self.is_provisioned,
            pending_records,
        }
    }

    pub fn diagnostics(&self) -> Diagnostics {
        Diagnostics {
            hourly: self.hourly_diagnostics.read().unwrap().diagnostics(),
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

impl Drop for ReplicationTask {
    fn drop(&mut self) {
        // stop the replication thread
        self.stop_flag.store(true, Ordering::Relaxed);
    }
}

#[cfg(target_os = "linux")]
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::fs;
    use std::io::Write;

    use crate::core::file_cache::FILE_CACHE;
    use mockall::mock;
    use reduct_base::io::{BoxedReadRecord, RecordMeta};
    use rstest::*;

    use crate::replication::remote_bucket::ErrorRecordMap;
    use crate::replication::Transaction;

    use crate::storage::bucket::Bucket;
    use reduct_base::msg::bucket_api::BucketSettings;
    use reduct_base::msg::diagnostics::DiagnosticsItem;
    use reduct_base::Labels;

    mock! {
        RmBucket {}

        impl RemoteBucket for RmBucket {
            fn write_batch(
                &mut self,
                entry_name: &str,
                record: Vec<(BoxedReadRecord, Transaction)>,
            ) -> Result<ErrorRecordMap, ReductError>;

            fn is_active(&self) -> bool;
        }

    }

    #[rstest]
    fn test_transaction_log_init(
        remote_bucket: MockRmBucket,
        settings: ReplicationSettings,
        path: PathBuf,
    ) {
        let replication = build_replication(path, remote_bucket, settings);
        assert_eq!(replication.log_map.read().unwrap().len(), 2);
        assert!(
            replication.log_map.read().unwrap().contains_key("test1"),
            "Transaction log is initialized"
        );
        assert!(
            replication.log_map.read().unwrap().contains_key("test2"),
            "Transaction log is initialized"
        );
    }

    #[rstest]
    fn test_transaction_log_init_err(
        remote_bucket: MockRmBucket,
        settings: ReplicationSettings,
        path: PathBuf,
    ) {
        {
            // create a broken transaction log
            let log_path = ReplicationTask::build_path_to_transaction_log(
                &path,
                &settings.src_bucket,
                "test1",
                &"test".to_string(),
            );

            // create bucket to avoid error on loading entries
            FILE_CACHE
                .remove_dir(&path.join(&settings.src_bucket))
                .unwrap();
            Bucket::new(
                &settings.src_bucket,
                &path,
                BucketSettings::default(),
                Cfg::default(),
            )
            .unwrap();

            fs::create_dir_all(log_path.parent().unwrap()).unwrap();
            let mut log_file = fs::File::create(&log_path).unwrap();

            log_file
                .write_all(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
                .unwrap();
        }

        let replication = build_replication(path, remote_bucket, settings);
        assert_eq!(replication.log_map.read().unwrap().len(), 2);
        let log_len = replication
            .log_map
            .read()
            .unwrap()
            .get("test1")
            .unwrap()
            .read()
            .unwrap()
            .len();
        assert_eq!(
            log_len, 0,
            "Task recreated a new transaction log for 'test1' after broken log"
        );
    }

    #[rstest]
    fn test_add_new_entry(
        mut remote_bucket: MockRmBucket,
        mut notification: TransactionNotification,
        settings: ReplicationSettings,
        path: PathBuf,
    ) {
        remote_bucket
            .expect_write_batch()
            .returning(|_, _| Ok(ErrorRecordMap::new()));
        remote_bucket.expect_is_active().return_const(true);
        let mut replication = build_replication(path, remote_bucket, settings.clone());

        notification.entry = "new_entry".to_string();
        fs::create_dir_all(
            replication
                .storage
                .data_path()
                .join(settings.src_bucket)
                .join("new_entry"),
        )
        .unwrap();

        replication.notify(notification).unwrap();
        sleep(Duration::from_millis(100));
        assert!(transaction_log_is_empty(&replication));
        assert_eq!(
            replication.info(),
            ReplicationInfo {
                name: "test".to_string(),
                is_active: true,
                is_provisioned: false,
                pending_records: 0,
            }
        );
    }

    #[rstest]
    fn test_replication_ok_active(
        mut remote_bucket: MockRmBucket,
        notification: TransactionNotification,
        settings: ReplicationSettings,
        path: PathBuf,
    ) {
        remote_bucket
            .expect_write_batch()
            .returning(|_, _| Ok(ErrorRecordMap::new()));
        remote_bucket.expect_is_active().return_const(true);
        let mut replication = build_replication(path, remote_bucket, settings);

        replication.notify(notification).unwrap();
        sleep(Duration::from_millis(100));
        assert!(transaction_log_is_empty(&replication));
        assert_eq!(
            replication.info(),
            ReplicationInfo {
                name: "test".to_string(),
                is_active: true,
                is_provisioned: false,
                pending_records: 0,
            }
        );
        assert_eq!(
            replication.diagnostics(),
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
    fn test_replication_inactive(
        mut remote_bucket: MockRmBucket,
        notification: TransactionNotification,
        settings: ReplicationSettings,
        path: PathBuf,
    ) {
        remote_bucket
            .expect_write_batch()
            .returning(|_, _| Ok(ErrorRecordMap::new()));
        remote_bucket.expect_is_active().return_const(false);
        let mut replication = build_replication(path, remote_bucket, settings);

        replication.notify(notification).unwrap();
        sleep(Duration::from_millis(100));
        assert!(!transaction_log_is_empty(&replication));
        assert_eq!(
            replication.info(),
            ReplicationInfo {
                name: "test".to_string(),
                is_active: false,
                is_provisioned: false,
                pending_records: 1,
            }
        );
        assert_eq!(
            replication.diagnostics(),
            Diagnostics {
                hourly: DiagnosticsItem {
                    ok: 0,
                    errored: 0,
                    errors: HashMap::new(),
                }
            }
        )
    }

    #[rstest]
    fn test_replication_filter_each_entry(
        mut notification: TransactionNotification,
        mut remote_bucket: MockRmBucket,
        settings: ReplicationSettings,
        path: PathBuf,
    ) {
        remote_bucket
            .expect_write_batch()
            .return_const(Ok(ErrorRecordMap::new()));

        let settings = ReplicationSettings {
            each_n: Some(2),
            ..settings
        };

        let mut replication = build_replication(path, MockRmBucket::new(), settings.clone());

        let mut time = 10;
        for entry in &["test1", "test2"] {
            for _ in 0..3 {
                notification.entry = entry.to_string();
                notification.event = Transaction::WriteRecord(time.clone());
                replication.notify(notification.clone()).unwrap();
                time += 10;
            }
        }

        assert_eq!(replication.log_map.read().unwrap().len(), 2);
        assert_eq!(
            get_entries_from_transaction_log(&mut replication, "test1"),
            vec![Transaction::WriteRecord(10), Transaction::WriteRecord(30)]
        );
        assert_eq!(
            get_entries_from_transaction_log(&mut replication, "test2"),
            vec![Transaction::WriteRecord(40), Transaction::WriteRecord(60)]
        );
    }

    #[rstest]
    fn test_broken_transaction_log(
        mut remote_bucket: MockRmBucket,
        notification: TransactionNotification,
        settings: ReplicationSettings,
        path: PathBuf,
    ) {
        remote_bucket
            .expect_write_batch()
            .return_const(Ok(ErrorRecordMap::new()));
        remote_bucket.expect_is_active().return_const(true);
        let mut replication = build_replication(path, remote_bucket, settings.clone());
        replication.notify(notification.clone()).unwrap();

        let path = ReplicationTask::build_path_to_transaction_log(
            replication.storage.data_path(),
            &settings.src_bucket,
            &notification.entry,
            &replication.name,
        );
        fs::write(path.clone(), "broken").unwrap();
        sleep(Duration::from_millis(100));

        assert_eq!(
            get_entries_from_transaction_log(&mut replication, "test1"),
            vec![],
            "Transaction log is empty"
        );
    }

    #[rstest]
    fn test_broken_transaction_log_failed_recover(
        mut remote_bucket: MockRmBucket,
        notification: TransactionNotification,
        settings: ReplicationSettings,
        path: PathBuf,
    ) {
        remote_bucket
            .expect_write_batch()
            .return_const(Ok(ErrorRecordMap::new()));
        remote_bucket.expect_is_active().return_const(true);
        let mut replication = build_replication(path, remote_bucket, settings.clone());
        replication.notify(notification.clone()).unwrap();

        let path = ReplicationTask::build_path_to_transaction_log(
            replication.storage.data_path(),
            &settings.src_bucket,
            &notification.entry,
            &replication.name,
        );

        FILE_CACHE
            .remove_dir(&path.parent().unwrap().parent().unwrap().to_path_buf())
            .unwrap();
        sleep(Duration::from_millis(100));

        assert!(
            !path.exists(),
            "We could not recover the transaction log, it was removed. However, the replication should continue"
        );

        fs::create_dir_all(path.parent().unwrap()).unwrap();
        sleep(Duration::from_millis(200));

        assert_eq!(
            get_entries_from_transaction_log(&mut replication, "test1"),
            vec![],
            "Transaction log recovered"
        );
    }

    fn build_replication(
        path: PathBuf,
        remote_bucket: MockRmBucket,
        settings: ReplicationSettings,
    ) -> ReplicationTask {
        let cfg = Cfg {
            data_path: path.clone(),
            ..Default::default()
        };

        let storage = Arc::new(Storage::load(cfg, None));

        let bucket = match storage.get_bucket(&settings.src_bucket) {
            Ok(bucket) => bucket.upgrade().unwrap(),
            Err(_) => storage
                .create_bucket("src", BucketSettings::default())
                .unwrap()
                .upgrade_and_unwrap(),
        };

        let mut time = 10;
        for entry in ["test1", "test2"] {
            for _ in 0..3 {
                let mut writer = bucket
                    .begin_write(entry, time, 4, "text/plain".to_string(), Labels::new())
                    .wait()
                    .unwrap();
                writer.blocking_send(Ok(Some(Bytes::from("test")))).unwrap();
                writer.blocking_send(Ok(None)).unwrap_or(());
                time += 10;
            }

            time += 10;
        }

        let repl = ReplicationTask::build(
            "test".to_string(),
            settings,
            ReplicationSystemOptions {
                transaction_log_size: 1000,
                remote_bucket_unavailable_timeout: Duration::from_secs(5),
                next_transaction_timeout: Duration::from_millis(50),
                log_recovery_timeout: Duration::from_millis(100),
            },
            IoConfig::default(),
            Arc::new(RwLock::new(remote_bucket)),
            Arc::new(RwLock::new(HashMap::new())),
            storage,
        );

        sleep(Duration::from_millis(10)); // wait for the transaction log to be initialized in worker
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
            meta: RecordMeta::builder().timestamp(10).build(),
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
            when: None,
        }
    }

    #[fixture]
    fn path() -> PathBuf {
        tempfile::tempdir().unwrap().keep()
    }

    fn transaction_log_is_empty(replication: &ReplicationTask) -> bool {
        sleep(Duration::from_millis(50));
        sleep(Duration::from_millis(50));

        replication
            .log_map
            .read()
            .unwrap()
            .get("test1")
            .unwrap()
            .read()
            .unwrap()
            .is_empty()
    }

    fn get_entries_from_transaction_log(
        replication: &mut ReplicationTask,
        entry: &str,
    ) -> Vec<Transaction> {
        replication
            .log_map
            .read()
            .unwrap()
            .get(entry)
            .unwrap()
            .read()
            .unwrap()
            .front(10)
            .unwrap()
    }
}
