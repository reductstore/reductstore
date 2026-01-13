// Copyright 2023-2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::cfg::io::IoConfig;
use crate::cfg::Cfg;
use crate::core::file_cache::FILE_CACHE;
use crate::core::sync::AsyncRwLock;
use crate::replication::diagnostics::DiagnosticsCounter;
use crate::replication::remote_bucket::{create_remote_bucket, RemoteBucket};
use crate::replication::replication_sender::{ReplicationSender, SyncState};
use crate::replication::transaction_filter::TransactionFilter;
use crate::replication::transaction_log::{TransactionLog, TransactionLogMap, TransactionLogRef};
use crate::replication::TransactionNotification;
use crate::storage::engine::StorageEngine;
use log::{error, info};
use reduct_base::error::ReductError;
use reduct_base::msg::diagnostics::Diagnostics;
use reduct_base::msg::replication_api::{ReplicationInfo, ReplicationMode, ReplicationSettings};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

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
    filter_map: HashMap<String, TransactionFilter>,
    log_map: TransactionLogMap,
    storage: Arc<StorageEngine>,
    hourly_diagnostics: Arc<AsyncRwLock<DiagnosticsCounter>>,
    stop_flag: Arc<AtomicBool>,
    is_active: Arc<AtomicBool>,
    mode: Arc<AtomicU8>,
    worker_handle: Option<JoinHandle<()>>,
    worker_bucket: Option<Box<dyn RemoteBucket + Send + Sync>>,
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
        storage: Arc<StorageEngine>,
    ) -> Self {
        let ReplicationSettings {
            dst_bucket: remote_bucket,
            dst_host: remote_host,
            dst_token: remote_token,
            ..
        } = settings.clone();

        let remote_bucket =
            create_remote_bucket(remote_host.as_str(), remote_bucket.as_str(), remote_token);

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
            storage,
        )
    }

    fn build(
        name: String,
        settings: ReplicationSettings,
        system_options: ReplicationSystemOptions,
        io_config: IoConfig,
        remote_bucket: Box<dyn RemoteBucket + Send + Sync>,
        storage: Arc<StorageEngine>,
    ) -> Self {
        let log_map: TransactionLogMap =
            Arc::new(AsyncRwLock::new(HashMap::<String, TransactionLogRef>::new()));
        let hourly_diagnostics = Arc::new(AsyncRwLock::new(DiagnosticsCounter::new(
            Duration::from_secs(3600),
        )));
        let stop_flag = Arc::new(AtomicBool::new(false));
        let mode = Arc::new(AtomicU8::new(settings.mode as u8));
        let is_active = Arc::new(AtomicBool::new(matches!(
            ReplicationTask::load_mode_from(&mode),
            ReplicationMode::Enabled
        )));
        Self {
            name,
            is_provisioned: false,
            settings,
            system_options,
            io_config,
            storage,
            filter_map: HashMap::new(),
            log_map,
            hourly_diagnostics,
            stop_flag,
            is_active,
            mode,
            worker_handle: None,
            worker_bucket: Some(remote_bucket),
        }
    }

    pub fn start(&mut self) {
        if self.is_running() {
            return;
        }

        let remote_bucket = self.worker_bucket.take().unwrap();
        let replication_name = self.name.clone();
        let thr_settings = self.settings.clone();
        let thr_io_config = self.io_config.clone();
        let thr_log_map = Arc::clone(&self.log_map);
        let thr_storage = Arc::clone(&self.storage);
        let thr_hourly_diagnostics = Arc::clone(&self.hourly_diagnostics);
        let thr_system_options = self.system_options.clone();
        let thr_stop_flag = Arc::clone(&self.stop_flag);
        let thr_is_active = Arc::clone(&self.is_active);
        let thr_mode = Arc::clone(&self.mode);

        let handle = tokio::spawn(async move {
            let init_transaction_logs = async || {
                let mut logs = thr_log_map.write().await?;
                for entry in thr_storage
                    .get_bucket(&thr_settings.src_bucket)
                    .await?
                    .upgrade()?
                    .info()
                    .await?
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
                    )
                    .await
                    {
                        Ok(log) => log,
                        Err(err) => {
                            error!(
                                "Failed to load transaction log for entry '{}': {:?}",
                                entry.name, err
                            );
                            info!("Creating a new transaction log for entry '{}'", entry.name);
                            FILE_CACHE.remove(&path).await?;
                            TransactionLog::try_load_or_create(
                                &path,
                                thr_system_options.transaction_log_size,
                            )
                            .await?
                        }
                    };

                    logs.insert(entry.name, Arc::new(AsyncRwLock::new(log)));
                }

                Ok::<(), ReductError>(())
            };

            if let Err(err) = init_transaction_logs().await {
                error!("Failed to initialize transaction logs: {:?}", err);
            }

            let mut sender = ReplicationSender::new(
                thr_log_map.clone(),
                thr_storage.clone(),
                thr_settings.clone(),
                thr_io_config.clone(),
                remote_bucket,
            );

            while !thr_stop_flag.load(Ordering::Relaxed) {
                match ReplicationTask::load_mode_from(&thr_mode) {
                    ReplicationMode::Disabled => {
                        thr_is_active.store(false, Ordering::Relaxed);
                        ReplicationTask::sleep_with_stop(
                            &thr_stop_flag,
                            thr_system_options.next_transaction_timeout,
                        )
                        .await;
                        continue;
                    }
                    ReplicationMode::Paused => {
                        let available = sender.probe_availability().await;
                        thr_is_active.store(available, Ordering::Relaxed);
                        ReplicationTask::sleep_with_stop(
                            &thr_stop_flag,
                            thr_system_options.next_transaction_timeout,
                        )
                        .await;
                        continue;
                    }
                    ReplicationMode::Enabled => {}
                }

                let mut counter = None;
                match sender.run().await {
                    Ok(SyncState::SyncedOrRemoved(c)) => {
                        thr_is_active.store(true, Ordering::Relaxed);
                        counter = Some(c);
                    }
                    Ok(SyncState::NotAvailable(c)) => {
                        thr_is_active.store(false, Ordering::Relaxed);
                        counter = Some(c);
                        ReplicationTask::sleep_with_stop(
                            &thr_stop_flag,
                            thr_system_options.remote_bucket_unavailable_timeout,
                        )
                        .await;
                    }
                    Ok(SyncState::NoTransactions) => {
                        // NOTE: we don't want to spin the CPU when there is nothing to do or the bucket is not available
                        thr_is_active.store(true, Ordering::Relaxed);
                        ReplicationTask::sleep_with_stop(
                            &thr_stop_flag,
                            thr_system_options.next_transaction_timeout,
                        )
                        .await;
                    }
                    Ok(SyncState::BrokenLog(entry_name)) => {
                        thr_is_active.store(false, Ordering::Relaxed);

                        info!("Transaction log is corrupted, dropping the whole log");
                        let path = ReplicationTask::build_path_to_transaction_log(
                            thr_storage.data_path(),
                            &thr_settings.src_bucket,
                            &entry_name,
                            &replication_name,
                        );
                        if let Err(err) = FILE_CACHE.remove(&path).await {
                            error!("Failed to remove transaction log: {:?}", err);
                        }

                        info!("Creating a new transaction log: {:?}", path);
                        match TransactionLog::try_load_or_create(
                            &path,
                            thr_system_options.transaction_log_size,
                        )
                        .await
                        {
                            Ok(log) => {
                                thr_log_map
                                    .write()
                                    .await
                                    .unwrap()
                                    .insert(entry_name, Arc::new(AsyncRwLock::new(log)));
                            }

                            Err(err) => {
                                error!("Failed to create transaction log: {:?}", err);
                                ReplicationTask::sleep_with_stop(
                                    &thr_stop_flag,
                                    thr_system_options.log_recovery_timeout,
                                )
                                .await;
                            }
                        }
                    }
                    Err(err) => {
                        thr_is_active.store(false, Ordering::Relaxed);
                        error!("Replication sender error: {:?}", err);
                        ReplicationTask::sleep_with_stop(
                            &thr_stop_flag,
                            thr_system_options.next_transaction_timeout,
                        )
                        .await;
                    }
                }

                if let Some(c) = counter {
                    match thr_hourly_diagnostics.write().await {
                        Ok(mut diagnostics) => {
                            for (result, count) in c.into_iter() {
                                diagnostics.count(result, count);
                            }
                        }
                        Err(err) => error!("Failed to acquire hourly diagnostics lock: {:?}", err),
                    }
                }
            }
        });

        self.worker_handle = Some(handle);
    }

    pub async fn notify(
        &mut self,
        notification: TransactionNotification,
    ) -> Result<(), ReductError> {
        if matches!(self.load_mode(), ReplicationMode::Disabled) {
            return Ok(());
        }
        // We need to have a filter for each entry
        let entry_name = notification.entry.clone();
        let notifications = {
            if !self.filter_map.contains_key(&notification.entry) {
                self.filter_map.insert(
                    notification.entry.clone(),
                    TransactionFilter::try_new(
                        self.name(),
                        self.settings.clone(),
                        self.io_config.clone(),
                    )?,
                );
            }

            let filter = self.filter_map.get_mut(&entry_name).unwrap();
            filter.filter(notification)
        };

        // NOTE: very important not to lock the log_map for too long
        // because it is used by the replication thread
        let exists = { self.log_map.read().await?.contains_key(&entry_name) };
        if !exists {
            let log = TransactionLog::try_load_or_create(
                &Self::build_path_to_transaction_log(
                    self.storage.data_path(),
                    &self.settings.src_bucket,
                    &entry_name,
                    &self.name,
                ),
                self.system_options.transaction_log_size,
            )
            .await?;
            let mut map = self.log_map.write().await?;
            map.entry(entry_name.clone())
                .or_insert_with(|| Arc::new(AsyncRwLock::new(log)));
        };

        let log = {
            let map = self.log_map.read().await?;
            Arc::clone(map.get(&entry_name).unwrap())
        };

        for notification in notifications.into_iter() {
            if let Some(_) = log.write().await?.push_back(notification.event).await? {
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
            dst_token: None,
            mode: self.load_mode(),
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

    pub fn set_mode(&mut self, mode: ReplicationMode) {
        self.settings.mode = mode.clone();
        self.mode.store(mode as u8, Ordering::Relaxed);
    }

    pub fn mode(&self) -> ReplicationMode {
        self.load_mode()
    }

    pub fn is_running(&self) -> bool {
        self.worker_handle.is_some()
    }

    pub async fn info(&self) -> Result<ReplicationInfo, ReductError> {
        let mut pending_records = 0;
        for (_, log) in self.log_map.read().await?.iter() {
            pending_records += log.read().await?.len() as u64;
        }

        let mode = self.load_mode();
        Ok(ReplicationInfo {
            name: self.name.clone(),
            mode: mode.clone(),
            is_active: matches!(mode, ReplicationMode::Enabled | ReplicationMode::Paused)
                && self.is_active.load(Ordering::Relaxed),
            is_provisioned: self.is_provisioned,
            pending_records,
        })
    }

    pub async fn diagnostics(&self) -> Result<Diagnostics, ReductError> {
        Ok(Diagnostics {
            hourly: self.hourly_diagnostics.read().await?.diagnostics(),
        })
    }

    async fn sleep_with_stop(stop_flag: &Arc<AtomicBool>, duration: Duration) {
        const SLICE: Duration = Duration::from_millis(50);
        let mut remaining = duration;
        while remaining > Duration::ZERO && !stop_flag.load(Ordering::Relaxed) {
            let step = remaining.min(SLICE);
            tokio::time::sleep(step).await;
            remaining = remaining.saturating_sub(step);
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

    fn load_mode(&self) -> ReplicationMode {
        Self::load_mode_from(&self.mode)
    }

    fn load_mode_from(mode: &Arc<AtomicU8>) -> ReplicationMode {
        match mode.load(Ordering::Relaxed) {
            x if x == ReplicationMode::Paused as u8 => ReplicationMode::Paused,
            x if x == ReplicationMode::Disabled as u8 => ReplicationMode::Disabled,
            _ => ReplicationMode::Enabled,
        }
    }
}

impl Drop for ReplicationTask {
    fn drop(&mut self) {
        self.stop_flag.store(true, Ordering::Relaxed);
        if let Some(handle) = self.worker_handle.take() {
            // Use recv() without unwrap to avoid panic if the channel is disconnected
            // (e.g., during test cleanup or thread pool shutdown)
            tokio::spawn(async move {
                if let Err(err) = handle.await {
                    error!("Replication worker task failed to join: {:?}", err);
                }
            });
        }
    }
}

#[cfg(target_os = "linux")]
#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use bytes::Bytes;
    use std::fs;
    use std::io::Write;
    use std::time::Instant;

    use crate::core::file_cache::FILE_CACHE;
    use mockall::mock;
    use reduct_base::io::{BoxedReadRecord, RecordMeta};
    use rstest::*;

    use crate::replication::remote_bucket::ErrorRecordMap;
    use crate::replication::Transaction;

    use crate::backend::Backend;
    use crate::core::sync::rwlock_timeout;
    use crate::storage::bucket::Bucket;
    use reduct_base::msg::bucket_api::BucketSettings;
    use reduct_base::msg::diagnostics::DiagnosticsItem;
    use reduct_base::Labels;
    use tokio::time::sleep as tokio_sleep;

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
    async fn test_transaction_log_init(
        remote_bucket: MockRmBucket,
        settings: ReplicationSettings,
        path: PathBuf,
    ) {
        let replication = build_replication(path, remote_bucket, settings).await;
        let log_map = replication.log_map.read().await.unwrap();
        assert_eq!(log_map.len(), 2);
        assert!(
            log_map.contains_key("test1"),
            "Transaction log is initialized"
        );
        assert!(
            log_map.contains_key("test2"),
            "Transaction log is initialized"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_transaction_log_init_err(
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
                .await
                .unwrap();
            FILE_CACHE
                .create_dir_all(&path.join(&settings.src_bucket))
                .await
                .unwrap();
            Bucket::try_build(
                &settings.src_bucket,
                &path,
                BucketSettings::default(),
                Cfg::default(),
            )
            .await
            .unwrap();

            fs::create_dir_all(log_path.parent().unwrap()).unwrap();
            let mut log_file = fs::File::create(&log_path).unwrap();

            log_file
                .write_all(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
                .unwrap();
        }

        let replication = build_replication(path, remote_bucket, settings).await;
        let log_map = replication.log_map.read().await.unwrap();
        assert_eq!(log_map.len(), 2);
        let log_len = log_map.get("test1").unwrap().read().await.unwrap().len();
        assert_eq!(
            log_len, 0,
            "Task recreated a new transaction log for 'test1' after broken log"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_add_new_entry(
        mut remote_bucket: MockRmBucket,
        mut notification: TransactionNotification,
        settings: ReplicationSettings,
        path: PathBuf,
    ) {
        remote_bucket
            .expect_write_batch()
            .returning(|_, _| Ok(ErrorRecordMap::new()));
        remote_bucket.expect_is_active().return_const(true);
        let mut replication = build_replication(path, remote_bucket, settings.clone()).await;

        notification.entry = "new_entry".to_string();
        fs::create_dir_all(
            replication
                .storage
                .data_path()
                .join(settings.src_bucket)
                .join("new_entry"),
        )
        .unwrap();

        replication.notify(notification).await.unwrap();
        tokio_sleep(Duration::from_millis(100)).await;
        assert!(transaction_log_is_empty(&replication).await);
        assert_eq!(
            replication.info().await.unwrap(),
            ReplicationInfo {
                name: "test".to_string(),
                mode: ReplicationMode::Enabled,
                is_active: true,
                is_provisioned: false,
                pending_records: 0,
            }
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_replication_ok_active(
        mut remote_bucket: MockRmBucket,
        notification: TransactionNotification,
        settings: ReplicationSettings,
        path: PathBuf,
    ) {
        remote_bucket
            .expect_write_batch()
            .returning(|_, _| Ok(ErrorRecordMap::new()));
        remote_bucket.expect_is_active().return_const(true);
        let mut replication = build_replication(path, remote_bucket, settings).await;

        replication.notify(notification).await.unwrap();
        tokio_sleep(Duration::from_millis(100)).await;
        assert!(transaction_log_is_empty(&replication).await);
        assert_eq!(
            replication.info().await.unwrap(),
            ReplicationInfo {
                name: "test".to_string(),
                mode: ReplicationMode::Enabled,
                is_active: true,
                is_provisioned: false,
                pending_records: 0,
            }
        );
        assert_eq!(
            replication.diagnostics().await.unwrap(),
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
    async fn test_replication_inactive(
        mut remote_bucket: MockRmBucket,
        notification: TransactionNotification,
        settings: ReplicationSettings,
        path: PathBuf,
    ) {
        remote_bucket
            .expect_write_batch()
            .returning(|_, _| Ok(ErrorRecordMap::new()));
        remote_bucket.expect_is_active().return_const(false);
        let mut replication = build_replication(path, remote_bucket, settings).await;

        replication.notify(notification).await.unwrap();
        tokio_sleep(Duration::from_millis(100)).await;
        assert!(!transaction_log_is_empty(&replication).await);
        assert_eq!(
            replication.info().await.unwrap(),
            ReplicationInfo {
                name: "test".to_string(),
                mode: ReplicationMode::Enabled,
                is_active: false,
                is_provisioned: false,
                pending_records: 1,
            }
        );
        assert_eq!(
            replication.diagnostics().await.unwrap(),
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
    #[tokio::test]
    async fn test_replication_paused_mode_available(
        mut remote_bucket: MockRmBucket,
        notification: TransactionNotification,
        mut settings: ReplicationSettings,
        path: PathBuf,
    ) {
        settings.mode = ReplicationMode::Paused;
        remote_bucket.expect_probe_availability().returning(|| ());
        remote_bucket.expect_is_active().return_const(true);
        let mut replication = build_replication(path, remote_bucket, settings).await;

        replication.notify(notification).await.unwrap();
        tokio_sleep(Duration::from_millis(100)).await;
        assert_eq!(
            replication.info().await.unwrap().is_active,
            true,
            "is_active should reflect remote availability when paused"
        );
        assert_eq!(replication.info().await.unwrap().pending_records, 1);
    }

    #[rstest]
    #[tokio::test]
    async fn test_replication_paused_mode_unavailable(
        mut remote_bucket: MockRmBucket,
        notification: TransactionNotification,
        mut settings: ReplicationSettings,
        path: PathBuf,
    ) {
        settings.mode = ReplicationMode::Paused;
        remote_bucket.expect_probe_availability().returning(|| ());
        remote_bucket.expect_is_active().return_const(false);
        let mut replication = build_replication(path, remote_bucket, settings).await;

        replication.notify(notification).await.unwrap();
        tokio_sleep(Duration::from_millis(100)).await;
        assert_eq!(
            replication.info().await.unwrap().is_active,
            false,
            "is_active should be false when remote unavailable"
        );
        assert_eq!(replication.info().await.unwrap().pending_records, 1);
    }

    #[rstest]
    #[tokio::test]
    async fn test_replication_disabled_mode(
        remote_bucket: MockRmBucket,
        notification: TransactionNotification,
        mut settings: ReplicationSettings,
        path: PathBuf,
    ) {
        settings.mode = ReplicationMode::Disabled;
        let mut replication = build_replication(path, remote_bucket, settings).await;

        replication.notify(notification).await.unwrap();
        tokio_sleep(Duration::from_millis(100)).await;
        assert_eq!(replication.info().await.unwrap().pending_records, 0);
        assert_eq!(replication.info().await.unwrap().is_active, false);
    }

    #[rstest]
    #[tokio::test]
    async fn test_replication_filter_each_entry(
        mut notification: TransactionNotification,
        mut remote_bucket: MockRmBucket,
        settings: ReplicationSettings,
        path: PathBuf,
    ) {
        remote_bucket
            .expect_write_batch()
            .returning(|_, _| Ok(ErrorRecordMap::new()));

        let settings = ReplicationSettings {
            each_n: Some(2),
            ..settings
        };

        let mut replication = build_replication(path, MockRmBucket::new(), settings.clone()).await;

        let mut time = 10;
        for entry in &["test1", "test2"] {
            for _ in 0..3 {
                notification.entry = entry.to_string();
                notification.event = Transaction::WriteRecord(time.clone());
                replication.notify(notification.clone()).await.unwrap();
                time += 10;
            }
        }

        assert_eq!(replication.log_map.read().await.unwrap().len(), 2);
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
        path: PathBuf,
    ) {
        remote_bucket
            .expect_write_batch()
            .returning(|_, _| Ok(ErrorRecordMap::new()));
        remote_bucket.expect_is_active().return_const(true);
        let mut replication = build_replication(path, remote_bucket, settings.clone()).await;
        replication.notify(notification.clone()).await.unwrap();

        let path = ReplicationTask::build_path_to_transaction_log(
            replication.storage.data_path(),
            &settings.src_bucket,
            &notification.entry,
            &replication.name,
        );
        fs::write(path.clone(), "broken").unwrap();
        tokio_sleep(Duration::from_millis(100)).await;

        assert_eq!(
            get_entries_from_transaction_log(&mut replication, "test1").await,
            vec![],
            "Transaction log is empty"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_broken_transaction_log_failed_recover(
        mut remote_bucket: MockRmBucket,
        notification: TransactionNotification,
        settings: ReplicationSettings,
        path: PathBuf,
    ) {
        remote_bucket
            .expect_write_batch()
            .returning(|_, _| Ok(ErrorRecordMap::new()));
        remote_bucket.expect_is_active().return_const(true);
        let mut replication = build_replication(path, remote_bucket, settings.clone()).await;
        replication.notify(notification.clone()).await.unwrap();

        let path = ReplicationTask::build_path_to_transaction_log(
            replication.storage.data_path(),
            &settings.src_bucket,
            &notification.entry,
            &replication.name,
        );

        FILE_CACHE
            .remove_dir(&path.parent().unwrap().parent().unwrap().to_path_buf())
            .await
            .unwrap();
        tokio_sleep(Duration::from_millis(100)).await;

        assert!(
                !path.exists(),
                "We could not recover the transaction log, it was removed. However, the replication should continue"
            );

        fs::create_dir_all(path.parent().unwrap()).unwrap();
        tokio_sleep(Duration::from_millis(200)).await;

        assert_eq!(
            get_entries_from_transaction_log(&mut replication, "test1").await,
            vec![],
            "Transaction log recovered"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_sender_error_handling(
        mut remote_bucket: MockRmBucket,
        notification: TransactionNotification,
        settings: ReplicationSettings,
    ) {
        remote_bucket
            .expect_write_batch()
            .returning(|_, _| Ok(ErrorRecordMap::new()));
        remote_bucket.expect_is_active().return_const(true);

        let path = tempfile::tempdir().unwrap().keep();
        let mut replication = build_replication(path, remote_bucket, settings.clone()).await;
        replication.notify(notification.clone()).await.unwrap();
        {
            let _lock = replication.log_map.write().await.unwrap();
            tokio_sleep(rwlock_timeout() + Duration::from_millis(100)).await;
        }

        assert_eq!(
            replication.info().await.unwrap(),
            ReplicationInfo {
                name: "test".to_string(),
                mode: ReplicationMode::Enabled,
                is_active: false,
                is_provisioned: false,
                pending_records: 1,
            }
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_stop_interrupts_long_sleep(
        mut remote_bucket: MockRmBucket,
        notification: TransactionNotification,
        settings: ReplicationSettings,
        path: PathBuf,
    ) {
        remote_bucket
            .expect_write_batch()
            .returning(|_, _| Ok(ErrorRecordMap::new()));
        remote_bucket.expect_is_active().return_const(false);

        let mut replication = build_replication(path, remote_bucket, settings).await;
        replication.notify(notification).await.unwrap();
        tokio_sleep(Duration::from_millis(100)).await; // allow worker to start processing

        let start = Instant::now();
        drop(replication);
        assert!(
            start.elapsed() < Duration::from_millis(500),
            "Shutdown should not wait for full remote_bucket_unavailable_timeout"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_double_start(remote_bucket: MockRmBucket, settings: ReplicationSettings) {
        let path = tempfile::tempdir().unwrap().keep();
        let mut replication = build_replication(path, remote_bucket, settings).await;

        let handle_before = replication.worker_handle.as_ref().unwrap().id();
        replication.start();
        let handle_after = replication.worker_handle.as_ref().unwrap().id();

        assert_eq!(
            handle_before, handle_after,
            "Starting an already started replication should have no effect"
        );
    }

    async fn build_replication(
        path: PathBuf,
        remote_bucket: MockRmBucket,
        settings: ReplicationSettings,
    ) -> ReplicationTask {
        let cfg = Cfg {
            data_path: path.clone(),
            ..Default::default()
        };

        FILE_CACHE.set_storage_backend(
            Backend::builder()
                .local_data_path(path.clone())
                .try_build()
                .await
                .unwrap(),
        );

        let storage = StorageEngine::builder()
            .with_data_path(path)
            .with_cfg(cfg)
            .build()
            .await;
        let storage = Arc::new(storage);

        let bucket = match storage.get_bucket(&settings.src_bucket).await {
            Ok(bucket) => bucket.upgrade().unwrap(),
            Err(_) => storage
                .create_bucket("src", BucketSettings::default())
                .await
                .unwrap()
                .upgrade_and_unwrap(),
        };

        let mut time = 10;
        for entry in ["test1", "test2"] {
            for _ in 0..3 {
                let mut writer = bucket
                    .begin_write(entry, time, 4, "text/plain".to_string(), Labels::new())
                    .await
                    .unwrap();
                writer.send(Ok(Some(Bytes::from("test")))).await.unwrap();
                writer.send(Ok(None)).await.unwrap_or(());
                time += 10;
            }

            time += 10;
        }

        let mut repl = ReplicationTask::build(
            "test".to_string(),
            settings,
            ReplicationSystemOptions {
                transaction_log_size: 1000,
                remote_bucket_unavailable_timeout: Duration::from_secs(5),
                next_transaction_timeout: Duration::from_millis(50),
                log_recovery_timeout: Duration::from_millis(100),
            },
            IoConfig::default(),
            Box::new(remote_bucket),
            storage,
        );

        repl.start();
        tokio_sleep(Duration::from_millis(10)).await; // wait for the transaction log to be initialized in worker
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
            dst_token: Some("token".to_string()),
            entries: vec!["test1".to_string(), "test2".to_string()],
            include: Labels::new(),
            exclude: Labels::new(),
            each_n: None,
            each_s: None,
            when: None,
            mode: ReplicationMode::Enabled,
        }
    }

    #[fixture]
    fn path() -> PathBuf {
        tempfile::tempdir().unwrap().keep()
    }

    async fn transaction_log_is_empty(replication: &ReplicationTask) -> bool {
        tokio_sleep(Duration::from_millis(50)).await;
        tokio_sleep(Duration::from_millis(50)).await;

        replication
            .log_map
            .read()
            .await
            .unwrap()
            .get("test1")
            .unwrap()
            .read()
            .await
            .unwrap()
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
}
