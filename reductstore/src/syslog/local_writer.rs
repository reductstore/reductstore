// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::storage::engine::StorageEngine;
use crate::syslog::path::{entry_path, record_labels};
use crate::syslog::{LogSystemEvent, SystemEvent};
use async_trait::async_trait;
use bytes::Bytes;
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::io::WriteRecord;
use reduct_base::msg::bucket_api::BucketSettings;
use reduct_base::Labels;
use std::sync::Arc;

pub(super) struct LocalSystemLogger {
    bucket_name: &'static str,
    bucket_settings: BucketSettings,
    storage: Arc<StorageEngine>,
}

impl LocalSystemLogger {
    pub(super) fn new(
        bucket_name: &'static str,
        bucket_settings: BucketSettings,
        storage: Arc<StorageEngine>,
    ) -> Self {
        Self {
            bucket_name,
            bucket_settings,
            storage,
        }
    }

    async fn log_local(&self, event: SystemEvent) -> Result<(), ReductError> {
        let entry_name = entry_path(&event);
        let labels = record_labels(&event);
        let payload = event.to_flat_json()?;
        let mut writer = match self
            .begin_write(&event, &entry_name, payload.len() as u64, labels.clone())
            .await
        {
            Ok(writer) => writer,
            Err(err) if err.status == ErrorCode::NotFound => {
                let bucket = self
                    .storage
                    .create_system_bucket(self.bucket_name, self.bucket_settings.clone())
                    .await?;
                // The server owns this bucket, so protect it from being removed,
                // renamed or reconfigured through the API.
                if let Ok(bucket) = bucket.upgrade() {
                    bucket.set_provisioned(true);
                }
                self.begin_write(&event, &entry_name, payload.len() as u64, labels)
                    .await?
            }
            Err(err) => return Err(err),
        };
        writer.send(Ok(Some(Bytes::from(payload)))).await?;
        writer.send(Ok(None)).await?;
        Ok(())
    }

    /// Begin writing the record of an event. The storage engine notifies
    /// replications about the written record unless the event has its
    /// `replicate` flag cleared, e.g. logs of the replication module itself.
    async fn begin_write(
        &self,
        event: &SystemEvent,
        entry_name: &str,
        content_size: u64,
        labels: Labels,
    ) -> Result<Box<dyn WriteRecord + Sync + Send>, ReductError> {
        let content_type = "application/json".to_string();
        if event.replicate {
            self.storage
                .begin_write(
                    self.bucket_name,
                    entry_name,
                    event.timestamp,
                    content_size,
                    content_type,
                    labels,
                )
                .await
        } else {
            self.storage
                .begin_write_without_notification(
                    self.bucket_name,
                    entry_name,
                    event.timestamp,
                    content_size,
                    content_type,
                    labels,
                )
                .await
        }
    }
}

#[async_trait]
impl LogSystemEvent for LocalSystemLogger {
    async fn log_event(&mut self, event: SystemEvent) -> Result<(), ReductError> {
        self.log_local(event).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::Cfg;
    use crate::core::sync::{rwlock_timeout, set_rwlock_timeout, AsyncRwLock};
    use crate::replication::{ManageReplications, ReplicationRepoBuilder};
    use crate::syslog::{SystemEventKind, SYSTEM_BUCKET_NAME};
    use reduct_base::internal_server_error;
    use reduct_base::msg::replication_api::{ReplicationMode, ReplicationSettings};
    use rstest::{fixture, rstest};
    use serde_json::json;
    use serial_test::serial;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{sleep, Duration};

    struct RestoreRwLockTimeout(Duration);

    impl Drop for RestoreRwLockTimeout {
        fn drop(&mut self) {
            set_rwlock_timeout(self.0);
        }
    }

    fn system_event(kind: SystemEventKind, replicate: bool) -> SystemEvent {
        SystemEvent {
            kind,
            replicate,
            event_type: "usage".to_string(),
            timestamp: 100,
            instance: "instance-1".to_string(),
            entry_name: "traffic".to_string(),
            status: 200,
            message: "".to_string(),
            payload: json!({"write_bytes": 42}),
        }
    }

    #[fixture]
    async fn storage() -> Arc<StorageEngine> {
        let tmp_dir = tempfile::tempdir().unwrap();
        let cfg = Cfg {
            data_path: tmp_dir.keep(),
            ..Cfg::default()
        };
        let storage = StorageEngine::builder()
            .with_data_path(cfg.data_path.clone())
            .with_cfg(cfg)
            .build()
            .await;
        Arc::new(storage)
    }

    fn writer(storage: Arc<StorageEngine>) -> LocalSystemLogger {
        LocalSystemLogger::new(SYSTEM_BUCKET_NAME, BucketSettings::default(), storage)
    }

    type SystemReplicationRepo = Arc<AsyncRwLock<Box<dyn ManageReplications + Send + Sync>>>;

    /// A real repo with `$system` as replication source.
    async fn system_replication_repo(storage: Arc<StorageEngine>) -> SystemReplicationRepo {
        storage
            .create_system_bucket(SYSTEM_BUCKET_NAME, BucketSettings::default())
            .await
            .unwrap();
        let repo = ReplicationRepoBuilder::new(Cfg::default())
            .build(Arc::clone(&storage))
            .await;
        repo.create_replication(
            "sys-replication",
            ReplicationSettings {
                src_bucket: SYSTEM_BUCKET_NAME.to_string(),
                dst_bucket: "dst-bucket".to_string(),
                dst_host: "http://localhost".to_string(),
                dst_token: None,
                entries: vec![],
                dst_prefix: String::new(),
                when: None,
                mode: ReplicationMode::Enabled,
                compression: Default::default(),
            },
        )
        .await
        .unwrap();
        Arc::new(AsyncRwLock::new(repo))
    }

    /// Register the replication repo as the notifier of the storage engine,
    /// the same way the components do it.
    fn attach_notifier(storage: &StorageEngine, repo: &SystemReplicationRepo) {
        let repo = Arc::clone(repo);
        storage
            .set_replication_notifier(Some(Arc::new(move |notification| {
                let repo = Arc::clone(&repo);
                Box::pin(async move { repo.read().await?.notify(notification).await })
            })))
            .unwrap();
    }

    async fn pending_records(repo: &SystemReplicationRepo) -> u64 {
        repo.read()
            .await
            .unwrap()
            .get_info("sys-replication")
            .await
            .unwrap()
            .info
            .pending_records
    }

    #[rstest]
    #[tokio::test]
    async fn usage_event_notifies_replication(#[future] storage: Arc<StorageEngine>) {
        let storage = storage.await;
        let repo = system_replication_repo(Arc::clone(&storage)).await;
        attach_notifier(&storage, &repo);
        let mut writer = writer(storage);

        writer
            .log_event(system_event(SystemEventKind::Usage, true))
            .await
            .unwrap();
        sleep(Duration::from_millis(50)).await;

        assert_eq!(
            pending_records(&repo).await,
            1,
            "a usage system event must produce exactly one pending transaction"
        );
    }

    #[rstest]
    #[tokio::test]
    #[serial]
    async fn usage_event_notifies_replication_while_repo_read_guard_is_held(
        #[future] storage: Arc<StorageEngine>,
    ) {
        let _restore_timeout = RestoreRwLockTimeout(rwlock_timeout());
        set_rwlock_timeout(Duration::from_millis(20));

        let storage = storage.await;
        let repo = system_replication_repo(Arc::clone(&storage)).await;
        attach_notifier(&storage, &repo);
        let mut writer = writer(storage);

        let guard = repo.read().await.unwrap();
        writer
            .log_event(system_event(SystemEventKind::Usage, true))
            .await
            .unwrap();
        drop(guard);
        sleep(Duration::from_millis(50)).await;

        assert_eq!(
            pending_records(&repo).await,
            1,
            "a caller holding the outer repository read guard must not block system-event replication"
        );
    }

    /// Regression test for #1593.
    #[rstest]
    #[tokio::test]
    #[serial]
    async fn concurrent_replication_update_does_not_block_system_event_notification(
        #[future] storage: Arc<StorageEngine>,
    ) {
        let _restore_timeout = RestoreRwLockTimeout(rwlock_timeout());
        set_rwlock_timeout(Duration::from_millis(20));

        let storage = storage.await;
        let repo = system_replication_repo(Arc::clone(&storage)).await;
        attach_notifier(&storage, &repo);
        let mut writer = writer(storage);

        let update_repo = Arc::clone(&repo);
        let update = async move {
            update_repo
                .read()
                .await
                .unwrap()
                .update_replication(
                    "sys-replication",
                    ReplicationSettings {
                        src_bucket: SYSTEM_BUCKET_NAME.to_string(),
                        dst_bucket: "dst-bucket".to_string(),
                        dst_host: "http://localhost".to_string(),
                        dst_token: None,
                        entries: vec![],
                        dst_prefix: String::new(),
                        when: None,
                        mode: ReplicationMode::Enabled,
                        compression: Default::default(),
                    },
                )
                .await
        };
        let notify = writer.log_event(system_event(SystemEventKind::Usage, true));

        let (update_result, log_result) = tokio::join!(update, notify);
        update_result.expect("update_replication must not be blocked by the notifier");
        log_result.expect("the system event write itself must always succeed");

        sleep(Duration::from_millis(50)).await;
        assert_eq!(
            pending_records(&repo).await,
            1,
            "a concurrent replication update must not drop the system-event replication notification"
        );
    }

    #[rstest]
    #[case::replications(SystemEventKind::Replication)]
    #[case::logs(SystemEventKind::Log)]
    #[tokio::test]
    async fn non_replicable_events_do_not_notify(
        #[future] storage: Arc<StorageEngine>,
        #[case] kind: SystemEventKind,
    ) {
        let storage = storage.await;
        let repo = system_replication_repo(Arc::clone(&storage)).await;
        attach_notifier(&storage, &repo);
        let mut writer = writer(storage);

        writer.log_event(system_event(kind, false)).await.unwrap();
        sleep(Duration::from_millis(50)).await;

        assert_eq!(
            pending_records(&repo).await,
            0,
            "events with the replicate flag cleared must not produce replication transactions"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn cleared_notifier_stops_notifications(#[future] storage: Arc<StorageEngine>) {
        let storage = storage.await;
        let repo = system_replication_repo(Arc::clone(&storage)).await;
        attach_notifier(&storage, &repo);
        storage.set_replication_notifier(None).unwrap();
        let mut writer = writer(storage);

        writer
            .log_event(system_event(SystemEventKind::Usage, true))
            .await
            .unwrap();
        sleep(Duration::from_millis(50)).await;

        assert_eq!(
            pending_records(&repo).await,
            0,
            "no transactions may be produced after the notifier is detached"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn notification_error_is_swallowed(#[future] storage: Arc<StorageEngine>) {
        let storage = storage.await;
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&calls);
        storage
            .set_replication_notifier(Some(Arc::new(move |_notification| {
                seen.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Err(internal_server_error!("replication is down")) })
            })))
            .unwrap();
        let mut writer = writer(storage);

        writer
            .log_event(system_event(SystemEventKind::Usage, true))
            .await
            .expect("a replication failure must never fail the system-event write");
        assert_eq!(calls.load(Ordering::SeqCst), 1, "notifier must be invoked");
    }

    #[rstest]
    #[case::replicable(true, 1)]
    #[case::not_replicable(false, 0)]
    #[tokio::test]
    async fn lazily_created_system_bucket_notifies_once(
        #[future] storage: Arc<StorageEngine>,
        #[case] replicate: bool,
        #[case] expected_calls: usize,
    ) {
        let storage = storage.await;
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&calls);
        storage
            .set_replication_notifier(Some(Arc::new(move |notification| {
                assert_eq!(notification.bucket, SYSTEM_BUCKET_NAME);
                seen.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(()) })
            })))
            .unwrap();
        let mut writer = writer(Arc::clone(&storage));
        assert!(storage.get_bucket(SYSTEM_BUCKET_NAME).await.is_err());

        writer
            .log_event(system_event(SystemEventKind::Usage, replicate))
            .await
            .unwrap();

        assert!(storage.get_bucket(SYSTEM_BUCKET_NAME).await.is_ok());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            expected_calls,
            "the retried write after creating the bucket must notify at most once"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn no_notifier_registered_still_writes(#[future] storage: Arc<StorageEngine>) {
        let storage = storage.await;
        let mut writer = writer(storage);
        writer
            .log_event(system_event(SystemEventKind::Usage, true))
            .await
            .unwrap();
    }
}
