// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::replication::{Transaction, TransactionNotification};
use crate::storage::engine::StorageEngine;
use crate::syslog::path::{entry_path, record_labels};
use crate::syslog::sink::ReplicationNotifier;
use crate::syslog::{LogSystemEvent, SystemEvent};
use async_trait::async_trait;
use bytes::Bytes;
use log::warn;
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::io::RecordMeta;
use reduct_base::msg::bucket_api::BucketSettings;
use std::sync::Arc;

pub(super) struct LocalSystemLogger {
    bucket_name: &'static str,
    bucket_settings: BucketSettings,
    storage: Arc<StorageEngine>,
    /// Late-bound (issue #1457): registered by the components wiring once the
    /// replication repo exists, so `$system` writes replicate like API writes.
    replication_notifier: Option<ReplicationNotifier>,
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
            replication_notifier: None,
        }
    }

    async fn log_local(&self, event: SystemEvent) -> Result<(), ReductError> {
        let entry_name = entry_path(&event);
        let labels = record_labels(&event);
        let payload = event.to_flat_json()?;
        let mut writer = match self
            .storage
            .begin_write(
                self.bucket_name,
                &entry_name,
                event.timestamp,
                payload.len() as u64,
                "application/json".to_string(),
                labels.clone(),
            )
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
                self.storage
                    .begin_write(
                        self.bucket_name,
                        &entry_name,
                        event.timestamp,
                        payload.len() as u64,
                        "application/json".to_string(),
                        labels.clone(),
                    )
                    .await?
            }
            Err(err) => return Err(err),
        };
        writer.send(Ok(Some(Bytes::from(payload)))).await?;
        writer.send(Ok(None)).await?;

        self.notify_replication(&event, entry_name, labels).await;
        Ok(())
    }

    /// Notify replication about a successful `$system` write, exactly as the
    /// API handlers do (issue #1457). Events whose `replicate` flag is cleared
    /// by their producer (diagnostics of a `$system`-source replication, logs
    /// emitted by the replication module) are skipped to prevent feedback
    /// loops. A notification failure is swallowed and logged — a replication
    /// problem must never fail a system-event write.
    async fn notify_replication(
        &self,
        event: &SystemEvent,
        entry_name: String,
        labels: reduct_base::Labels,
    ) {
        if !event.replicate {
            return;
        }
        let Some(notifier) = &self.replication_notifier else {
            return;
        };

        let notification = TransactionNotification {
            bucket: self.bucket_name.to_string(),
            entry: entry_name,
            meta: RecordMeta::builder()
                .timestamp(event.timestamp)
                .labels(labels)
                .build(),
            event: Transaction::WriteRecord(event.timestamp),
        };
        if let Err(err) = notifier(notification).await {
            warn!(
                "Failed to notify replication about system event '{}': {}",
                event.entry_name, err
            );
        }
    }
}

#[async_trait]
impl LogSystemEvent for LocalSystemLogger {
    async fn log_event(&mut self, event: SystemEvent) -> Result<(), ReductError> {
        self.log_local(event).await
    }

    async fn set_replication_notifier(&mut self, notifier: Option<ReplicationNotifier>) {
        self.replication_notifier = notifier;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::Cfg;
    use crate::core::sync::AsyncRwLock;
    use crate::replication::{ManageReplications, ReplicationRepoBuilder};
    use crate::syslog::{SystemEventKind, SYSTEM_BUCKET_NAME};
    use reduct_base::internal_server_error;
    use reduct_base::msg::replication_api::{ReplicationMode, ReplicationSettings};
    use rstest::{fixture, rstest};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{sleep, Duration};

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

    /// A real repo with `$system` as replication source, and a notifier closure
    /// wired to it exactly as the components wiring does.
    async fn system_replication_repo(storage: Arc<StorageEngine>) -> SystemReplicationRepo {
        // The source bucket must exist before the replication is created; in
        // production the first system event creates it.
        storage
            .create_system_bucket(SYSTEM_BUCKET_NAME, BucketSettings::default())
            .await
            .unwrap();
        let mut repo = ReplicationRepoBuilder::new(Cfg::default())
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

    fn notifier_for(repo: &SystemReplicationRepo) -> ReplicationNotifier {
        let repo = Arc::clone(repo);
        Arc::new(move |notification| {
            let repo = Arc::clone(&repo);
            Box::pin(async move { repo.write().await?.notify(notification).await })
        })
    }

    async fn pending_records(repo: &SystemReplicationRepo) -> u64 {
        repo.write()
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
        let mut writer = writer(storage);
        writer
            .set_replication_notifier(Some(notifier_for(&repo)))
            .await;

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
    #[case::replications(SystemEventKind::Replication)]
    #[case::logs(SystemEventKind::Log)]
    #[tokio::test]
    async fn non_replicable_events_do_not_notify(
        #[future] storage: Arc<StorageEngine>,
        #[case] kind: SystemEventKind,
    ) {
        let storage = storage.await;
        let repo = system_replication_repo(Arc::clone(&storage)).await;
        let mut writer = writer(storage);
        writer
            .set_replication_notifier(Some(notifier_for(&repo)))
            .await;

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
        let mut writer = writer(storage);
        writer
            .set_replication_notifier(Some(notifier_for(&repo)))
            .await;
        // Shutdown detaches the notifier before stopping replication workers.
        writer.set_replication_notifier(None).await;

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
        let mut writer = writer(storage);
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&calls);
        writer
            .set_replication_notifier(Some(Arc::new(move |_notification| {
                seen.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Err(internal_server_error!("replication is down")) })
            })))
            .await;

        writer
            .log_event(system_event(SystemEventKind::Usage, true))
            .await
            .expect("a replication failure must never fail the system-event write");
        assert_eq!(calls.load(Ordering::SeqCst), 1, "notifier must be invoked");
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
