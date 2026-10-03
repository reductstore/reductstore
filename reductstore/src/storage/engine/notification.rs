// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::replication::{ReplicationNotifier, Transaction, TransactionNotification};
use crate::storage::bucket::update_records::UpdateLabelsMulti;
use crate::storage::engine::StorageEngine;
use crate::storage::entry::update_labels::UpdateLabels;
use async_trait::async_trait;
use log::warn;
use reduct_base::error::ReductError;
use reduct_base::io::{RecordMeta, WriteChunk, WriteRecord};
use reduct_base::Labels;
use std::collections::BTreeMap;
use std::time::Duration;

type EntryUpdateResult = BTreeMap<u64, Result<Labels, ReductError>>;

impl StorageEngine {
    /// Register (`Some`) or clear (`None`) the notifier of replications.
    ///
    /// The engine notifies it about every record written with [`StorageEngine::begin_write`]
    /// and every record updated with [`StorageEngine::update_labels`] or
    /// [`StorageEngine::update_labels_multi`], whoever writes it: the HTTP or Zenoh API,
    /// the system logger or a replication to a bucket of the same instance.
    pub(crate) fn set_replication_notifier(
        &self,
        notifier: Option<ReplicationNotifier>,
    ) -> Result<(), ReductError> {
        *self.replication_notifier.write()? = notifier;
        Ok(())
    }

    /// Update labels of records in an entry and notify replications about the updated records.
    pub(crate) async fn update_labels(
        &self,
        bucket_name: &str,
        entry_name: &str,
        updates: Vec<UpdateLabels>,
    ) -> Result<EntryUpdateResult, ReductError> {
        let entry = self
            .get_bucket(bucket_name)
            .await?
            .upgrade()?
            .get_entry(entry_name)
            .await?
            .upgrade()?;
        let result = entry.update_labels(updates).await?;
        self.notify_updates(bucket_name, entry_name, &result)
            .await?;
        Ok(result)
    }

    /// Update labels of records in several entries of a bucket and notify replications about
    /// the updated records.
    pub(crate) async fn update_labels_multi(
        &self,
        bucket_name: &str,
        updates: Vec<UpdateLabelsMulti>,
    ) -> Result<BTreeMap<String, EntryUpdateResult>, ReductError> {
        let result = self
            .get_bucket(bucket_name)
            .await?
            .upgrade()?
            .update_labels(updates)
            .await?;
        for (entry_name, entry_result) in &result {
            self.notify_updates(bucket_name, entry_name, entry_result)
                .await?;
        }
        Ok(result)
    }

    /// Wrap the writer to notify replications when the record is written completely.
    pub(super) fn notify_on_finish(
        &self,
        writer: Box<dyn WriteRecord + Sync + Send>,
        bucket_name: &str,
        entry_name: &str,
        time: u64,
        labels: Labels,
    ) -> Result<Box<dyn WriteRecord + Sync + Send>, ReductError> {
        let Some(notifier) = self.replication_notifier()? else {
            return Ok(writer);
        };

        Ok(Box::new(NotifyingWriter {
            writer,
            notifier,
            notification: Some(TransactionNotification {
                bucket: bucket_name.to_string(),
                entry: entry_name.to_string(),
                meta: RecordMeta::builder().timestamp(time).labels(labels).build(),
                event: Transaction::WriteRecord(time),
            }),
        }))
    }

    pub(super) fn replication_notifier(&self) -> Result<Option<ReplicationNotifier>, ReductError> {
        Ok(self.replication_notifier.read()?.clone())
    }

    async fn notify_updates(
        &self,
        bucket_name: &str,
        entry_name: &str,
        result: &EntryUpdateResult,
    ) -> Result<(), ReductError> {
        let Some(notifier) = self.replication_notifier()? else {
            return Ok(());
        };

        for (time, labels) in result {
            let Ok(labels) = labels else {
                continue;
            };

            notify(
                &notifier,
                TransactionNotification {
                    bucket: bucket_name.to_string(),
                    entry: entry_name.to_string(),
                    meta: RecordMeta::builder()
                        .timestamp(*time)
                        .labels(labels.clone())
                        .build(),
                    event: Transaction::UpdateRecord(*time),
                },
            )
            .await;
        }
        Ok(())
    }
}

/// A writer which notifies replications after the last chunk of the record is written.
struct NotifyingWriter {
    writer: Box<dyn WriteRecord + Sync + Send>,
    notifier: ReplicationNotifier,
    notification: Option<TransactionNotification>,
}

#[async_trait]
impl WriteRecord for NotifyingWriter {
    async fn send(&mut self, chunk: WriteChunk) -> Result<(), ReductError> {
        let is_last = matches!(chunk, Ok(None));
        self.writer.send(chunk).await?;
        if is_last {
            self.notify().await;
        }
        Ok(())
    }

    async fn send_timeout(
        &mut self,
        chunk: WriteChunk,
        timeout: Duration,
    ) -> Result<(), ReductError> {
        let is_last = matches!(chunk, Ok(None));
        self.writer.send_timeout(chunk, timeout).await?;
        if is_last {
            self.notify().await;
        }
        Ok(())
    }
}

impl NotifyingWriter {
    async fn notify(&mut self) {
        if let Some(notification) = self.notification.take() {
            notify(&self.notifier, notification).await;
        }
    }
}

/// The record is already stored, so a failed notification doesn't fail the write.
async fn notify(notifier: &ReplicationNotifier, notification: TransactionNotification) {
    let (bucket, entry, time) = (
        notification.bucket.clone(),
        notification.entry.clone(),
        *notification.event.timestamp(),
    );
    if let Err(err) = notifier(notification).await {
        warn!(
            "Failed to notify replications about record {}/{}/{}: {}",
            bucket, entry, time, err
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::Cfg;
    use crate::storage::engine::StorageEngine;
    use bytes::Bytes;
    use reduct_base::error::ErrorCode;
    use reduct_base::internal_server_error;
    use reduct_base::msg::bucket_api::BucketSettings;
    use rstest::{fixture, rstest};
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;

    type Notifications = Arc<Mutex<Vec<TransactionNotification>>>;

    #[rstest]
    #[case::send(false)]
    #[case::send_timeout(true)]
    #[tokio::test]
    async fn notifies_when_record_is_written_completely(
        #[future] storage: Arc<StorageEngine>,
        #[case] with_timeout: bool,
    ) {
        let storage = storage.await;
        let notifications = capture_notifications(&storage);

        let mut writer = storage
            .begin_write(
                "bucket",
                "entry",
                10,
                4,
                "text/plain".into(),
                labels("a", "1"),
            )
            .await
            .unwrap();
        send(&mut writer, Ok(Some(Bytes::from("data"))), with_timeout).await;
        assert!(
            notifications.lock().unwrap().is_empty(),
            "the record isn't written completely yet"
        );

        send(&mut writer, Ok(None), with_timeout).await;
        let notifications = notifications.lock().unwrap();
        assert_eq!(notifications.len(), 1);
        assert_eq!(notifications[0].bucket, "bucket");
        assert_eq!(notifications[0].entry, "entry");
        assert_eq!(notifications[0].event, Transaction::WriteRecord(10));
        assert_eq!(notifications[0].meta.labels(), &labels("a", "1"));
    }

    #[rstest]
    #[tokio::test]
    async fn does_not_notify_about_aborted_write(#[future] storage: Arc<StorageEngine>) {
        let storage = storage.await;
        let notifications = capture_notifications(&storage);

        let mut writer = storage
            .begin_write("bucket", "entry", 10, 8, "text/plain".into(), Labels::new())
            .await
            .unwrap();
        writer.send(Ok(Some(Bytes::from("data")))).await.unwrap();
        writer
            .send(Err(internal_server_error!("Failed to read the source")))
            .await
            .unwrap();

        assert!(notifications.lock().unwrap().is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn writes_without_notification(#[future] storage: Arc<StorageEngine>) {
        let storage = storage.await;
        let notifications = capture_notifications(&storage);

        write(&storage, "entry", 10, Labels::new(), false).await;

        assert!(notifications.lock().unwrap().is_empty());
        assert!(storage
            .get_bucket("bucket")
            .await
            .unwrap()
            .upgrade()
            .unwrap()
            .get_entry("entry")
            .await
            .is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn stops_notifying_when_notifier_is_cleared(#[future] storage: Arc<StorageEngine>) {
        let storage = storage.await;
        let notifications = capture_notifications(&storage);
        storage.set_replication_notifier(None).unwrap();

        write(&storage, "entry", 10, Labels::new(), true).await;
        storage
            .update_labels("bucket", "entry", vec![update(10, "a", "1")])
            .await
            .unwrap();

        assert!(notifications.lock().unwrap().is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn keeps_record_when_notification_fails(#[future] storage: Arc<StorageEngine>) {
        let storage = storage.await;
        storage
            .set_replication_notifier(Some(Arc::new(|_| {
                Box::pin(async { Err(internal_server_error!("Replication is stopped")) })
            })))
            .unwrap();

        write(&storage, "entry", 10, Labels::new(), true).await;
        let result = storage
            .update_labels("bucket", "entry", vec![update(10, "a", "1")])
            .await
            .unwrap();

        assert_eq!(result[&10].as_ref().unwrap(), &labels("a", "1"));
    }

    #[rstest]
    #[tokio::test]
    async fn notifies_about_updated_records_only(#[future] storage: Arc<StorageEngine>) {
        let storage = storage.await;
        write(&storage, "entry", 10, labels("a", "1"), false).await;
        let notifications = capture_notifications(&storage);

        let result = storage
            .update_labels(
                "bucket",
                "entry",
                vec![update(10, "b", "2"), update(20, "b", "2")],
            )
            .await
            .unwrap();

        assert_eq!(
            result[&20].as_ref().unwrap_err().status(),
            ErrorCode::NotFound
        );
        let notifications = notifications.lock().unwrap();
        assert_eq!(
            notifications.len(),
            1,
            "only the existing record is updated"
        );
        assert_eq!(notifications[0].event, Transaction::UpdateRecord(10));
        assert_eq!(
            notifications[0].meta.labels(),
            &Labels::from_iter([
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "2".to_string())
            ]),
            "notifies with the resulting labels"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn fails_to_update_labels_of_missing_entry(#[future] storage: Arc<StorageEngine>) {
        let storage = storage.await;
        let notifications = capture_notifications(&storage);

        let err = storage
            .update_labels("bucket", "missing", vec![update(10, "a", "1")])
            .await
            .unwrap_err();

        assert_eq!(err.status(), ErrorCode::NotFound);
        assert!(notifications.lock().unwrap().is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn notifies_about_records_updated_in_several_entries(
        #[future] storage: Arc<StorageEngine>,
    ) {
        let storage = storage.await;
        write(&storage, "entry-1", 10, Labels::new(), false).await;
        write(&storage, "entry-2", 20, Labels::new(), false).await;
        let notifications = capture_notifications(&storage);

        let updates = [("entry-1", 10), ("entry-2", 20), ("missing", 30)]
            .into_iter()
            .map(|(entry_name, time)| UpdateLabelsMulti {
                entry_name: entry_name.to_string(),
                time,
                update: labels("a", "1"),
                remove: HashSet::new(),
            })
            .collect();
        let result = storage
            .update_labels_multi("bucket", updates)
            .await
            .unwrap();

        assert!(result["missing"][&30].is_err());
        let notified: Vec<_> = notifications
            .lock()
            .unwrap()
            .iter()
            .map(|notification| (notification.entry.clone(), notification.event.clone()))
            .collect();
        assert_eq!(
            notified,
            vec![
                ("entry-1".to_string(), Transaction::UpdateRecord(10)),
                ("entry-2".to_string(), Transaction::UpdateRecord(20)),
            ]
        );
    }

    #[fixture]
    async fn storage() -> Arc<StorageEngine> {
        let cfg = Cfg {
            data_path: tempdir().unwrap().keep(),
            ..Cfg::default()
        };
        let storage = StorageEngine::builder()
            .with_data_path(cfg.data_path.clone())
            .with_cfg(cfg)
            .build()
            .await;
        storage
            .create_bucket("bucket", BucketSettings::default())
            .await
            .unwrap();
        Arc::new(storage)
    }

    fn capture_notifications(storage: &StorageEngine) -> Notifications {
        let notifications = Notifications::default();
        let captured = Arc::clone(&notifications);
        storage
            .set_replication_notifier(Some(Arc::new(move |notification| {
                captured.lock().unwrap().push(notification);
                Box::pin(async { Ok(()) })
            })))
            .unwrap();
        notifications
    }

    async fn write(
        storage: &StorageEngine,
        entry_name: &str,
        time: u64,
        labels: Labels,
        notify: bool,
    ) {
        let mut writer = if notify {
            storage
                .begin_write("bucket", entry_name, time, 4, "text/plain".into(), labels)
                .await
        } else {
            storage
                .begin_write_without_notification(
                    "bucket",
                    entry_name,
                    time,
                    4,
                    "text/plain".into(),
                    labels,
                )
                .await
        }
        .unwrap();
        writer.send(Ok(Some(Bytes::from("data")))).await.unwrap();
        writer.send(Ok(None)).await.unwrap();
    }

    async fn send(
        writer: &mut Box<dyn WriteRecord + Sync + Send>,
        chunk: WriteChunk,
        with_timeout: bool,
    ) {
        if with_timeout {
            writer
                .send_timeout(chunk, Duration::from_secs(1))
                .await
                .unwrap();
        } else {
            writer.send(chunk).await.unwrap();
        }
    }

    fn update(time: u64, name: &str, value: &str) -> UpdateLabels {
        UpdateLabels {
            time,
            update: labels(name, value),
            remove: HashSet::new(),
        }
    }

    fn labels(name: &str, value: &str) -> Labels {
        Labels::from_iter([(name.to_string(), value.to_string())])
    }
}
