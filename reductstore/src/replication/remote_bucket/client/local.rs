// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::replication::remote_bucket::client::{BoxedBucketApi, ReductBucketApi, ReductClientApi};
use crate::replication::remote_bucket::{ErrorRecordMap, LocalDestination};
use crate::replication::{Transaction, TransactionNotification};
use crate::storage::entry::update_labels::UpdateLabels;
use async_trait::async_trait;
use log::debug;
use reduct_base::error::ReductError;
use reduct_base::io::{BoxedReadRecord, RecordMeta};
use reduct_base::msg::bucket_api::BucketSettings;
use reduct_base::Labels;
use std::collections::HashSet;

const LOCAL_ENDPOINT: &str = "local://";

/// Client writing to a bucket of the same instance directly through the storage engine.
pub(super) struct LocalClient {
    destination: LocalDestination,
}

impl LocalClient {
    pub(super) fn new(destination: LocalDestination) -> Self {
        Self { destination }
    }

    fn bucket(&self, bucket_name: &str) -> BoxedBucketApi {
        Box::new(LocalBucket {
            destination: self.destination.clone(),
            bucket_name: bucket_name.to_string(),
        })
    }
}

#[async_trait]
impl ReductClientApi for LocalClient {
    async fn get_bucket(&self, bucket_name: &str) -> Result<BoxedBucketApi, ReductError> {
        self.destination.storage.get_bucket(bucket_name).await?;
        Ok(self.bucket(bucket_name))
    }

    async fn create_bucket(&self, bucket_name: &str) -> Result<BoxedBucketApi, ReductError> {
        // the same settings as the HTTP API applies to a request without a body
        self.destination
            .storage
            .create_bucket(bucket_name, BucketSettings::default())
            .await?;
        Ok(self.bucket(bucket_name))
    }

    fn endpoint(&self) -> &str {
        LOCAL_ENDPOINT
    }
}

/// A bucket of the same instance.
///
/// Errors are split like in the HTTP API: a failure of the whole batch (e.g. a missing bucket
/// or entry) is returned as `Err`, a failure of a single record is reported by its timestamp
/// in the error map, and the rest of the batch is still processed.
struct LocalBucket {
    destination: LocalDestination,
    bucket_name: String,
}

#[async_trait]
impl ReductBucketApi for LocalBucket {
    async fn write_batch(
        &self,
        entry: &str,
        mut records: Vec<BoxedReadRecord>,
    ) -> Result<ErrorRecordMap, ReductError> {
        // a missing bucket fails the whole batch instead of every record separately
        self.destination
            .storage
            .get_bucket(&self.bucket_name)
            .await?;

        records.sort_by_key(|record| record.meta().timestamp());

        let mut errors = ErrorRecordMap::new();
        for record in records {
            let time = record.meta().timestamp();
            let labels = record.meta().labels().clone();
            match self.write_record(entry, record).await {
                Ok(()) => self.notify(entry, Transaction::WriteRecord(time), labels),
                Err(err) => {
                    errors.insert(time, err);
                }
            }
        }

        Ok(errors)
    }

    async fn update_batch(
        &self,
        entry: &str,
        records: &Vec<BoxedReadRecord>,
    ) -> Result<ErrorRecordMap, ReductError> {
        // a missing bucket or entry fails the whole batch,
        // so that the caller can write the records instead of updating them
        let entry_to_update = self
            .destination
            .storage
            .get_bucket(&self.bucket_name)
            .await?
            .upgrade()?
            .get_entry(entry)
            .await?
            .upgrade()?;

        let updates = records
            .iter()
            .map(|record| labels_update(record.meta()))
            .collect();

        let mut errors = ErrorRecordMap::new();
        for (time, result) in entry_to_update.update_labels(updates).await? {
            match result {
                Ok(labels) => self.notify(entry, Transaction::UpdateRecord(time), labels),
                Err(err) => {
                    errors.insert(time, err);
                }
            }
        }

        Ok(errors)
    }

    fn endpoint(&self) -> &str {
        LOCAL_ENDPOINT
    }

    fn name(&self) -> &str {
        &self.bucket_name
    }
}

impl LocalBucket {
    /// Copy the record chunk by chunk, so that it is never buffered as a whole.
    async fn write_record(
        &self,
        entry: &str,
        mut record: BoxedReadRecord,
    ) -> Result<(), ReductError> {
        let meta = record.meta();
        let time = meta.timestamp();
        let mut writer = self
            .destination
            .storage
            .begin_write(
                &self.bucket_name,
                entry,
                time,
                meta.content_length(),
                meta.content_type().to_string(),
                meta.labels().clone(),
            )
            .await?;

        let io_timeout = self.destination.io_timeout;
        while let Some(chunk) = record.read_chunk() {
            match chunk {
                Ok(chunk) => writer.send_timeout(Ok(Some(chunk)), io_timeout).await?,
                Err(err) => {
                    // abort the destination record: it is marked as errored
                    // instead of hanging unfinished
                    if let Err(abort_err) = writer.send_timeout(Err(err.clone()), io_timeout).await
                    {
                        debug!(
                            "Failed to abort writing of {}/{}/{}: {}",
                            self.bucket_name, entry, time, abort_err
                        );
                    }
                    return Err(err);
                }
            }
        }

        writer.send_timeout(Ok(None), io_timeout).await
    }

    /// Notify the replications of the instance about a change in the destination bucket.
    fn notify(&self, entry: &str, event: Transaction, labels: Labels) {
        let meta = RecordMeta::builder()
            .timestamp(*event.timestamp())
            .labels(labels)
            .build();

        (self.destination.notifier)(TransactionNotification {
            bucket: self.bucket_name.clone(),
            entry: entry.to_string(),
            meta,
            event,
        });
    }
}

/// Convert labels of a source record to an update in the same way as the HTTP API does:
/// an empty value removes the label.
fn labels_update(meta: &RecordMeta) -> UpdateLabels {
    let mut update = Labels::new();
    let mut remove = HashSet::new();
    for (name, value) in meta.labels() {
        if value.is_empty() {
            remove.insert(name.clone());
        } else {
            update.insert(name.clone(), value.clone());
        }
    }

    UpdateLabels {
        time: meta.timestamp(),
        update,
        remove,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::Cfg;
    use crate::replication::remote_bucket::client::tests::MockRecordReader;
    use crate::replication::remote_bucket::{RemoteBucket, RemoteBucketBuilder};
    use crate::storage::engine::{StorageEngine, CHANNEL_BUFFER_SIZE, MAX_IO_BUFFER_SIZE};
    use crate::storage::proto::record::Label;
    use crate::storage::proto::{us_to_ts, Record};
    use bytes::Bytes;
    use reduct_base::error::ErrorCode;
    use reduct_base::io::ReadRecord;
    use reduct_base::{conflict, internal_server_error, not_found};
    use rstest::{fixture, rstest};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tempfile::tempdir;

    const BUCKET: &str = "dst";
    const ENTRY: &str = "entry";

    mod write_batch {
        use super::*;

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn writes_records_with_metadata_in_timestamp_order(#[future] env: Env) {
            let env = env.await;
            let records = vec![
                record(30, "image/png", "third", &[("kind", "c")]),
                record(10, "text/plain", "first", &[("kind", "a"), ("shared", "x")]),
                record(20, "application/json", "{}", &[]),
            ];

            let errors = env
                .bucket(BUCKET)
                .write_batch(ENTRY, records)
                .await
                .unwrap();

            assert!(errors.is_empty());
            let (meta, body) = env.read(BUCKET, ENTRY, 10).await.unwrap();
            assert_eq!(body, b"first");
            assert_eq!(meta.content_type(), "text/plain");
            assert_eq!(meta.labels(), &to_labels(&[("kind", "a"), ("shared", "x")]));
            let (meta, body) = env.read(BUCKET, ENTRY, 20).await.unwrap();
            assert_eq!(body, b"{}");
            assert_eq!(meta.content_type(), "application/json");
            assert!(meta.labels().is_empty());
            let (meta, body) = env.read(BUCKET, ENTRY, 30).await.unwrap();
            assert_eq!(body, b"third");
            assert_eq!(meta.content_type(), "image/png");
            assert_eq!(meta.labels(), &to_labels(&[("kind", "c")]));

            let notifications = env.notifications();
            assert_eq!(
                events(&notifications),
                vec![
                    Transaction::WriteRecord(10),
                    Transaction::WriteRecord(20),
                    Transaction::WriteRecord(30)
                ],
                "records are written and notified in timestamp order"
            );
            for notification in &notifications {
                assert_eq!(notification.bucket, BUCKET);
                assert_eq!(notification.entry, ENTRY);
                assert_eq!(
                    notification.meta.timestamp(),
                    *notification.event.timestamp()
                );
            }
            assert_eq!(
                notifications[0].meta.labels(),
                &to_labels(&[("kind", "a"), ("shared", "x")])
            );
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn streams_record_larger_than_io_buffer(#[future] env: Env) {
            let env = env.await;
            // more chunks than the destination writer can buffer, so the copy has to wait for it
            let chunk_size = 64 * 1024;
            let body: Vec<u8> = (0..chunk_size * 3 * CHANNEL_BUFFER_SIZE)
                .map(|i| (i % 251) as u8)
                .collect();
            assert!(body.len() > MAX_IO_BUFFER_SIZE);
            let chunks = body
                .chunks(chunk_size)
                .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
                .collect();

            let errors = env
                .bucket(BUCKET)
                .write_batch(ENTRY, vec![chunked_record(1, body.len() as u64, chunks)])
                .await
                .unwrap();

            assert!(errors.is_empty());
            let (meta, stored) = env.read(BUCKET, ENTRY, 1).await.unwrap();
            assert_eq!(meta.content_length(), body.len() as u64);
            assert!(stored == body, "the stored content differs from the source");
            assert_eq!(
                events(&env.notifications()),
                vec![Transaction::WriteRecord(1)]
            );
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn writes_empty_record(#[future] env: Env) {
            let env = env.await;

            let errors = env
                .bucket(BUCKET)
                .write_batch(ENTRY, vec![record(1, "text/plain", "", &[("a", "b")])])
                .await
                .unwrap();

            assert!(errors.is_empty());
            let (meta, body) = env.read(BUCKET, ENTRY, 1).await.unwrap();
            assert!(body.is_empty());
            assert_eq!(meta.labels(), &to_labels(&[("a", "b")]));
            assert_eq!(
                events(&env.notifications()),
                vec![Transaction::WriteRecord(1)]
            );
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn reports_conflict_for_existing_timestamp_and_writes_the_rest(#[future] env: Env) {
            let env = env.await;
            env.write(ENTRY, 2, "old", &[]).await;

            let errors = env
                .bucket(BUCKET)
                .write_batch(
                    ENTRY,
                    vec![
                        record(1, "text/plain", "one", &[]),
                        record(2, "text/plain", "new", &[]),
                        record(3, "text/plain", "three", &[]),
                    ],
                )
                .await
                .unwrap();

            assert_eq!(
                errors,
                ErrorRecordMap::from([(2, conflict!("A record with timestamp 2 already exists"))])
            );
            assert_eq!(env.read(BUCKET, ENTRY, 1).await.unwrap().1, b"one");
            assert_eq!(
                env.read(BUCKET, ENTRY, 2).await.unwrap().1,
                b"old",
                "the existing record is not overwritten"
            );
            assert_eq!(env.read(BUCKET, ENTRY, 3).await.unwrap().1, b"three");
            assert_eq!(
                events(&env.notifications()),
                vec![Transaction::WriteRecord(1), Transaction::WriteRecord(3)],
                "the conflicting record is not notified"
            );
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn aborts_only_the_record_which_fails_to_read(#[future] env: Env) {
            let env = env.await;
            let broken = chunked_record(
                1,
                6,
                vec![
                    Ok(Bytes::from("abc")),
                    Err(internal_server_error!("disk failure")),
                ],
            );

            let errors = env
                .bucket(BUCKET)
                .write_batch(ENTRY, vec![broken, record(2, "text/plain", "two", &[])])
                .await
                .unwrap();

            assert_eq!(
                errors,
                ErrorRecordMap::from([(1, internal_server_error!("disk failure"))])
            );
            let err = env.read(BUCKET, ENTRY, 1).await.err().unwrap();
            assert_eq!(err.status(), ErrorCode::InternalServerError);
            assert!(
                err.message().contains("is broken"),
                "the aborted record is marked as errored: {err}"
            );
            assert_eq!(env.read(BUCKET, ENTRY, 2).await.unwrap().1, b"two");
            assert_eq!(
                events(&env.notifications()),
                vec![Transaction::WriteRecord(2)]
            );

            let errors = env
                .bucket(BUCKET)
                .write_batch(ENTRY, vec![record(1, "text/plain", "abcdef", &[])])
                .await
                .unwrap();
            assert!(errors.is_empty(), "the aborted record can be written again");
            assert_eq!(env.read(BUCKET, ENTRY, 1).await.unwrap().1, b"abcdef");
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn reports_invalid_entry_name_for_each_record(#[future] env: Env) {
            let env = env.await;

            let errors = env
                .bucket(BUCKET)
                .write_batch(
                    "/invalid/",
                    vec![
                        record(1, "text/plain", "one", &[]),
                        record(2, "text/plain", "two", &[]),
                    ],
                )
                .await
                .unwrap();

            assert_eq!(errors.keys().copied().collect::<Vec<_>>(), vec![1, 2]);
            assert!(errors
                .values()
                .all(|err| err.status() == ErrorCode::UnprocessableEntity));
            assert!(env.notifications().is_empty());
        }

        #[rstest]
        #[tokio::test]
        async fn fails_when_bucket_is_missing(#[future] env: Env) {
            let env = env.await;

            let err = env
                .bucket("missing")
                .write_batch(ENTRY, vec![record(1, "text/plain", "one", &[])])
                .await
                .unwrap_err();

            assert_eq!(err, not_found!("Bucket 'missing' is not found"));
            assert!(env.notifications().is_empty());
        }
    }

    mod update_batch {
        use super::*;

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn updates_labels_and_notifies_with_resulting_labels(#[future] env: Env) {
            let env = env.await;
            env.write(
                ENTRY,
                1,
                "one",
                &[("keep", "1"), ("change", "old"), ("drop", "x")],
            )
            .await;
            env.write(ENTRY, 2, "two", &[("other", "2")]).await;

            // the source labels are applied as they are: an empty value removes a label
            let source = vec![record(
                1,
                "text/plain",
                "one",
                &[("change", "new"), ("added", "yes"), ("drop", "")],
            )];
            let errors = env
                .bucket(BUCKET)
                .update_batch(ENTRY, &source)
                .await
                .unwrap();

            assert!(errors.is_empty());
            let expected = to_labels(&[("keep", "1"), ("change", "new"), ("added", "yes")]);
            let (meta, body) = env.read(BUCKET, ENTRY, 1).await.unwrap();
            assert_eq!(meta.labels(), &expected);
            assert_eq!(body, b"one", "the content is not touched");
            assert_eq!(
                env.read(BUCKET, ENTRY, 2).await.unwrap().0.labels(),
                &to_labels(&[("other", "2")])
            );

            let notifications = env.notifications();
            assert_eq!(events(&notifications), vec![Transaction::UpdateRecord(1)]);
            assert_eq!(notifications[0].bucket, BUCKET);
            assert_eq!(notifications[0].entry, ENTRY);
            assert_eq!(notifications[0].meta.timestamp(), 1);
            assert_eq!(notifications[0].meta.labels(), &expected);
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn reports_missing_record_and_updates_the_rest(#[future] env: Env) {
            let env = env.await;
            env.write(ENTRY, 1, "one", &[("a", "1")]).await;
            let source = vec![
                record(1, "text/plain", "one", &[("a", "2")]),
                record(99, "text/plain", "missing", &[("a", "3")]),
            ];

            let errors = env
                .bucket(BUCKET)
                .update_batch(ENTRY, &source)
                .await
                .unwrap();

            assert_eq!(errors.keys().copied().collect::<Vec<_>>(), vec![99]);
            assert_eq!(errors[&99].status(), ErrorCode::NotFound);
            assert_eq!(
                env.read(BUCKET, ENTRY, 1).await.unwrap().0.labels(),
                &to_labels(&[("a", "2")])
            );
            assert_eq!(
                events(&env.notifications()),
                vec![Transaction::UpdateRecord(1)],
                "the missing record is not notified"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn fails_when_entry_is_missing(#[future] env: Env) {
            let env = env.await;

            let err = env
                .bucket(BUCKET)
                .update_batch(
                    "missing",
                    &vec![record(1, "text/plain", "one", &[("a", "b")])],
                )
                .await
                .unwrap_err();

            assert_eq!(err.status(), ErrorCode::NotFound);
            assert!(env.notifications().is_empty());
        }

        #[rstest]
        #[tokio::test]
        async fn fails_when_bucket_is_missing(#[future] env: Env) {
            let env = env.await;

            let err = env
                .bucket("missing")
                .update_batch(ENTRY, &vec![record(1, "text/plain", "one", &[("a", "b")])])
                .await
                .unwrap_err();

            assert_eq!(err, not_found!("Bucket 'missing' is not found"));
            assert!(env.notifications().is_empty());
        }
    }

    mod client {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn gets_existing_bucket_only(#[future] env: Env) {
            let env = env.await;
            let client = LocalClient::new(env.destination.clone());

            let bucket = client.get_bucket(BUCKET).await.unwrap();
            assert_eq!(bucket.name(), BUCKET);
            assert_eq!(bucket.endpoint(), "local://");
            assert_eq!(client.endpoint(), "local://");

            let err = client.get_bucket("missing").await.err().unwrap();
            assert_eq!(err, not_found!("Bucket 'missing' is not found"));
        }

        #[rstest]
        #[tokio::test]
        async fn get_or_create_bucket_creates_missing_bucket(#[future] env: Env) {
            let env = env.await;
            let client = LocalClient::new(env.destination.clone());

            let bucket = client.get_or_create_bucket("created").await.unwrap();

            assert_eq!(bucket.name(), "created");
            assert!(env.storage.get_bucket("created").await.is_ok());
        }

        #[rstest]
        #[tokio::test]
        async fn create_bucket_fails_with_conflict_when_bucket_exists(#[future] env: Env) {
            let env = env.await;
            let client = LocalClient::new(env.destination.clone());

            let err = client.create_bucket(BUCKET).await.err().unwrap();
            assert_eq!(err, conflict!("Bucket 'dst' already exists"));

            let bucket = client.get_or_create_bucket(BUCKET).await.unwrap();
            assert_eq!(bucket.name(), BUCKET, "an existing bucket is reused");
        }
    }

    mod remote_bucket {
        use super::*;

        #[rstest]
        #[case::missing_entry(false)]
        #[case::missing_record(true)]
        #[tokio::test(flavor = "multi_thread")]
        async fn creates_record_missing_at_destination_on_update(
            #[future] env: Env,
            #[case] entry_exists: bool,
        ) {
            let env = env.await;
            if entry_exists {
                env.write(ENTRY, 1, "existing", &[]).await;
            }
            let mut remote_bucket = local_remote_bucket(&env, BUCKET);

            let update = (
                record(2, "text/plain", "created", &[("a", "b")]),
                Transaction::UpdateRecord(2),
            );
            let errors = remote_bucket
                .write_batch(ENTRY, vec![update])
                .await
                .unwrap();

            assert!(errors.is_empty());
            assert!(remote_bucket.is_active());
            let (meta, body) = env.read(BUCKET, ENTRY, 2).await.unwrap();
            assert_eq!(body, b"created");
            assert_eq!(meta.labels(), &to_labels(&[("a", "b")]));
            assert_eq!(
                events(&env.notifications()),
                vec![Transaction::WriteRecord(2)],
                "the record is created, not updated"
            );
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn updates_existing_record_without_rewriting_it(#[future] env: Env) {
            let env = env.await;
            env.write(ENTRY, 1, "one", &[("a", "1")]).await;
            let mut remote_bucket = local_remote_bucket(&env, BUCKET);

            let update = (
                record(1, "text/plain", "other", &[("a", "2")]),
                Transaction::UpdateRecord(1),
            );
            let errors = remote_bucket
                .write_batch(ENTRY, vec![update])
                .await
                .unwrap();

            assert!(errors.is_empty());
            assert!(remote_bucket.is_active());
            let (meta, body) = env.read(BUCKET, ENTRY, 1).await.unwrap();
            assert_eq!(body, b"one");
            assert_eq!(meta.labels(), &to_labels(&[("a", "2")]));
            assert_eq!(
                events(&env.notifications()),
                vec![Transaction::UpdateRecord(1)]
            );
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn creates_missing_destination_bucket_on_first_write(#[future] env: Env) {
            let env = env.await;
            let mut remote_bucket = local_remote_bucket(&env, "created");
            assert!(!remote_bucket.is_active());

            let write = (
                record(1, "text/plain", "one", &[]),
                Transaction::WriteRecord(1),
            );
            let errors = remote_bucket.write_batch(ENTRY, vec![write]).await.unwrap();

            assert!(errors.is_empty());
            assert!(remote_bucket.is_active());
            assert_eq!(env.read("created", ENTRY, 1).await.unwrap().1, b"one");
            assert_eq!(env.notifications()[0].bucket, "created");
        }
    }

    /// The destination bucket and the notifications sent by the local client.
    struct Env {
        storage: Arc<StorageEngine>,
        destination: LocalDestination,
        notifications: Arc<Mutex<Vec<TransactionNotification>>>,
    }

    impl Env {
        fn bucket(&self, name: &str) -> LocalBucket {
            LocalBucket {
                destination: self.destination.clone(),
                bucket_name: name.to_string(),
            }
        }

        fn notifications(&self) -> Vec<TransactionNotification> {
            self.notifications.lock().unwrap().clone()
        }

        /// Write a record to the destination bucket directly through the storage engine.
        async fn write(&self, entry: &str, time: u64, body: &str, labels: &[(&str, &str)]) {
            let mut writer = self
                .storage
                .begin_write(
                    BUCKET,
                    entry,
                    time,
                    body.len() as u64,
                    "text/plain".to_string(),
                    to_labels(labels),
                )
                .await
                .unwrap();
            writer
                .send(Ok(Some(Bytes::from(body.to_string()))))
                .await
                .unwrap();
            writer.send(Ok(None)).await.unwrap();
        }

        /// Read a record back through the storage engine.
        async fn read(
            &self,
            bucket: &str,
            entry: &str,
            time: u64,
        ) -> Result<(RecordMeta, Vec<u8>), ReductError> {
            let mut reader = self
                .storage
                .get_bucket(bucket)
                .await?
                .upgrade()?
                .get_entry(entry)
                .await?
                .upgrade()?
                .begin_read(time)
                .await?;

            let mut body = Vec::new();
            while let Some(chunk) = reader.read_chunk() {
                body.extend_from_slice(&chunk?);
            }
            Ok((reader.meta().clone(), body))
        }
    }

    #[fixture]
    async fn env() -> Env {
        let cfg = Cfg {
            data_path: tempdir().unwrap().keep(),
            ..Cfg::default()
        };
        let storage = Arc::new(
            StorageEngine::builder()
                .with_data_path(cfg.data_path.clone())
                .with_cfg(cfg)
                .build()
                .await,
        );
        storage
            .create_bucket(BUCKET, BucketSettings::default())
            .await
            .unwrap();

        let notifications = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&notifications);
        let destination = LocalDestination {
            storage: Arc::clone(&storage),
            notifier: Arc::new(move |notification| captured.lock().unwrap().push(notification)),
            io_timeout: Duration::from_secs(5),
        };

        Env {
            storage,
            destination,
            notifications,
        }
    }

    /// A source record which delivers the body in one chunk.
    fn record(
        time: u64,
        content_type: &str,
        body: &str,
        labels: &[(&str, &str)],
    ) -> BoxedReadRecord {
        source_record(
            time,
            content_type,
            labels,
            body.len() as u64,
            vec![Ok(Bytes::from(body.to_string()))],
        )
    }

    /// A source record which delivers the given chunks; an `Err` chunk simulates a failed read.
    fn chunked_record(
        time: u64,
        content_length: u64,
        chunks: Vec<Result<Bytes, ReductError>>,
    ) -> BoxedReadRecord {
        source_record(
            time,
            "application/octet-stream",
            &[],
            content_length,
            chunks,
        )
    }

    fn source_record(
        time: u64,
        content_type: &str,
        labels: &[(&str, &str)],
        content_length: u64,
        chunks: Vec<Result<Bytes, ReductError>>,
    ) -> BoxedReadRecord {
        let (tx, rx) = crossbeam_channel::unbounded();
        for chunk in chunks {
            tx.send(chunk).unwrap();
        }

        MockRecordReader::form_record_with_rx(
            rx,
            Record {
                timestamp: Some(us_to_ts(&time)),
                labels: labels
                    .iter()
                    .map(|(name, value)| Label {
                        name: name.to_string(),
                        value: value.to_string(),
                    })
                    .collect(),
                begin: 0,
                end: content_length,
                content_type: content_type.to_string(),
                state: 0,
            },
        )
    }

    fn local_remote_bucket(env: &Env, bucket_name: &str) -> Box<dyn RemoteBucket + Send + Sync> {
        RemoteBucketBuilder::new()
            .bucket_name(bucket_name)
            .local(env.destination.clone())
            .build()
            .unwrap()
    }

    fn to_labels(labels: &[(&str, &str)]) -> Labels {
        labels
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect()
    }

    fn events(notifications: &[TransactionNotification]) -> Vec<Transaction> {
        notifications
            .iter()
            .map(|notification| notification.event.clone())
            .collect()
    }
}
