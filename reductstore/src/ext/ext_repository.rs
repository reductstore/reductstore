// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

mod create;
mod load;

use crate::asset::asset_manager::ManageStaticAsset;
use crate::ext::filter::ExtWhenFilter;
use crate::storage::query::base::QueryOptions;
use crate::storage::query::condition::{EvaluationStage, Parser};
use crate::storage::query::QueryRx;
use async_trait::async_trait;
use dlopen2::wrapper::{Container, WrapperApi};
use futures_util::StreamExt;
use reduct_base::error::ErrorCode::NoContent;
use reduct_base::error::ReductError;
use reduct_base::ext::{
    BoxedCommiter, BoxedProcessor, BoxedReadRecord, BoxedRecordStream, ExtSettings, IoExtension,
};
use reduct_base::msg::entry_api::QueryEntry;
use reduct_base::{no_content, unprocessable_entity};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock as AsyncRwLock;

type IoExtRef = Arc<AsyncRwLock<Box<dyn IoExtension + Send + Sync>>>;
type IoExtMap = HashMap<String, IoExtRef>;

#[derive(WrapperApi)]
struct ExtensionApi {
    get_ext: extern "Rust" fn(settings: ExtSettings) -> *mut (dyn IoExtension + Send + Sync),
}

#[async_trait]
pub(crate) trait ManageExtensions {
    async fn register_query(
        &self,
        query_id: u64,
        bucket_name: &str,
        entry_name: &str,
        query: QueryEntry,
    ) -> Result<(), ReductError>;

    /// Fetches and processes a record from the extension.
    ///
    /// This method is called for each record that is fetched from the storage engine.
    ///
    /// # Arguments
    ///
    /// * `query_id` - The ID of the query.
    /// * `query_rx` - The receiver for the query.
    ///
    /// # Returns
    ///
    ///  204 No Content if no records are available.
    async fn fetch_and_process_record(
        &self,
        query_id: u64,
        query_rx: Arc<AsyncRwLock<QueryRx>>,
    ) -> Option<Result<BoxedReadRecord, ReductError>>;
}

pub type BoxedManageExtensions = Box<dyn ManageExtensions + Sync + Send>;

pub(crate) struct QueryContext {
    query: QueryOptions,
    condition_filter: ExtWhenFilter,
    last_access: Instant,
    current_stream: Option<Pin<BoxedRecordStream>>,

    processor: BoxedProcessor,
    commiter: BoxedCommiter,
}

struct ExtRepository {
    extension_map: IoExtMap,
    query_map: AsyncRwLock<HashMap<u64, QueryContext>>,

    #[allow(dead_code)]
    ext_wrappers: Vec<Container<ExtensionApi>>, // we need to keep the wrappers alive

    #[allow(dead_code)]
    embedded_extensions: Vec<Box<dyn ManageStaticAsset + Sync + Send>>, // we need to keep them from being cleaned up
}

#[async_trait]
impl ManageExtensions for ExtRepository {
    /// Register a query with the extension
    ///
    /// # Arguments
    ///
    /// * `query_id` - The ID of the query
    /// * `bucket_name` - The name of the bucket
    /// * `entry_name` - The name of the entry
    /// * `query_request` - The query request
    ///
    /// # Errors
    ///
    /// * `ReductError::InternalServerError` - If the query is not valid
    async fn register_query(
        &self,
        query_id: u64,
        bucket_name: &str,
        entry_name: &str,
        query_request: QueryEntry,
    ) -> Result<(), ReductError> {
        let mut query_map = self.query_map.write().await;
        let ext_params = query_request.ext.as_ref();
        let controllers = if ext_params.is_some() && ext_params.unwrap().is_object() {
            let ext_query = ext_params.unwrap().as_object().unwrap();
            if ext_query.iter().count() > 1 {
                return Err(unprocessable_entity!(
                    "Multiple extensions are not supported in query id={}",
                    query_id
                ));
            }

            let Some(name) = ext_query.keys().next() else {
                return Err(unprocessable_entity!(
                    "Extension name is not found in query id={}",
                    query_id
                ));
            };

            if let Some(ext) = self.extension_map.get(name) {
                let (processor, commiter) =
                    ext.write()
                        .await
                        .query(bucket_name, entry_name, &query_request)?;
                Some((processor, commiter))
            } else {
                return Err(unprocessable_entity!(
                    "Unknown extension '{}' in query id={}",
                    name,
                    query_id
                ));
            }
        } else {
            None
        };

        let query_options: QueryOptions = query_request.into();
        // remove expired queries
        let mut ids_to_remove = Vec::new();

        for (key, query) in query_map.iter() {
            if query.last_access.elapsed() > query.query.ttl {
                ids_to_remove.push(*key);
            }
        }

        for key in ids_to_remove {
            query_map.remove(&key);
        }

        // check if the query has references to computed labels and no extension is found
        let condition = if let Some(condition) = &query_options.when {
            let node = Parser::new().parse(condition)?;
            if controllers.is_none() && node.stage() == &EvaluationStage::Compute {
                return Err(unprocessable_entity!(
                    "There is at least one reference to computed labels but no extension is found"
                ));
            }
            Some(node)
        } else {
            None
        };

        let condition_filter = ExtWhenFilter::new(condition, query_options.strict);

        if let Some((processor, commiter)) = controllers {
            query_map.insert(query_id, {
                QueryContext {
                    query: query_options,
                    condition_filter,
                    last_access: Instant::now(),
                    current_stream: None,
                    processor,
                    commiter,
                }
            });
        }

        Ok(())
    }

    async fn fetch_and_process_record(
        &self,
        query_id: u64,
        query_rx: Arc<AsyncRwLock<QueryRx>>,
    ) -> Option<Result<BoxedReadRecord, ReductError>> {
        // TODO: The code is awkward, we need to refactor it
        // unfortunately stream! macro does not work here and crashes compiler
        let mut lock = self.query_map.write().await;
        let query = match lock.get_mut(&query_id) {
            Some(query) => query,
            None => {
                return query_rx
                    .write()
                    .await
                    .recv()
                    .await
                    .map(|record| record.map(|r| Box::new(r) as BoxedReadRecord))
            }
        };

        query.last_access = Instant::now();

        if let Some(mut current_stream) = query.current_stream.take() {
            let item = current_stream.next().await;
            query.current_stream = Some(current_stream);

            if let Some(result) = item {
                if let Err(e) = result {
                    return Some(Err(e));
                }

                let record = result.unwrap();

                return match query.condition_filter.filter_record(record) {
                    Some(result) => {
                        let record = match result {
                            Ok(record) => record,
                            Err(e) => return Some(Err(e)),
                        };

                        query.commiter.commit_record(record).await
                    }
                    None => None,
                };
            } else {
                // stream is empty, we need to process the next record
                query.current_stream = None;
            }
        }

        let Some(record) = query_rx.write().await.recv().await else {
            return Some(Err(no_content!("No content")));
        };

        let record = match record {
            Ok(record) => record,
            Err(e) => {
                return if e.status == NoContent {
                    if let Some(last_record) = query.commiter.flush().await {
                        Some(last_record)
                    } else {
                        Some(Err(e))
                    }
                } else {
                    Some(Err(e))
                }
            }
        };

        assert!(query.current_stream.is_none(), "Must be None");

        let stream = match query.processor.process_record(Box::new(record)).await {
            Ok(stream) => stream,
            Err(e) => return Some(Err(e)),
        };

        query.current_stream = Some(Box::into_pin(stream));
        None
    }
}

pub(crate) use create::create_ext_repository;

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use futures_util::Stream;
    use reduct_base::ext::{Commiter, IoExtensionInfo, Processor};
    use rstest::{fixture, rstest};

    use crate::storage::entry::RecordReader;
    use crate::storage::proto::Record;
    use mockall::mock;
    use mockall::predicate::eq;
    use prost_wkt_types::Timestamp;
    use reduct_base::io::{ReadChunk, ReadRecord, RecordMeta};
    use reduct_base::msg::server_api::ServerInfo;
    use reduct_base::Labels;
    use serde_json::json;
    use std::task::{Context, Poll};
    use tempfile::tempdir;

    mod register_query {
        use super::*;

        use std::time::Duration;

        #[rstest]
        #[tokio::test]
        async fn test_no_ext_part(mock_ext: MockIoExtension) {
            let mocked_ext_repo = mocked_ext_repo("test-ext", mock_ext);
            assert!(mocked_ext_repo
                .register_query(1, "bucket", "entry", QueryEntry::default())
                .await
                .is_ok());

            let query_map = mocked_ext_repo.query_map.read().await;
            assert_eq!(
                query_map.len(),
                0,
                "We don't need to register the query without 'ext' part"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_with_ext_part(
            mut mock_ext: MockIoExtension,
            processor: BoxedProcessor,
            commiter: BoxedCommiter,
        ) {
            let query = QueryEntry {
                ext: Some(json!({
                    "test-ext": {},
                })),
                ..Default::default()
            };

            mock_ext
                .expect_query()
                .with(eq("bucket"), eq("entry"), eq(query.clone()))
                .return_once(|_, _, _| Ok((processor, commiter)));

            let mocked_ext_repo = mocked_ext_repo("test-ext", mock_ext);

            assert!(mocked_ext_repo
                .register_query(1, "bucket", "entry", query)
                .await
                .is_ok());

            let query_map = mocked_ext_repo.query_map.read().await;
            assert_eq!(
                query_map.len(),
                1,
                "We need to register the query with 'ext' part"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_without_ext_but_computed_labels(mut mock_ext: MockIoExtension) {
            let query = QueryEntry {
                when: Some(json!({"@label": { "$eq": "value" }})),
                ..Default::default()
            };
            mock_ext.expect_query().never();

            let mocked_ext_repo = mocked_ext_repo("test", mock_ext);

            assert_eq!(
                mocked_ext_repo
                    .register_query(1, "bucket", "entry", query)
                    .await
                    .err()
                    .unwrap(),
                unprocessable_entity!(
                    "There is at least one reference to computed labels but no extension is found"
                )
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_ttl(mut mock_ext: MockIoExtension) {
            let query = QueryEntry {
                ttl: Some(1),
                ext: Some(json!({
                    "test-ext": {},
                })),
                ..Default::default()
            };

            mock_ext
                .expect_query()
                .with(eq("bucket"), eq("entry"), eq(query.clone()))
                .returning(|_, _, _| {
                    Ok((
                        Box::new(MockProcessor::new()),
                        Box::new(MockCommiter::new()),
                    ))
                })
                .times(3);

            let mocked_ext_repo = mocked_ext_repo("test-ext", mock_ext);
            assert!(mocked_ext_repo
                .register_query(1, "bucket", "entry", query.clone())
                .await
                .is_ok());

            assert!(mocked_ext_repo
                .register_query(2, "bucket", "entry", query.clone())
                .await
                .is_ok());

            {
                let query_map = mocked_ext_repo.query_map.read().await;
                assert_eq!(query_map.len(), 2);
            }

            tokio::time::sleep(Duration::from_secs(2)).await;
            assert!(mocked_ext_repo
                .register_query(3, "bucket", "entry", query)
                .await
                .is_ok());
            {
                let query_map = mocked_ext_repo.query_map.read().await;
                assert_eq!(query_map.len(), 1,);

                assert!(query_map.get(&1).is_none(), "Query 1 should be expired");
                assert!(query_map.get(&2).is_none(), "Query 2 should be expired");
                assert!(query_map.get(&3).is_some());
            }
        }

        #[rstest]
        #[case(json!({"test-ext": {}, "test-ext2": {}}),  unprocessable_entity!("Multiple extensions are not supported in query id=1"))]
        #[case(json!({"unknown-ext": {}}),  unprocessable_entity!("Unknown extension 'unknown-ext' in query id=1"))]
        #[case(json!({}),  unprocessable_entity!("Extension name is not found in query id=1"))]
        #[tokio::test]
        async fn test_error_handling(
            mut mock_ext: MockIoExtension,
            processor: BoxedProcessor,
            commiter: BoxedCommiter,
            #[case] ext_params: serde_json::Value,
            #[case] expected_error: ReductError,
        ) {
            let query = QueryEntry {
                ext: Some(ext_params),
                ..Default::default()
            };

            mock_ext
                .expect_query()
                .with(eq("bucket"), eq("entry"), eq(query.clone()))
                .return_once(|_, _, _| Ok((processor, commiter)));

            let mocked_ext_repo = mocked_ext_repo("test-ext", mock_ext);
            assert_eq!(
                mocked_ext_repo
                    .register_query(1, "bucket", "entry", query)
                    .await
                    .err()
                    .unwrap(),
                expected_error
            );
        }
    }

    mod next_processed_record {
        use super::*;
        use crate::storage::entry::RecordReader;

        use mockall::predicate;
        use reduct_base::internal_server_error;

        #[rstest]
        #[tokio::test]
        async fn test_empty_query() {
            let mocked_ext_repo = mocked_ext_repo("test-ext", MockIoExtension::new());
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            drop(tx);

            let query_rx = Arc::new(AsyncRwLock::new(rx));
            assert!(mocked_ext_repo
                .fetch_and_process_record(1, query_rx)
                .await
                .is_none(),);
        }

        #[rstest]
        #[tokio::test]
        async fn test_error_query() {
            let mocked_ext_repo = mocked_ext_repo("test-ext", MockIoExtension::new());
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let err = internal_server_error!("Test error!");
            tx.send(Err(err.clone())).await.unwrap();

            let query_rx = Arc::new(AsyncRwLock::new(rx));
            assert_eq!(
                mocked_ext_repo
                    .fetch_and_process_record(1, query_rx)
                    .await
                    .unwrap()
                    .err()
                    .unwrap(),
                err
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_no_registered_query(record_reader: RecordReader) {
            let mocked_ext_repo = mocked_ext_repo("test-ext", MockIoExtension::new());
            let (tx, rx) = tokio::sync::mpsc::channel(1);

            tx.send(Ok(record_reader)).await.unwrap();

            let query_rx = Arc::new(AsyncRwLock::new(rx));
            assert!(mocked_ext_repo
                .fetch_and_process_record(1, query_rx)
                .await
                .unwrap()
                .is_ok(),);
        }

        #[rstest]
        #[tokio::test(flavor = "current_thread")]
        async fn test_process_not_ready(
            record_reader: RecordReader,
            mut mock_ext: MockIoExtension,
            mut processor: Box<MockProcessor>,
            mut commiter: Box<MockCommiter>,
        ) {
            processor
                .expect_process_record()
                .return_once(|_| Ok(MockStream::boxed(Poll::Pending) as BoxedRecordStream));
            commiter.expect_commit_record().never();

            mock_ext
                .expect_query()
                .with(eq("bucket"), eq("entry"), predicate::always())
                .return_once(|_, _, _| Ok((processor, commiter)));

            let query = QueryEntry {
                ext: Some(json!({
                    "test1": {},
                })),
                ..Default::default()
            };

            let mocked_ext_repo = mocked_ext_repo("test1", mock_ext);

            mocked_ext_repo
                .register_query(1, "bucket", "entry", query)
                .await
                .unwrap();

            let (tx, rx) = tokio::sync::mpsc::channel(1);
            tx.send(Ok(record_reader)).await.unwrap();

            let query_rx = Arc::new(AsyncRwLock::new(rx));
            assert!(mocked_ext_repo
                .fetch_and_process_record(1, query_rx)
                .await
                .is_none());
        }

        #[rstest]
        #[tokio::test(flavor = "current_thread")]
        async fn test_process_a_record(
            record_reader: RecordReader,
            mut mock_ext: MockIoExtension,
            mut processor: Box<MockProcessor>,
            mut commiter: Box<MockCommiter>,
        ) {
            processor.expect_process_record().return_once(|_| {
                Ok(MockStream::boxed(Poll::Ready(Some(Ok(MockRecord::boxed(
                    "key", "val",
                ))))))
            });

            commiter.expect_commit_record().return_once(|_| {
                Some(Ok(
                    Box::new(MockRecord::new("key", "val")) as BoxedReadRecord
                ))
            });
            commiter.expect_flush().return_once(|| None).times(1);

            mock_ext
                .expect_query()
                .with(eq("bucket"), eq("entry"), predicate::always())
                .return_once(|_, _, _| Ok((processor, commiter)));

            let query = QueryEntry {
                ext: Some(json!({
                    "test1": {},
                })),
                ..Default::default()
            };

            let mocked_ext_repo = mocked_ext_repo("test1", mock_ext);

            mocked_ext_repo
                .register_query(1, "bucket", "entry", query)
                .await
                .unwrap();

            let (tx, rx) = tokio::sync::mpsc::channel(2);
            tx.send(Ok(record_reader)).await.unwrap();
            tx.send(Err(no_content!(""))).await.unwrap();

            let query_rx = Arc::new(AsyncRwLock::new(rx));

            assert!(
                mocked_ext_repo
                    .fetch_and_process_record(1, query_rx.clone())
                    .await
                    .is_none(),
                "First run should be None (stupid implementation)"
            );

            let record = mocked_ext_repo
                .fetch_and_process_record(1, query_rx.clone())
                .await
                .unwrap();

            assert_eq!(record.unwrap().read().await, None);

            assert_eq!(
                mocked_ext_repo
                    .fetch_and_process_record(1, query_rx)
                    .await
                    .unwrap()
                    .err()
                    .unwrap(),
                no_content!("")
            );
        }

        #[rstest]
        #[tokio::test(flavor = "current_thread")]
        async fn test_process_flushed_record(
            record_reader: RecordReader,
            mut mock_ext: MockIoExtension,
            mut processor: Box<MockProcessor>,
            mut commiter: Box<MockCommiter>,
        ) {
            processor.expect_process_record().return_once(|_| {
                Ok(MockStream::boxed(Poll::Ready(Some(Ok(MockRecord::boxed(
                    "key", "val",
                ))))))
            });

            commiter.expect_commit_record().return_once(|_| None);

            commiter
                .expect_flush()
                .return_once(|| {
                    Some(Ok(
                        Box::new(MockRecord::new("key", "val")) as BoxedReadRecord
                    ))
                })
                .times(1);

            mock_ext
                .expect_query()
                .with(eq("bucket"), eq("entry"), predicate::always())
                .return_once(|_, _, _| Ok((processor, commiter)));

            let query = QueryEntry {
                ext: Some(json!({
                    "test1": {},
                })),
                ..Default::default()
            };

            let mocked_ext_repo = mocked_ext_repo("test1", mock_ext);
            mocked_ext_repo
                .register_query(1, "bucket", "entry", query)
                .await
                .unwrap();

            let (tx, rx) = tokio::sync::mpsc::channel(2);
            tx.send(Ok(record_reader)).await.unwrap();
            tx.send(Err(no_content!(""))).await.unwrap();

            let query_rx = Arc::new(AsyncRwLock::new(rx));
            assert!(
                mocked_ext_repo
                    .fetch_and_process_record(1, query_rx.clone())
                    .await
                    .is_none(),
                "First run should be None (stupid implementation)"
            );

            assert!(
                mocked_ext_repo
                    .fetch_and_process_record(1, query_rx.clone())
                    .await
                    .is_none(),
                "we don't commit the record waiting for flush"
            );

            assert!(
                mocked_ext_repo
                    .fetch_and_process_record(1, query_rx.clone())
                    .await
                    .unwrap()
                    .is_ok(),
                "we should get the record from flush"
            );

            drop(tx); // close the channel to simulate no more records
            assert_eq!(
                mocked_ext_repo
                    .fetch_and_process_record(1, query_rx)
                    .await
                    .unwrap()
                    .err()
                    .unwrap(),
                no_content!("No content"),
                "and we are done"
            );
        }
    }

    #[fixture]
    fn mock_ext() -> MockIoExtension {
        MockIoExtension::new()
    }

    #[fixture]
    fn processor() -> Box<MockProcessor> {
        Box::new(MockProcessor::new())
    }

    #[fixture]
    fn commiter() -> Box<MockCommiter> {
        Box::new(MockCommiter::new())
    }

    #[fixture]
    fn record_reader() -> RecordReader {
        let record = Record {
            timestamp: Some(Timestamp {
                seconds: 1,
                nanos: 0,
            }),
            ..Default::default()
        };
        RecordReader::form_record(record, false)
    }

    #[fixture]
    pub fn mocked_record() -> Box<MockRecord> {
        Box::new(MockRecord::new("key1", "val1"))
    }

    fn mocked_ext_repo(name: &str, mock_ext: MockIoExtension) -> ExtRepository {
        let ext_settings = ExtSettings::builder()
            .server_info(ServerInfo::default())
            .build();
        let mut ext_repo =
            ExtRepository::try_load(vec![tempdir().unwrap().keep()], vec![], ext_settings).unwrap();
        ext_repo.extension_map.insert(
            name.to_string(),
            Arc::new(AsyncRwLock::new(Box::new(mock_ext))),
        );
        ext_repo
    }

    mock! {
        IoExtension {}

        #[async_trait]
        impl IoExtension for IoExtension {
            fn info(&self) -> &IoExtensionInfo;


            fn query(
                &mut self,
                bucket_name: &str,
                entry_name: &str,
                query: &QueryEntry,
            ) -> Result<(BoxedProcessor, BoxedCommiter), ReductError>;
        }

    }

    mock! {
        Processor {}

        #[async_trait]
        impl Processor for Processor {
            async fn process_record(
                &mut self,
                record: BoxedReadRecord,
            ) -> Result<BoxedRecordStream, ReductError>;
        }
    }

    mock! {
        Commiter {}

        #[async_trait]
        impl Commiter for Commiter {
            async fn commit_record(&mut self, record: BoxedReadRecord) -> Option<Result<BoxedReadRecord, ReductError>>;
            async fn flush(&mut self) -> Option<Result<BoxedReadRecord, ReductError>>;
        }
    }

    struct MockStream {
        ret_value: Option<Poll<Option<Result<BoxedReadRecord, ReductError>>>>,
    }
    impl Stream for MockStream {
        type Item = Result<BoxedReadRecord, ReductError>;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Option<Self::Item>> {
            let Some(ret_value) = self.ret_value.take() else {
                return Poll::Ready(None);
            };

            ret_value
        }
    }

    impl MockStream {
        fn boxed(ret_value: Poll<Option<Result<BoxedReadRecord, ReductError>>>) -> Box<Self> {
            Box::new(MockStream {
                ret_value: Some(ret_value),
            })
        }
    }

    #[derive(Clone, PartialEq, Debug)]
    pub struct MockRecord {
        meta: RecordMeta,
    }

    impl MockRecord {
        pub fn new(key: &str, val: &str) -> Self {
            let meta = RecordMeta::builder()
                .timestamp(0)
                .computed_labels(Labels::from_iter(
                    vec![(key.to_string(), val.to_string())].into_iter(),
                ))
                .build();
            MockRecord { meta }
        }

        pub fn meta_mut(&mut self) -> &mut RecordMeta {
            &mut self.meta
        }

        pub fn boxed(key: &str, val: &str) -> BoxedReadRecord {
            Box::new(MockRecord::new(key, val))
        }
    }

    #[async_trait]
    impl ReadRecord for MockRecord {
        async fn read(&mut self) -> ReadChunk {
            None
        }

        fn meta(&self) -> &RecordMeta {
            &self.meta
        }
    }
}
