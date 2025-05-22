// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::asset::asset_manager::ManageStaticAsset;
use crate::ext::filter::ExtWhenFilter;
use crate::storage::query::base::QueryOptions;
use crate::storage::query::condition::{EvaluationStage, Parser};
use crate::storage::query::QueryRx;
use async_trait::async_trait;
use dlopen2::wrapper::{Container, WrapperApi};
use futures_util::StreamExt;
use log::{error, info};
use reduct_base::error::ErrorCode::NoContent;
use reduct_base::error::ReductError;
use reduct_base::ext::{
    BoxedCommiter, BoxedProcessor, BoxedReadRecord, BoxedRecordStream, ExtSettings, IoExtension,
};
use reduct_base::io::ReadRecord;
use reduct_base::msg::entry_api::QueryEntry;
use reduct_base::{internal_server_error, no_content, unprocessable_entity};
use std::collections::HashMap;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
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
    id: u64,
    query: QueryOptions,
    condition_filter: ExtWhenFilter,
    last_access: Instant,
    ext: IoExtRef,
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

impl ExtRepository {
    fn try_load(
        paths: Vec<PathBuf>,
        embedded_extensions: Vec<Box<dyn ManageStaticAsset + Sync + Send>>,
        settings: ExtSettings,
    ) -> Result<ExtRepository, ReductError> {
        let mut extension_map = IoExtMap::new();

        let query_map = AsyncRwLock::new(HashMap::new());
        let mut ext_wrappers = Vec::new();

        for path in paths {
            if !path.exists() {
                return Err(internal_server_error!(
                    "Extension directory {:?} does not exist",
                    path
                ));
            }

            for entry in path.read_dir()? {
                let path = entry?.path();
                if path.is_file()
                    && path
                        .extension()
                        .map_or(false, |ext| ext == "so" || ext == "dll" || ext == "dylib")
                {
                    let ext_wrapper = unsafe {
                        match Container::<ExtensionApi>::load(path.clone()) {
                            Ok(wrapper) => wrapper,
                            Err(e) => {
                                error!("Failed to load extension '{:?}': {:?}", path, e);
                                continue;
                            }
                        }
                    };

                    let ext = unsafe { Box::from_raw(ext_wrapper.get_ext(settings.clone())) };

                    info!("Load extension: {:?}", ext.info());

                    let name = ext.info().name().to_string();
                    extension_map.insert(name, Arc::new(AsyncRwLock::new(ext)));
                    ext_wrappers.push(ext_wrapper);
                }
            }
        }

        Ok(ExtRepository {
            extension_map,
            query_map,
            ext_wrappers,
            embedded_extensions,
        })
    }
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
        let ext = if ext_params.is_some() && ext_params.unwrap().is_object() {
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
                ext.write().await.register_query(
                    query_id,
                    bucket_name,
                    entry_name,
                    &query_request,
                )?;
                Some(Arc::clone(ext))
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
                if let Err(e) = query.ext.write().await.unregister_query(query.id) {
                    error!("Failed to unregister query {}: {:?}", query_id, e);
                }
                ids_to_remove.push(*key);
            }
        }

        for key in ids_to_remove {
            query_map.remove(&key);
        }

        // check if the query has references to computed labels and no extension is found
        let condition = if let Some(condition) = &query_options.when {
            let node = Parser::new().parse(condition)?;
            if ext.is_none() && node.stage() == &EvaluationStage::Compute {
                return Err(unprocessable_entity!(
                    "There is at least one reference to computed labels but no extension is found"
                ));
            }
            Some(node)
        } else {
            None
        };

        let condition_filter = ExtWhenFilter::new(condition, query_options.strict);

        if let Some(ext) = ext {
            let (processor, commiter) = ext.write().await.query(query_id)?;

            query_map.insert(query_id, {
                QueryContext {
                    id: query_id,
                    query: query_options,
                    condition_filter,
                    ext,
                    last_access: Instant::now(),
                    current_stream: None,
                    processor: processor,
                    commiter: commiter,
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
        // if the query is not registered, just return the record
        if self.query_map.read().await.get(&query_id).is_none() {
            let Some(record) = query_rx.write().await.recv().await else {
                return Some(Err(no_content!("No content")));
            };

            return Some(record.map(|r| Box::new(r) as BoxedReadRecord));
        }

        // TODO: The code is awkward, we need to refactor it
        // unfortunatly stream! macro does not work here and crashes compiler
        let mut lock = self.query_map.write().await;
        let query = lock.get_mut(&query_id).unwrap();

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
                    if let Some(last_record) = (query.commiter.flush().await) {
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

pub fn create_ext_repository(
    external_path: Option<PathBuf>,
    embedded_extensions: Vec<Box<dyn ManageStaticAsset + Sync + Send>>,
    settings: ExtSettings,
) -> Result<BoxedManageExtensions, ReductError> {
    if external_path.is_some() || !embedded_extensions.is_empty() {
        let mut paths = if let Some(path) = external_path {
            vec![path]
        } else {
            Vec::new()
        };

        for embedded in &embedded_extensions {
            if let Ok(path) = embedded.absolut_path("") {
                paths.push(path);
            }
        }

        Ok(Box::new(ExtRepository::try_load(
            paths,
            embedded_extensions,
            settings,
        )?))
    } else {
        // Dummy extension repository if
        struct NoExtRepository;

        #[async_trait]
        impl ManageExtensions for NoExtRepository {
            async fn register_query(
                &self,
                _query_id: u64,
                _bucket_name: &str,
                _entry_name: &str,
                _query: QueryEntry,
            ) -> Result<(), ReductError> {
                Ok(())
            }

            async fn fetch_and_process_record(
                &self,
                _query_id: u64,
                query_rx: Arc<AsyncRwLock<QueryRx>>,
            ) -> Option<Result<BoxedReadRecord, ReductError>> {
                query_rx
                    .write()
                    .await
                    .recv()
                    .await
                    .map(|record| record.map(|r| Box::new(r) as BoxedReadRecord))
            }
        }

        Ok(Box::new(NoExtRepository))
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use futures_util::Stream;
    use reduct_base::ext::IoExtensionInfo;
    use reqwest::blocking::get;
    use reqwest::StatusCode;
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
    use std::fs;
    use std::task::{Context, Poll};
    use tempfile::tempdir;
    use test_log::test as log_test;

    mod load {
        use super::*;
        use reduct_base::msg::server_api::ServerInfo;
        #[log_test(rstest)]
        fn test_load_extension(ext_repo: ExtRepository) {
            assert_eq!(ext_repo.extension_map.len(), 1);
            let ext = ext_repo
                .extension_map
                .get("test-ext")
                .unwrap()
                .blocking_read();
            let info = ext.info().clone();
            assert_eq!(
                info,
                IoExtensionInfo::builder()
                    .name("test-ext")
                    .version("0.1.1")
                    .build()
            );
        }

        #[log_test(rstest)]
        fn test_failed_load(ext_settings: ExtSettings) {
            let path = tempdir().unwrap().keep();
            fs::create_dir_all(&path).unwrap();
            fs::write(&path.join("libtest.so"), b"test").unwrap();
            let ext_repo = ExtRepository::try_load(vec![path], vec![], ext_settings).unwrap();
            assert_eq!(ext_repo.extension_map.len(), 0);
        }

        #[log_test(rstest)]
        fn test_failed_open_dir(ext_settings: ExtSettings) {
            let path = PathBuf::from("non_existing_dir");
            let ext_repo = ExtRepository::try_load(vec![path], vec![], ext_settings);
            assert_eq!(
                ext_repo.err().unwrap(),
                internal_server_error!("Extension directory \"non_existing_dir\" does not exist")
            );
        }

        #[fixture]
        fn ext_settings() -> ExtSettings {
            ExtSettings::builder()
                .server_info(ServerInfo::default())
                .build()
        }

        #[fixture]
        fn ext_repo(ext_settings: ExtSettings) -> ExtRepository {
            // This is the path to the build directory of the extension from ext_stub crate
            const EXTENSION_VERSION: &str = "0.1.1";

            if !cfg!(target_arch = "x86_64") {
                panic!("Unsupported architecture");
            }

            let file_name = if cfg!(target_os = "linux") {
                // This is the path to the build directory of the extension from ext_stub crate
                "libtest_ext-x86_64-unknown-linux-gnu.so"
            } else if cfg!(target_os = "macos") {
                "libtest_ext-x86_64-apple-darwin.dylib"
            } else if cfg!(target_os = "windows") {
                "libtest_ext-x86_64-pc-windows-gnu.dll"
            } else {
                panic!("Unsupported platform")
            };

            let ext_path = PathBuf::from(tempdir().unwrap().keep()).join("ext");
            fs::create_dir_all(ext_path.clone()).unwrap();

            let link = format!(
                "https://github.com/reductstore/test-ext/releases/download/v{}/{}",
                EXTENSION_VERSION, file_name
            );

            let mut resp = get(link).expect("Failed to download extension");
            if resp.status() != StatusCode::OK {
                if resp.status() == StatusCode::FOUND {
                    resp = get(resp.headers().get("location").unwrap().to_str().unwrap())
                        .expect("Failed to download extension");
                } else {
                    panic!("Failed to download extension: {}", resp.status());
                }
            }

            fs::write(ext_path.join(file_name), resp.bytes().unwrap())
                .expect("Failed to write extension");

            let empty_ext_path = tempdir().unwrap().keep();
            ExtRepository::try_load(vec![ext_path, empty_ext_path], vec![], ext_settings).unwrap()
        }
    }
    mod register_query {
        use super::*;
        use mockall::predicate::ge;
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
        async fn test_with_ext_part(mut mock_ext: MockIoExtension) {
            let query = QueryEntry {
                ext: Some(json!({
                    "test-ext": {},
                })),
                ..Default::default()
            };

            mock_ext
                .expect_register_query()
                .with(eq(1), eq("bucket"), eq("entry"), eq(query.clone()))
                .return_const(Ok(()));

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
            mock_ext.expect_register_query().never();

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
                .expect_register_query()
                .with(ge(1), eq("bucket"), eq("entry"), eq(query.clone()))
                .return_const(Ok(()));
            mock_ext
                .expect_unregister_query()
                .with(eq(1))
                .return_const(Ok(()));
            mock_ext
                .expect_unregister_query()
                .with(eq(2))
                .return_const(Ok(()));

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
        #[tokio::test]
        async fn error_unknown_extension(mut mock_ext: MockIoExtension) {
            let query = QueryEntry {
                ext: Some(json!({
                    "unknown-ext": {},
                })),
                ..Default::default()
            };

            mock_ext.expect_register_query().never();

            let mocked_ext_repo = mocked_ext_repo("test-ext", mock_ext);
            assert_eq!(
                mocked_ext_repo
                    .register_query(1, "bucket", "entry", query)
                    .await
                    .err()
                    .unwrap(),
                unprocessable_entity!("Unknown extension 'unknown-ext' in query id=1")
            );
        }
    }

    mod next_processed_record {
        use super::*;
        use crate::storage::entry::RecordReader;
        use assert_matches::assert_matches;

        use mockall::predicate;
        use reduct_base::internal_server_error;

        #[rstest]
        #[tokio::test]
        async fn test_empty_query() {
            let mocked_ext_repo = mocked_ext_repo("test-ext", MockIoExtension::new());
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            drop(tx);

            let query_rx = Arc::new(AsyncRwLock::new(rx));
            assert_matches!(
                mocked_ext_repo.fetch_and_process_record(1, query_rx).await,
                ProcessStatus::Stop
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_error_query() {
            let mocked_ext_repo = mocked_ext_repo("test-ext", MockIoExtension::new());
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let err = internal_server_error!("Test error!");
            tx.send(Err(err.clone())).await.unwrap();

            let query_rx = Arc::new(AsyncRwLock::new(rx));
            assert_matches!(
                mocked_ext_repo.fetch_and_process_record(1, query_rx).await,
                ProcessStatus::Ready(Err(_))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_no_registered_query(record_reader: RecordReader) {
            let mocked_ext_repo = mocked_ext_repo("test-ext", MockIoExtension::new());
            let (tx, rx) = tokio::sync::mpsc::channel(1);

            tx.send(Ok(record_reader)).await.unwrap();

            let query_rx = Arc::new(AsyncRwLock::new(rx));
            assert_matches!(
                mocked_ext_repo.fetch_and_process_record(1, query_rx).await,
                ProcessStatus::Ready(Ok(_))
            );
        }

        #[rstest]
        #[tokio::test(flavor = "current_thread")]
        async fn test_process_not_ready(
            record_reader: RecordReader,
            mut mock_ext: MockIoExtension,
        ) {
            mock_ext.expect_register_query().return_const(Ok(()));
            mock_ext
                .expect_process_record()
                .with(eq(1), predicate::always())
                .return_once(|_, _| Ok(MockStream::boxed(Poll::Pending)));

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
            let status = mocked_ext_repo.fetch_and_process_record(1, query_rx).await;
            let NotReady = status else {
                panic!("Expected ProcessStatus::NotReady");
            };
        }
    }

    #[fixture]
    fn mock_ext() -> MockIoExtension {
        MockIoExtension::new()
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

            fn register_query(
                &mut self,
                query_id: u64,
                bucket_name: &str,
                entry_name: &str,
                query: &QueryEntry,
            ) -> Result<(), ReductError>;

            fn unregister_query(&mut self, query_id: u64) -> Result<(), ReductError>;

            async fn process_record(
                &mut self,
                query_id: u64,
                record: BoxedReadRecord,
            ) -> Result<BoxedRecordStream, ReductError>;
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
