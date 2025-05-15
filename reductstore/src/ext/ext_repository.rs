// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::asset::asset_manager::ManageStaticAsset;
use crate::ext::filter::ExtWhenFilter;
use crate::storage::query::base::QueryOptions;
use crate::storage::query::condition::{EvaluationStage, Parser};
use crate::storage::query::QueryRx;
use async_trait::async_trait;
use dlopen2::wrapper::{Container, WrapperApi};
use log::{error, info};
use reduct_base::error::ReductError;
use reduct_base::ext::{BoxedReadRecord, ExtSettings, IoExtension, ProcessStatus};
use reduct_base::msg::entry_api::QueryEntry;
use reduct_base::{internal_server_error, unprocessable_entity, Labels};
use std::collections::HashMap;
use std::path::PathBuf;
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
    async fn next_processed_record(
        &self,
        query_id: u64,
        query_rx: Arc<AsyncRwLock<QueryRx>>,
    ) -> ProcessStatus;
}

pub type BoxedManageExtensions = Box<dyn ManageExtensions + Sync + Send>;

pub(crate) struct QueryContext {
    id: u64,
    query: QueryOptions,
    condition_filter: ExtWhenFilter,
    last_access: Instant,
    ext_pipeline: Vec<IoExtRef>,
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

    async fn send_record_to_ext_pipeline(
        query_id: u64,
        mut record: BoxedReadRecord,
        ext_pipeline: Vec<IoExtRef>,
    ) -> ProcessStatus {
        let mut computed_labels = Labels::new();
        for ext in ext_pipeline {
            computed_labels.extend(record.computed_labels().clone().into_iter());
            let status = ext
                .write()
                .await
                .next_processed_record(query_id, record)
                .await;

            if let ProcessStatus::Ready(result) = status {
                if let Ok(processed_record) = result {
                    record = processed_record;
                } else {
                    return ProcessStatus::Ready(result);
                }
            } else {
                return status;
            }
        }

        record
            .computed_labels_mut()
            .extend(computed_labels.clone().into_iter());
        ProcessStatus::Ready(Ok(record))
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
        let mut pipeline = Vec::new();
        let mut query_map = self.query_map.write().await;

        let ext_params = query_request.ext.as_ref();
        if ext_params.is_some() && ext_params.unwrap().is_object() {
            for (name, _) in ext_params.unwrap().as_object().unwrap().iter() {
                if let Some(ext) = self.extension_map.get(name) {
                    ext.write().await.register_query(
                        query_id,
                        bucket_name,
                        entry_name,
                        &query_request,
                    )?;
                    pipeline.push(Arc::clone(ext));
                } else {
                    return Err(unprocessable_entity!(
                        "Unknown extension '{}' in query id={}",
                        name,
                        query_id
                    ));
                }
            }
        }

        let query_options: QueryOptions = query_request.into();
        // remove expired queries
        let mut ids_to_remove = Vec::new();

        for (key, query) in query_map.iter() {
            if query.last_access.elapsed() > query.query.ttl {
                for ext in &query.ext_pipeline {
                    if let Err(e) = ext.write().await.unregister_query(query.id) {
                        error!("Failed to unregister query {}: {:?}", query_id, e);
                    }
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
            if pipeline.is_empty() && node.stage() == &EvaluationStage::Compute {
                return Err(unprocessable_entity!(
                    "There is at least one reference to computed labels but no extension is found"
                ));
            }
            Some(node)
        } else {
            None
        };

        if pipeline.is_empty() {
            // No extension found, we don't need to register the query
            return Ok(());
        }

        query_map.insert(query_id, {
            QueryContext {
                id: query_id,
                query: query_options,
                condition_filter: ExtWhenFilter::new(condition),
                last_access: Instant::now(),
                ext_pipeline: pipeline,
            }
        });

        Ok(())
    }

    async fn next_processed_record(
        &self,
        query_id: u64,
        query_rx: Arc<AsyncRwLock<QueryRx>>,
    ) -> ProcessStatus {
        // check if registered
        if let Some(record) = query_rx.write().await.recv().await {
            match record {
                Ok(record) => {
                    let record = Box::new(record);
                    let pipline = {
                        // check if query is registered and
                        let mut lock = self.query_map.write().await;

                        let query = lock.get_mut(&query_id);
                        if query.is_none() {
                            return ProcessStatus::Ready(Ok(record));
                        }

                        let query = query.unwrap();
                        query.last_access = Instant::now();
                        query.ext_pipeline.clone()
                    };

                    let status = Self::send_record_to_ext_pipeline(query_id, record, pipline).await;
                    if let ProcessStatus::Ready(Ok(record)) = status {
                        let mut lock = self.query_map.write().await;
                        let query = lock.get_mut(&query_id).unwrap();
                        query
                            .condition_filter
                            .filter_record(ProcessStatus::Ready(Ok(record)), query.query.strict)
                    } else {
                        status
                    }
                }
                Err(e) => ProcessStatus::Ready(Err(e)),
            }
        } else {
            ProcessStatus::Stop
        }
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

            async fn next_processed_record(
                &self,
                _query_id: u64,
                query_rx: Arc<AsyncRwLock<QueryRx>>,
            ) -> ProcessStatus {
                let record = query_rx.write().await.recv().await;
                match record {
                    Some(Ok(record)) => ProcessStatus::Ready(Ok(Box::new(record))),
                    Some(Err(e)) => ProcessStatus::Ready(Err(e)),
                    None => ProcessStatus::Stop,
                }
            }
        }

        Ok(Box::new(NoExtRepository))
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
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
    use serde_json::json;
    use std::fs;
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

    mod send_record_to_ext_pipeline {
        use super::*;
        use assert_matches::assert_matches;
        use mockall::predicate::always;
        use reduct_base::internal_server_error;

        #[rstest]
        #[tokio::test]
        async fn test_no_ext() {
            let record = Box::new(MockRecord::new("key1", "val1"));
            let query = QueryContext {
                id: 1,
                query: QueryOptions::default(),
                condition_filter: ExtWhenFilter::new(None),
                last_access: Instant::now(),
                ext_pipeline: vec![],
            };

            let status = ExtRepository::send_record_to_ext_pipeline(
                query.id,
                record,
                query.ext_pipeline.clone(),
            )
            .await;
            assert_matches!(status, ProcessStatus::Ready(_));
        }

        #[rstest]
        #[tokio::test(flavor = "current_thread")]
        async fn test_stop_pipeline_at_errors() {
            let record = Box::new(MockRecord::new("key1", "val1"));
            let mut mock_ext_1 = MockIoExtension::new();
            let mut mock_ext_2 = MockIoExtension::new();

            mock_ext_1
                .expect_next_processed_record()
                .with(eq(1), always())
                .return_once(|_, _| ProcessStatus::Ready(Err(internal_server_error!("test"))));
            mock_ext_2.expect_next_processed_record().never();

            let query = QueryContext {
                id: 1,
                query: QueryOptions::default(),
                condition_filter: ExtWhenFilter::new(None),
                last_access: Instant::now(),
                ext_pipeline: vec![
                    Arc::new(AsyncRwLock::new(Box::new(mock_ext_1))),
                    Arc::new(AsyncRwLock::new(Box::new(mock_ext_2))),
                ],
            };

            let status = ExtRepository::send_record_to_ext_pipeline(
                query.id,
                record,
                query.ext_pipeline.clone(),
            )
            .await;
            let ProcessStatus::Ready(result) = status else {
                panic!("Expected ProcessStatus::Ready");
            };

            assert_eq!(result.err().unwrap(), internal_server_error!("test"));
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
                mocked_ext_repo.next_processed_record(1, query_rx).await,
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
                mocked_ext_repo.next_processed_record(1, query_rx).await,
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
                mocked_ext_repo.next_processed_record(1, query_rx).await,
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
                .expect_next_processed_record()
                .with(eq(1), predicate::always())
                .return_once(|_, _| ProcessStatus::NotReady);

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
            let status = mocked_ext_repo.next_processed_record(1, query_rx).await;
            let ProcessStatus::NotReady = status else {
                panic!("Expected ProcessStatus::NotReady");
            };
        }

        #[rstest]
        #[tokio::test(flavor = "current_thread")]
        async fn test_process_in_pipeline(record_reader: RecordReader) {
            let record1 = Box::new(MockRecord::new("key1", "val1"));
            let record2 = Box::new(MockRecord::new("key2", "val2"));

            let mut mock1 = MockIoExtension::new();
            let mut mock2 = MockIoExtension::new();

            mock1.expect_register_query().return_const(Ok(()));
            mock2.expect_register_query().return_const(Ok(()));
            mock1
                .expect_next_processed_record()
                .with(eq(1), predicate::always())
                .return_once(|_, _| ProcessStatus::Ready(Ok(record1)));
            mock2
                .expect_next_processed_record()
                .with(eq(1), predicate::always())
                .return_once(|_, _| ProcessStatus::Ready(Ok(record2)));

            let mut mocked_ext_repo = mocked_ext_repo("test1", mock1);
            mocked_ext_repo.extension_map.insert(
                "test2".to_string(),
                Arc::new(AsyncRwLock::new(Box::new(mock2))),
            );

            let query = QueryEntry {
                ext: Some(json!({
                    "test1": {},
                    "test2": {}
                })),
                when: Some(json!({"@key2": { "$eq": "val2" }})),
                ..Default::default()
            };
            mocked_ext_repo
                .register_query(1, "bucket", "entry", query)
                .await
                .unwrap();
            let (tx, rx) = tokio::sync::mpsc::channel(1);

            tx.send(Ok(record_reader)).await.unwrap();

            let query_rx = Arc::new(AsyncRwLock::new(rx));
            let status = mocked_ext_repo.next_processed_record(1, query_rx).await;

            let ProcessStatus::Ready(record) = status else {
                panic!("Expected ProcessStatus::Ready");
            };

            assert_eq!(
                record.unwrap().computed_labels(),
                &Labels::from_iter(
                    vec![
                        ("key1".to_string(), "val1".to_string()),
                        ("key2".to_string(), "val2".to_string())
                    ]
                    .into_iter()
                ),
                "Computed labels to be merged from both extensions"
            );
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

            async fn next_processed_record(
                &mut self,
                query_id: u64,
                record: BoxedReadRecord,
            ) -> ProcessStatus;
        }

    }

    #[derive(Clone, PartialEq, Debug)]
    pub struct MockRecord {
        computed_labels: Labels,
        labels: Labels,
    }

    impl MockRecord {
        pub fn new(key: &str, val: &str) -> Self {
            MockRecord {
                computed_labels: Labels::from_iter(
                    vec![(key.to_string(), val.to_string())].into_iter(),
                ),
                labels: Labels::new(),
            }
        }

        pub fn labels_mut(&mut self) -> &mut Labels {
            &mut self.labels
        }
    }

    impl RecordMeta for MockRecord {
        fn timestamp(&self) -> u64 {
            0
        }

        fn labels(&self) -> &Labels {
            &self.labels
        }
    }

    #[async_trait]
    impl ReadRecord for MockRecord {
        async fn read(&mut self) -> ReadChunk {
            None
        }

        fn last(&self) -> bool {
            todo!()
        }

        fn computed_labels(&self) -> &Labels {
            &self.computed_labels
        }

        fn computed_labels_mut(&mut self) -> &mut Labels {
            &mut self.computed_labels
        }

        fn content_length(&self) -> u64 {
            todo!()
        }

        fn content_type(&self) -> &str {
            todo!()
        }
    }
}
