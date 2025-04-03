// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::base::QueryOptions;
use crate::storage::query::condition::{EvaluationStage, Parser};
use crate::storage::query::filters::WhenFilter;
use crate::storage::query::QueryRx;
use async_trait::async_trait;
use axum::extract::Path;
use dlopen2::wrapper::{Container, WrapperApi};
use log::{error, info};
use reduct_base::error::ReductError;
use reduct_base::ext::{BoxedReadRecord, IoExtension, ProcessStatus};
use reduct_base::msg::entry_api::QueryEntry;
use reduct_base::{internal_server_error, unprocessable_entity, Labels};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Instant;
use tokio::sync::RwLock as AsyncRwLock;

type IoExtRef = Arc<RwLock<Box<dyn IoExtension + Send + Sync>>>;
type IoExtMap = HashMap<String, IoExtRef>;

#[derive(WrapperApi)]
struct ExtensionApi {
    get_ext: extern "C" fn() -> *mut (dyn IoExtension + Send + Sync),
}

#[async_trait]
pub(crate) trait ManageExtensions {
    fn register_query(
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

struct QueryContext {
    query_id: u64,
    query: QueryOptions,
    condition_filter: Option<WhenFilter>,
    last_access: Instant,
    ext_pipeline: Vec<IoExtRef>,
}

struct ExtRepository {
    extension_map: IoExtMap,
    query_map: RwLock<HashMap<u64, QueryContext>>,

    #[allow(dead_code)]
    ext_wrappers: Vec<Container<ExtensionApi>>, // we need to keep the wrappers alive
}

impl ExtRepository {
    pub(crate) fn try_load(path: &PathBuf) -> Result<ExtRepository, ReductError> {
        let mut extension_map = IoExtMap::new();

        let query_map = RwLock::new(HashMap::new());

        if !path.exists() {
            error!("No extension found in path {}", path.display());
            return Ok(ExtRepository {
                extension_map,
                query_map,
                ext_wrappers: Vec::new(),
            });
        }

        let mut ext_wrappers = Vec::new();
        for entry in path.read_dir()? {
            let path = entry?.path();
            if path.is_file()
                && path
                    .extension()
                    .map_or(false, |ext| ext == "so" || ext == "dll")
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

                let ext = unsafe { Arc::new(RwLock::new(Box::from_raw(ext_wrapper.get_ext()))) };

                info!("Load extension: {:?}", ext.read()?.info());

                let name = ext.read()?.info().name().to_string();
                extension_map.insert(name, ext);
                ext_wrappers.push(ext_wrapper);
            }
        }

        Ok(ExtRepository {
            extension_map,
            query_map,
            ext_wrappers,
        })
    }

    fn send_record_to_ext_pipeline(
        query_id: u64,
        mut record: BoxedReadRecord,
        query: &QueryContext,
    ) -> ProcessStatus {
        let mut computed_labels = Labels::new();
        for ext in &query.ext_pipeline {
            computed_labels.extend(record.computed_labels().clone().into_iter());
            let status = ext.read().unwrap().next_processed_record(query_id, record);
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
    fn register_query(
        &self,
        query_id: u64,
        bucket_name: &str,
        entry_name: &str,
        query_request: QueryEntry,
    ) -> Result<(), ReductError> {
        let mut subscribers = Vec::new();
        let mut query_map = self.query_map.write()?;

        for (name, ext) in self.extension_map.iter() {
            if let Some(ext_query) = &query_request.ext {
                if ext_query.get(name).is_none() {
                    continue;
                }

                ext.write()?
                    .register_query(query_id, bucket_name, entry_name, &query_request)?;
                subscribers.push(Arc::clone(ext));
            }
        }

        let query_options: QueryOptions = query_request.into();
        // remove expired queries
        query_map.retain(|_, query| {
            if query.last_access.elapsed() > query.query.ttl {
                false
            } else {
                true
            }
        });

        // check if the query has references to computed labels and no extension is found
        let condition_filter = if let Some(condition) = &query_options.when {
            let node = Parser::new().parse(condition)?;
            if subscribers.is_empty() && node.stage() == &EvaluationStage::Compute {
                return Err(unprocessable_entity!(
                    "There is at least one reference to computed labels but no extension is found"
                ));
            }
            Some(WhenFilter::new(node))
        } else {
            None
        };

        if subscribers.is_empty() {
            // No extension found, we don't need to register the query
            return Ok(());
        }

        query_map.insert(query_id, {
            QueryContext {
                query_id,
                query: query_options,
                condition_filter,
                last_access: Instant::now(),
                ext_pipeline: subscribers,
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
                    // check if query is registered and
                    let mut lock = self.query_map.write().unwrap();

                    let query = lock.get_mut(&query_id);
                    if query.is_none() {
                        return ProcessStatus::Ready(Ok(record));
                    }

                    let query = query.unwrap();
                    query.last_access = Instant::now();

                    let status = match Self::send_record_to_ext_pipeline(query_id, record, &query) {
                        ProcessStatus::Ready(Ok(record)) => ProcessStatus::Ready(Ok(record)),
                        ProcessStatus::Ready(Err(e)) => return ProcessStatus::Ready(Err(e)),
                        ProcessStatus::NotReady => return ProcessStatus::NotReady,
                        ProcessStatus::Stop => return ProcessStatus::Stop,
                    };

                    if query.condition_filter.is_none() {
                        return status;
                    }

                    if let ProcessStatus::Ready(record) = &status {
                        match query
                            .condition_filter
                            .as_mut()
                            .unwrap()
                            .filter_with_computed(&record.as_ref().unwrap())
                        {
                            Ok(true) => status,
                            Ok(false) => ProcessStatus::NotReady,
                            Err(e) => {
                                if query.query.strict {
                                    ProcessStatus::Ready(Err(e))
                                } else {
                                    status
                                }
                            }
                        }
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

pub fn create_ext_repository(path: Option<PathBuf>) -> Result<BoxedManageExtensions, ReductError> {
    if let Some(path) = path {
        Ok(Box::new(ExtRepository::try_load(&path)?))
    } else {
        // Dummy extension repository if
        struct NoExtRepository;

        #[async_trait]
        impl ManageExtensions for NoExtRepository {
            fn register_query(
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
mod tests {
    use super::*;
    use reduct_base::ext::IoExtensionInfo;
    use reqwest::blocking::get;
    use reqwest::StatusCode;
    use rstest::{fixture, rstest};

    use chrono::Duration;
    use mockall::mock;
    use mockall::predicate::{eq, le};
    use serde_json::json;
    use std::thread::sleep;
    use std::{env, fs};
    use tempfile::tempdir;
    use test_log::test as log_test;
    use tokio::join;

    mock! {
        IoExtension {}

        impl IoExtension for IoExtension {
            fn info(&self) -> &IoExtensionInfo;
            fn register_query(
                &self,
                query_id: u64,
                bucket_name: &str,
                entry_name: &str,
                query: &QueryEntry,
            ) -> Result<(), ReductError>;
            fn next_processed_record(
                &self,
                query_id: u64,
                record: BoxedReadRecord,
            ) -> ProcessStatus;
        }

    }

    #[log_test(rstest)]
    fn test_load_extension(ext_repo: ExtRepository) {
        assert_eq!(ext_repo.extension_map.len(), 1);
        let ext = ext_repo
            .extension_map
            .get("test-ext")
            .unwrap()
            .read()
            .unwrap();
        let info = ext.info().clone();
        assert_eq!(
            info,
            IoExtensionInfo::builder()
                .name("test-ext")
                .version("0.1.0")
                .build()
        );
    }
    mod register_query {
        use super::*;
        #[rstest]
        fn test_no_ext_part(mock_ext: MockIoExtension) {
            let mocked_ext_repo = mocked_ext_repo("test-ext", mock_ext);
            assert!(mocked_ext_repo
                .register_query(1, "bucket", "entry", QueryEntry::default())
                .is_ok());

            let query_map = mocked_ext_repo.query_map.read().unwrap();
            assert_eq!(
                query_map.len(),
                0,
                "We don't need to register the query without 'ext' part"
            );
        }

        #[rstest]
        fn test_with_ext_part(mut mock_ext: MockIoExtension) {
            let query = QueryEntry {
                ext: Some(json!({
                    "test-ext": {},
                })),
                ..Default::default()
            };

            mock_ext
                .expect_register_query()
                .with(eq(1), eq("bucket"), eq("entry"), eq(query.clone()))
                .return_once(move |_, _, _, _| Ok(()));

            let mocked_ext_repo = mocked_ext_repo("test-ext", mock_ext);

            assert!(mocked_ext_repo
                .register_query(1, "bucket", "entry", query)
                .is_ok());

            let query_map = mocked_ext_repo.query_map.read().unwrap();
            assert_eq!(
                query_map.len(),
                1,
                "We need to register the query with 'ext' part"
            );
        }

        #[rstest]
        fn test_without_ext_but_computed_labels(mut mock_ext: MockIoExtension) {
            let query = QueryEntry {
                when: Some(json!({"@label": { "$eq": "value" }})),
                ..Default::default()
            };
            mock_ext.expect_register_query().never();

            let mocked_ext_repo = mocked_ext_repo("test", mock_ext);

            assert_eq!(
                mocked_ext_repo
                    .register_query(1, "bucket", "entry", query)
                    .err()
                    .unwrap(),
                unprocessable_entity!(
                    "There is at least one reference to computed labels but no extension is found"
                )
            );
        }

        #[rstest]
        fn test_ttl(mut mock_ext: MockIoExtension) {
            let query = QueryEntry {
                ttl: Some(1),
                ext: Some(json!({
                    "test-ext": {},
                })),
                ..Default::default()
            };

            mock_ext
                .expect_register_query()
                .with(eq(1), eq("bucket"), eq("entry"), eq(query.clone()))
                .returning(move |_, _, _, _| Ok(()));

            let mocked_ext_repo = mocked_ext_repo("test-ext", mock_ext);
            assert!(mocked_ext_repo
                .register_query(1, "bucket", "entry", query.clone())
                .is_ok());

            let query_map = mocked_ext_repo.query_map.read().unwrap();
            assert_eq!(query_map.len(), 1);

            sleep(Duration::seconds(2).to_std().unwrap());
            assert!(mocked_ext_repo
                .register_query(2, "bucket", "entry", query)
                .is_ok());
            let query_map = mocked_ext_repo.query_map.read().unwrap();
            assert_eq!(query_map.len(), 1,);

            assert!(query_map.get(&1).is_none(), "Query 1 should be expired");
            assert!(query_map.get(&2).is_some());
        }
    }

    #[fixture]
    fn ext_repo() -> ExtRepository {
        // This is the path to the build directory of the extension from ext_stub crate
        const EXTENSION_VERSION: &str = "0.1.0";

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

        let ext_path = PathBuf::from(tempdir().unwrap().into_path()).join("ext");
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

        ExtRepository::try_load(&ext_path.to_path_buf()).unwrap()
    }

    #[fixture]
    fn mock_ext() -> MockIoExtension {
        MockIoExtension::new()
    }

    fn mocked_ext_repo(name: &str, mock_ext: MockIoExtension) -> ExtRepository {
        let mut ext_repo = ExtRepository::try_load(&PathBuf::from("ext")).unwrap();
        ext_repo
            .extension_map
            .insert(name.to_string(), Arc::new(RwLock::new(Box::new(mock_ext))));
        ext_repo
    }
}
