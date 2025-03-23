// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::base::QueryOptions;
use crate::storage::query::condition::{EvaluationStage, Parser};
use crate::storage::query::filters::WhenFilter;
use crate::storage::query::QueryRx;
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

struct QueryContext {
    query_id: u64,
    query: QueryOptions,
    condition_filter: Option<WhenFilter>,
    last_access: Instant,
    ext_pipeline: Vec<IoExtRef>,
}

pub struct ExtRepository {
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

    pub fn register_query(
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

    pub(crate) async fn next_processed_record(
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

#[cfg(test)]
mod tests {
    use super::*;
    use reduct_base::ext::IoExtensionInfo;
    use rstest::{fixture, rstest};
    use std::env;
    use test_log::test as log_test;

    #[log_test(rstest)]
    fn test_load_extension(ext_path: PathBuf) {
        let ext_repo = ExtRepository::try_load(&ext_path).unwrap();
        assert_eq!(ext_repo.extension_map.len(), 1);
        let ext = ext_repo
            .extension_map
            .get("ext_stub")
            .unwrap()
            .read()
            .unwrap();
        let info = ext.info().clone();
        assert_eq!(
            info,
            IoExtensionInfo::builder()
                .name("ext_stub")
                .version("0.1")
                .build()
        );
    }

    #[fixture]
    fn ext_path() -> PathBuf {
        // This is the path to the build directory of the extension from ext_stub crate
        PathBuf::from(env::var("OUT_DIR").unwrap())
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf()
    }
}
