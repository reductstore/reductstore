// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::core::weak::Weak;
use crate::storage::entry::RecordReader;
use crate::storage::proto::record::Label;
use crate::storage::query::base::QueryOptions;
use crate::storage::query::condition::{EvaluationStage, Parser};
use crate::storage::query::filters::{RecordFilter, WhenFilter};
use crate::storage::query::QueryRx;
use crate::storage::storage::CHANNEL_BUFFER_SIZE;
use async_trait::async_trait;
use dlopen2::wrapper::{Container, WrapperApi};
use log::{debug, error, info};
use reduct_base::error::ReductError;
use reduct_base::ext::{BoxedReadRecord, IoExtension, IoExtensionInfo, ProcessStatus};
use reduct_base::io::{ReadChunk, ReadRecord, RecordMeta};
use reduct_base::msg::entry_api::QueryEntry;
use reduct_base::msg::entry_api::QueryType::Query;
use reduct_base::{internal_server_error, not_found, unprocessable_entity, Labels};
use std::collections::hash_map::Values;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::RwLock as AsyncRwLock;

type IoExtRef = Arc<RwLock<Box<dyn IoExtension + Send + Sync>>>;
type IoExtMap = HashMap<String, IoExtRef>;

#[derive(WrapperApi)]
struct ExtensionApi {
    get_ext: extern "C" fn() -> *mut (dyn IoExtension + Send + Sync),
}

struct QueryContext {
    query_id: u64,
    bucket_name: String,
    entry_name: String,
    query: QueryOptions,
    condition_filter: Option<WhenFilter>,
    last_access: Instant,
    ext_pipeline: Vec<IoExtRef>,
}

pub struct ExtRepository {
    extension_map: IoExtMap,
    query_map: RwLock<HashMap<u64, QueryContext>>,
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
                    Container::<ExtensionApi>::load(path)
                        .map_err(|e| internal_server_error!("Failed to load extension: {}", e))?
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
                bucket_name: bucket_name.to_string(),
                entry_name: entry_name.to_string(),
                query: query_options,
                condition_filter,
                last_access: Instant::now(),
                ext_pipeline: subscribers,
            }
        });

        Ok(())
    }

    pub async fn next_processed_record(
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

                    let mut query = lock.get_mut(&query_id);
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
                                if (query.query.strict) {
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
        let mut status = ProcessStatus::Stop;
        let mut computed_labels = Labels::new();
        for ext in &query.ext_pipeline {
            computed_labels.extend(record.computed_labels().clone().into_iter());
            status = ext.read().unwrap().next_processed_record(query_id, record);
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
