// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::core::weak::Weak;
use crate::storage::entry::RecordReader;
use crate::storage::proto::record::Label;
use crate::storage::query::condition::Parser;
use crate::storage::query::filters::{RecordFilter, WhenFilter};
use crate::storage::query::QueryRx;
use crate::storage::storage::CHANNEL_BUFFER_SIZE;
use async_trait::async_trait;
use dlopen2::wrapper::{Container, WrapperApi};
use log::{error, info};
use reduct_base::error::ReductError;
use reduct_base::ext::{BoxedReadRecord, IoExtension, IoExtensionInfo, ProcessStatus};
use reduct_base::io::{ReadChunk, ReadRecord, RecordMeta};
use reduct_base::msg::entry_api::QueryEntry;
use reduct_base::{internal_server_error, not_found, Labels};
use std::collections::hash_map::Values;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::RwLock as AsyncRwLock;

type IoExtRef = Arc<RwLock<Box<dyn IoExtension + Send + Sync>>>;
type IoExtMap = HashMap<String, IoExtRef>;

#[derive(WrapperApi)]
struct PluginApi {
    get_plugin: extern "C" fn() -> *mut (dyn IoExtension + Send + Sync),
}

struct QueryContext {
    query_id: u64,
    bucket_name: String,
    entry_name: String,
    query: QueryEntry,
    condition_filter: Option<WhenFilter>,
}

pub struct ExtRepository {
    extension_map: IoExtMap,
    mock: MockExt,
    query_map: RwLock<HashMap<u64, QueryContext>>,
}

struct MockExt {}

impl IoExtension for MockExt {
    fn info(&self) -> IoExtensionInfo {
        todo!()
    }

    fn register_query(
        &self,
        query_id: u64,
        bucket_name: &str,
        entry_name: &str,
        query: &QueryEntry,
    ) -> Result<(), ReductError> {
        todo!()
    }

    fn next_processed_record(&self, query_id: u64, reader: BoxedReadRecord) -> ProcessStatus {
        struct Wrapper {
            reader: BoxedReadRecord,
            labels: Labels,
            computed_labels: Labels,
        }

        impl RecordMeta for Wrapper {
            fn timestamp(&self) -> u64 {
                self.reader.timestamp()
            }

            fn labels(&self) -> &Labels {
                &self.labels
            }
        }

        #[async_trait]
        impl ReadRecord for Wrapper {
            async fn read(&mut self) -> ReadChunk {
                self.reader.read().await
            }

            async fn read_timeout(&mut self, timeout: Duration) -> ReadChunk {
                self.reader.read_timeout(timeout).await
            }

            fn blocking_read(&mut self) -> ReadChunk {
                self.reader.blocking_read()
            }

            fn last(&self) -> bool {
                self.reader.last()
            }
            fn computed_labels(&self) -> &Labels {
                &self.computed_labels
            }

            fn computed_labels_mut(&mut self) -> &mut Labels {
                &mut self.computed_labels
            }

            fn content_length(&self) -> u64 {
                self.reader.content_length()
            }

            fn content_type(&self) -> &str {
                self.reader.content_type()
            }
        }

        let mut labels = reader.labels().clone();
        let wrapper = Wrapper {
            reader,
            labels,
            computed_labels: Labels::from_iter(vec![(
                "from_plugin".to_string(),
                "true".to_string(),
            )]),
        };

        ProcessStatus::Ready(Ok(Box::new(wrapper)))
    }
}

impl ExtRepository {
    pub(crate) fn try_load(path: &PathBuf) -> Result<ExtRepository, ReductError> {
        let mut extension_map = IoExtMap::new();
        let query_map = RwLock::new(HashMap::new());
        let mock = MockExt {};
        if !path.exists() {
            error!("No extension found in path {}", path.display());
            return Ok(ExtRepository {
                extension_map,
                mock,
                query_map,
            });
        }

        for entry in path.read_dir()? {
            let path = entry?.path();
            if path.is_file()
                && path
                    .extension()
                    .map_or(false, |ext| ext == "so" || ext == "dll")
            {
                let plugin_api_wrapper = unsafe {
                    Container::<PluginApi>::load(path)
                        .map_err(|e| internal_server_error!("Failed to load plugin: {}", e))?
                };
                let plugin = unsafe { Box::from_raw(plugin_api_wrapper.get_plugin()) };
                info!("Load extension: {:?}", plugin.info());

                let name = plugin.info().name().to_string();
                extension_map.insert(name, Arc::new(RwLock::new(plugin)));
            }
        }

        Ok(ExtRepository {
            extension_map,
            mock,
            query_map,
        })
    }

    pub fn register_query(
        &self,
        query_id: u64,
        bucket_name: &str,
        entry_name: &str,
        query_options: QueryEntry,
    ) -> Result<(), ReductError> {
        let condition_filter = if let Some(condition) = &query_options.when {
            Some(WhenFilter::new(Parser::new().parse(condition)?))
        } else {
            None
        };

        self.query_map.write()?.insert(query_id, {
            QueryContext {
                query_id,
                bucket_name: bucket_name.to_string(),
                entry_name: entry_name.to_string(),
                query: query_options,
                condition_filter,
            }
        });
        Ok(())
    }

    /// TODO: Call in the code that removes queries
    pub fn remove_query(&self, query_id: u64) -> Result<(), ReductError> {
        self.query_map.write()?.remove(&query_id);
        Ok(())
    }

    pub async fn next_processed_record(
        &self,
        query_id: u64,
        query_rx: Arc<AsyncRwLock<QueryRx>>,
    ) -> ProcessStatus {
        if let Some(record) = query_rx.write().await.recv().await {
            match record {
                Ok(record) => {
                    let status = self.mock.next_processed_record(query_id, Box::new(record));
                    let mut lock = self.query_map.write().unwrap();
                    let filter = &mut lock.get_mut(&query_id).unwrap().condition_filter;
                    if filter.is_none() {
                        return status;
                    }

                    if let ProcessStatus::Ready(record) = &status {
                        match filter
                            .as_mut()
                            .unwrap()
                            .filter_with_computed(&record.as_ref().unwrap())
                        {
                            Ok(true) => status,
                            Ok(false) => ProcessStatus::NotReady,
                            Err(e) => ProcessStatus::Ready(Err(e)),
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
