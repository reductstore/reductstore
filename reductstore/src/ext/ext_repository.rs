// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::core::weak::Weak;
use crate::storage::entry::RecordReader;
use crate::storage::query::QueryRx;
use crate::storage::storage::CHANNEL_BUFFER_SIZE;
use async_trait::async_trait;
use dlopen2::wrapper::{Container, WrapperApi};
use log::{error, info};
use reduct_base::error::ReductError;
use reduct_base::ext::{BoxedReadRecord, IoExtension, IoExtensionInfo};
use reduct_base::io::{ReadChunk, ReadRecord};
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

pub struct ExtRepository {
    extension_map: IoExtMap,
    mock: MockExt,
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

    fn next_processed_record(
        &self,
        query_id: u64,
        record: BoxedReadRecord,
    ) -> Option<Result<BoxedReadRecord, ReductError>> {
        struct Wrapper {
            record: BoxedReadRecord,
            labels: Labels,
            computed_labels: Labels,
        }

        #[async_trait]
        impl ReadRecord for Wrapper {
            async fn read(&mut self) -> ReadChunk {
                self.record.read().await
            }

            async fn read_timeout(&mut self, timeout: Duration) -> ReadChunk {
                self.record.read_timeout(timeout).await
            }

            fn blocking_read(&mut self) -> ReadChunk {
                self.record.blocking_read()
            }

            fn timestamp(&self) -> u64 {
                self.record.timestamp()
            }

            fn content_length(&self) -> u64 {
                self.record.content_length()
            }

            fn content_type(&self) -> &str {
                self.record.content_type()
            }

            fn last(&self) -> bool {
                self.record.last()
            }

            fn labels(&self) -> &Labels {
                &self.labels
            }

            fn computed_labels(&self) -> &Labels {
                &self.computed_labels
            }

            fn computed_labels_mut(&mut self) -> &mut Labels {
                &mut self.computed_labels
            }
        }

        let mut labels = record.labels().clone();
        let wrapper = Wrapper {
            record,
            labels,
            computed_labels: Labels::from_iter(vec![(
                "from_plugin".to_string(),
                "true".to_string(),
            )]),
        };

        Some(Ok(Box::new(wrapper)))
    }
}

impl ExtRepository {
    pub(crate) fn try_load(path: &PathBuf) -> Result<ExtRepository, ReductError> {
        let mut extension_map = IoExtMap::new();
        let mock = MockExt {};
        if !path.exists() {
            error!("No extension found in path {}", path.display());
            return Ok(ExtRepository {
                extension_map,
                mock,
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
        })
    }

    pub fn register_query(
        &self,
        query_id: u64,
        bucket_name: &str,
        entry_name: &str,
        query_options: QueryEntry,
    ) -> Result<(), ReductError> {
        todo!()
    }

    pub async fn next_processed_record(
        &self,
        query_id: u64,
        query_rx: Arc<AsyncRwLock<QueryRx>>,
    ) -> Option<Result<BoxedReadRecord, ReductError>> {
        if let Some(record) = query_rx.write().await.recv().await {
            match record {
                Ok(record) => self.mock.next_processed_record(query_id, Box::new(record)),
                Err(e) => Some(Err(e)),
            }
        } else {
            None
        }
    }
}
