// Copyright 2023-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::cfg::io::IoConfig;
use crate::core::sync::AsyncRwLock;
use crate::storage::block_manager::BlockManager;
use crate::storage::entry::RecordReader;
use async_trait::async_trait;
use reduct_base::error::ReductError;
use reduct_base::msg::entry_api::QueryEntry;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Query is used to iterate over the records among multiple blocks.
#[async_trait]
pub(in crate::storage) trait Query {
    ///  Get next record
    ///
    /// # Arguments
    ///
    /// * `block_indexes` - The indexes of the blocks to read.
    /// * `block_manager` - The block manager that manages the blocks.
    ///
    /// # Returns
    ///
    /// * `RecordReader` - The record reader.
    ///
    /// # Errors
    ///
    /// * `HTTPError` - If the record cannot be read.
    /// * `HTTPError(NoContent)` - If all records have been read.
    async fn next(
        &mut self,
        block_manager: Arc<AsyncRwLock<BlockManager>>,
    ) -> Result<RecordReader, ReductError>;

    /// Get the IO settings for the query.
    ///
    /// # Arguments
    ///
    /// * `defaults` - The default IO settings if not specified in the query directives.
    ///
    /// # Returns
    ///
    /// * `IoConfig` - The IO settings for the query.
    fn io_settings(&self) -> &IoConfig;
}

/// QueryOptions is used to specify the options for a query.
#[derive(Clone, Debug)]
pub(crate) struct QueryOptions {
    /// The time to live of the query.
    pub ttl: Duration,
    /// Only include the records that match the key-value pairs.
    pub include: HashMap<String, String>,
    /// Exclude the records that match the key-value pairs.
    pub exclude: HashMap<String, String>,
    /// If true, the query will never be done
    pub continuous: bool,
    /// The maximum number of records to return only for non-continuous queries.
    pub limit: Option<u64>,
    /// Return each N records
    pub each_n: Option<u64>,
    /// Return a record every S seconds
    pub each_s: Option<f64>,
    /// Only metadata
    pub only_metadata: bool,
    /// Condition
    pub when: Option<Value>,
    /// Strict condition
    pub strict: bool,
    /// Extension part
    #[allow(dead_code)] // used in extension
    pub ext: Option<Value>,
    // Io Config
}

impl From<QueryEntry> for QueryOptions {
    fn from(query: QueryEntry) -> QueryOptions {
        QueryOptions {
            ttl: Duration::from_secs(query.ttl.unwrap_or(Self::default().ttl.as_secs())),
            include: query.include.unwrap_or_default(),
            exclude: query.exclude.unwrap_or_default(),
            continuous: query.continuous.unwrap_or(false),
            limit: query.limit,
            each_n: query.each_n,
            each_s: query.each_s,
            only_metadata: query.only_metadata.unwrap_or(false),
            when: query.when,
            strict: query.strict.unwrap_or(false),
            ext: query.ext,
        }
    }
}

impl Default for QueryOptions {
    fn default() -> QueryOptions {
        QueryOptions {
            ttl: Duration::from_secs(60),
            include: HashMap::new(),
            exclude: HashMap::new(),
            continuous: false,
            limit: None,
            each_n: None,
            each_s: None,
            only_metadata: false,
            when: None,
            strict: false,
            ext: None,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::backend::Backend;
    use crate::cfg::Cfg;
    use crate::core::file_cache::FILE_CACHE;
    use crate::storage::block_manager::block_index::BlockIndex;
    use crate::storage::proto::record::{Label, State as RecordState};
    use crate::storage::proto::Record;
    use prost_wkt_types::Timestamp;
    use rstest::fixture;
    use std::io::{SeekFrom, Write};
    use tempfile::tempdir;

    #[fixture]
    pub(crate) fn block_manager() -> Arc<AsyncRwLock<BlockManager>> {
        // Two blocks
        // the first block has two records: 0, 5
        // the second block has a record: 1000
        let dir = tempdir().unwrap().keep().join("bucket").join("entry");
        let rt = tokio::runtime::Handle::current();
        rt.block_on(async {
            FILE_CACHE
                .set_storage_backend(
                    rt.block_on(Backend::builder().local_data_path(dir.clone()).try_build())
                        .unwrap(),
                )
                .await;
            let mut block_manager = BlockManager::build(
                dir.clone(),
                BlockIndex::new(dir.join("index")),
                Cfg::default().into(),
            )
            .await;
            let block_ref = block_manager.start_new_block(0, 10).await.unwrap();

            {
                let mut block = block_ref.write().await.unwrap();
                block.insert_or_update_record(Record {
                    timestamp: Some(Timestamp {
                        seconds: 0,
                        nanos: 0,
                    }),
                    begin: 0,
                    end: 10,
                    state: RecordState::Finished as i32,
                    labels: vec![
                        Label {
                            name: "block".to_string(),
                            value: "1".to_string(),
                        },
                        Label {
                            name: "record".to_string(),
                            value: "1".to_string(),
                        },
                        Label {
                            name: "flag".to_string(),
                            value: "true".to_string(),
                        },
                    ],
                    content_type: "".to_string(),
                });

                block.insert_or_update_record(Record {
                    timestamp: Some(Timestamp {
                        seconds: 0,
                        nanos: 5000,
                    }),
                    begin: 10,
                    end: 20,
                    state: RecordState::Finished as i32,
                    labels: vec![
                        Label {
                            name: "block".to_string(),
                            value: "1".to_string(),
                        },
                        Label {
                            name: "record".to_string(),
                            value: "2".to_string(),
                        },
                        Label {
                            name: "flag".to_string(),
                            value: "false".to_string(),
                        },
                    ],
                    content_type: "".to_string(),
                });
            }

            block_manager.save_block(block_ref.clone()).await.unwrap();

            macro_rules! write_record {
                ($block:expr, $index:expr, $content:expr) => {{
                    let blk = $block.read().await.unwrap();
                    let (path, offset) = block_manager.begin_write_record(&blk, $index).unwrap();
                    let mut file = FILE_CACHE
                        .write_or_create(&path, SeekFrom::Start(offset))
                        .await
                        .unwrap();
                    file.write($content).unwrap();
                    file.flush().unwrap();
                }};
            }

            write_record!(block_ref, 0, b"0123456789");
            write_record!(block_ref, 5, b"0123456789");

            rt.block_on(block_manager.finish_block(block_ref)).unwrap();
            let block_ref = rt
                .block_on(block_manager.start_new_block(1000, 10))
                .unwrap();
            {
                let mut block = rt.block_on(block_ref.write()).unwrap();

                block.insert_or_update_record(Record {
                    timestamp: Some(Timestamp {
                        seconds: 0,
                        nanos: 1000_000,
                    }),
                    begin: 0,
                    end: 10,
                    state: RecordState::Finished as i32,
                    labels: vec![
                        Label {
                            name: "block".to_string(),
                            value: "2".to_string(),
                        },
                        Label {
                            name: "record".to_string(),
                            value: "1".to_string(),
                        },
                    ],
                    content_type: "".to_string(),
                });
            }
            rt.block_on(block_manager.save_block(block_ref.clone()))
                .unwrap();

            write_record!(block_ref, 1000, b"0123456789");

            rt.block_on(block_manager.finish_block(block_ref)).unwrap();
            Arc::new(AsyncRwLock::new(block_manager))
        })
    }
}
