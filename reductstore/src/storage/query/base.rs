// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::block_manager::BlockManager;
use reduct_base::error::ReductError;

use std::collections::HashMap;

use crate::storage::bucket::RecordReader;

use axum::async_trait;

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

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
    /// * `bool` - True if it is the last record (should be remove in the future, doesn't work with include/exclude).
    ///
    /// # Errors
    ///
    /// * `HTTPError` - If the record cannot be read.
    /// * `HTTPError(NoContent)` - If all records have been read.
    async fn next(
        &mut self,
        block_manager: Arc<RwLock<BlockManager>>,
    ) -> Result<RecordReader, ReductError>;
}

/// QueryOptions is used to specify the options for a query.
#[derive(Clone, Debug)]
pub struct QueryOptions {
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
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::storage::proto::record::{Label, State as RecordState};
    use crate::storage::proto::Record;

    use crate::storage::block_manager::block_index::BlockIndex;
    use prost_wkt_types::Timestamp;
    use rstest::fixture;
    use tempfile::tempdir;
    use tokio::io::AsyncWriteExt;

    #[fixture]
    pub(crate) async fn block_manager() -> Arc<RwLock<BlockManager>> {
        // Two blocks
        // the first block has two records: 0, 5
        // the second block has a record: 1000
        let dir = tempdir().unwrap().into_path();
        let mut block_manager = BlockManager::new(dir.clone(), BlockIndex::new(dir.join("index")));
        let block_ref = block_manager.start_new_block(0, 10).await.unwrap();

        {
            let mut block = block_ref.write().await;
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
                ],
                content_type: "".to_string(),
            });
        }

        block_manager.save_block(block_ref.clone()).await.unwrap();

        macro_rules! write_record {
            ($block:expr, $index:expr, $content:expr) => {{
                let (file, _) = block_manager
                    .begin_write_record($block.clone(), $index)
                    .await
                    .unwrap();
                let mut file = file.write().await;
                file.write($content).await.unwrap();
                file.flush().await.unwrap();
            }};
        }

        write_record!(block_ref, 0, b"0123456789");
        write_record!(block_ref, 5, b"0123456789");

        block_manager.finish_block(block_ref).await.unwrap();
        let block_ref = block_manager.start_new_block(1000, 10).await.unwrap();
        {
            let mut block = block_ref.write().await;

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
        block_manager.save_block(block_ref.clone()).await.unwrap();

        write_record!(block_ref, 1000, b"0123456789");

        block_manager.finish_block(block_ref).await.unwrap();
        let block_manager = Arc::new(RwLock::new(block_manager));
        block_manager
    }
}
