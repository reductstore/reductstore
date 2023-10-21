// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::storage::block_manager::BlockManager;
use crate::storage::reader::RecordReader;
use reduct_base::error::ReductError;

use std::collections::{BTreeSet, HashMap};

use std::sync::{Arc, RwLock};
use std::time::Duration;

#[derive(PartialEq, Debug)]
pub enum QueryState {
    /// The query is running.
    Running(usize),
    /// The query is done.
    Done,
    /// The query is outdated.
    Expired,
}

/// Query is used to iterate over the records among multiple blocks.
pub trait Query {
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
    fn next(
        &mut self,
        block_indexes: &BTreeSet<u64>,
        block_manager: &mut BlockManager,
    ) -> Result<(Arc<RwLock<RecordReader>>, bool), ReductError>;

    /// Get the state of the query.
    fn state(&self) -> &QueryState;
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
    pub limit: Option<usize>,
}

impl Default for QueryOptions {
    fn default() -> QueryOptions {
        QueryOptions {
            ttl: Duration::from_secs(60),
            include: HashMap::new(),
            exclude: HashMap::new(),
            continuous: false,
            limit: None,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::storage::block_manager::ManageBlock;
    use crate::storage::proto::record::{Label, State as RecordState};
    use crate::storage::proto::Record;
    use bytes::Bytes;
    use prost_wkt_types::Timestamp;
    use rstest::fixture;
    use tempfile::tempdir;
    use tokio::io::AsyncWriteExt;

    #[fixture]
    pub(crate) async fn block_manager_and_index() -> (BlockManager, BTreeSet<u64>) {
        // Two blocks
        // the first block has two records: 0, 5
        // the second block has a record: 1000
        let dir = tempdir().unwrap().into_path();
        let mut block_manager = BlockManager::new(dir);
        let mut block = block_manager.start(0, 10).unwrap();

        block.records.push(Record {
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

        block.records.push(Record {
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

        block.latest_record_time = Some(Timestamp {
            seconds: 0,
            nanos: 5000,
        });
        block.size = 20;
        block_manager.save(block.clone()).unwrap();

        macro_rules! write_record {
            ($block:expr, $index:expr, $content:expr) => {{
                let mut file = block_manager.begin_write(&$block, $index).await.unwrap();
                file.write($content).await.unwrap();
                file.flush().await.unwrap();
            }};
        }

        write_record!(block, 0, b"0123456789");
        write_record!(block, 1, b"0123456789");

        block_manager.finish(&block).unwrap();
        let mut block = block_manager.start(1000, 10).unwrap();

        block.records.push(Record {
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

        block.latest_record_time = Some(Timestamp {
            seconds: 0,
            nanos: 1000_000,
        });
        block.size = 10;
        block_manager.save(block.clone()).unwrap();

        write_record!(block, 0, b"0123456789");

        block_manager.finish(&block).unwrap();
        (block_manager, BTreeSet::from([0, 1000]))
    }
}
