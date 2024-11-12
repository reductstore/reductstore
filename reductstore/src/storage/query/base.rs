// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::block_manager::BlockManager;
use reduct_base::error::ReductError;

use std::collections::HashMap;

use crate::storage::entry::RecordReader;
use reduct_base::msg::entry_api::QueryEntry;
use serde_json::Value;
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// Query is used to iterate over the records among multiple blocks.
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
    fn next(
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
    /// Only metadata
    pub only_metadata: bool,
    /// Condition
    pub when: Option<Value>,
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
    use std::io::Write;
    use tempfile::tempdir;

    #[fixture]
    pub(crate) fn block_manager() -> Arc<RwLock<BlockManager>> {
        // Two blocks
        // the first block has two records: 0, 5
        // the second block has a record: 1000
        let dir = tempdir().unwrap().into_path().join("bucket").join("entry");
        let mut block_manager = BlockManager::new(dir.clone(), BlockIndex::new(dir.join("index")));
        let block_ref = block_manager.start_new_block(0, 10).unwrap();

        {
            let mut block = block_ref.write().unwrap();
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

        block_manager.save_block(block_ref.clone()).unwrap();

        macro_rules! write_record {
            ($block:expr, $index:expr, $content:expr) => {{
                let blk = $block.read().unwrap();
                let (file, _) = block_manager.begin_write_record(&blk, $index).unwrap();
                let rc = file.upgrade().unwrap();
                let mut file = rc.write().unwrap();
                file.write($content).unwrap();
                file.flush().unwrap();
            }};
        }

        write_record!(block_ref, 0, b"0123456789");
        write_record!(block_ref, 5, b"0123456789");

        block_manager.finish_block(block_ref).unwrap();
        let block_ref = block_manager.start_new_block(1000, 10).unwrap();
        {
            let mut block = block_ref.write().unwrap();

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
        block_manager.save_block(block_ref.clone()).unwrap();

        write_record!(block_ref, 1000, b"0123456789");

        block_manager.finish_block(block_ref).unwrap();
        let block_manager = Arc::new(RwLock::new(block_manager));
        block_manager
    }
}
