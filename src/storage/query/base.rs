// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::core::status::HTTPError;
use crate::storage::block_manager::BlockManager;
use crate::storage::reader::RecordReader;
use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::rc::Rc;
use std::time::Duration;

#[derive(PartialEq, Debug)]
pub enum QueryState {
    /// The query is running.
    Running,
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
    ) -> Result<(Rc<RefCell<RecordReader>>, bool), HTTPError>;

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
}

impl QueryOptions {
    pub fn default() -> QueryOptions {
        QueryOptions {
            ttl: Duration::from_secs(60),
            include: HashMap::new(),
            exclude: HashMap::new(),
            continuous: false,
        }
    }
}
