// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::storage::block_manager::BlockManager;
use crate::storage::query::base::QueryState::Running;
use crate::storage::query::base::{Query, QueryOptions, QueryState};
use crate::storage::query::historical::HistoricalQuery;
use crate::storage::reader::RecordReader;
use reduct_base::error::{ErrorCode, HttpError};
use std::collections::BTreeSet;
use std::sync::{Arc, RwLock};

/// A query that is limited to a certain number of records.
pub(crate) struct LimitedQuery {
    query: HistoricalQuery,
    options: QueryOptions,
}

impl LimitedQuery {
    pub fn new(start: u64, stop: u64, options: QueryOptions) -> LimitedQuery {
        LimitedQuery {
            query: HistoricalQuery::new(start, stop, options.clone()),
            options,
        }
    }
}

impl Query for LimitedQuery {
    fn next(
        &mut self,
        block_indexes: &BTreeSet<u64>,
        block_manager: &mut BlockManager,
    ) -> Result<(Arc<RwLock<RecordReader>>, bool), HttpError> {
        // TODO: It could be done better, maybe it make sense to move the limit into HistoricalQuery instead of manipulating the state here.
        if let QueryState::Done = self.state() {
            return Err(HttpError {
                status: ErrorCode::NoContent,
                message: "No content".to_string(),
            });
        }

        let (reader, last) = self.query.next(block_indexes, block_manager)?;

        if let Running(count) = self.state() {
            if *count >= self.options.limit.unwrap() {
                self.query.state = QueryState::Done;
                return Ok((reader, true));
            }
        }

        Ok((reader, last))
    }

    fn state(&self) -> &QueryState {
        self.query.state()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::query::base::tests::block_manager_and_index;
    use reduct_base::error::ErrorCode;
    use rstest::rstest;

    #[rstest]
    fn test_limit(block_manager_and_index: (Arc<RwLock<BlockManager>>, BTreeSet<u64>)) {
        let (mut block_manager, block_indexes) = block_manager_and_index;
        let mut block_manager = block_manager.write().unwrap();

        let mut query = LimitedQuery::new(
            0,
            u64::MAX,
            QueryOptions {
                limit: Some(1),
                ..Default::default()
            },
        );

        let (reader, last) = query.next(&block_indexes, &mut block_manager).unwrap();
        assert_eq!(reader.read().unwrap().timestamp(), 0);
        assert!(last);

        assert_eq!(
            query.next(&block_indexes, &mut block_manager).err(),
            Some(HttpError {
                status: ErrorCode::NoContent,
                message: "No content".to_string(),
            })
        );
    }
}
