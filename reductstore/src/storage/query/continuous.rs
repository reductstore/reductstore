// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::storage::block_manager::BlockManager;
use crate::storage::query::base::{Query, QueryOptions, QueryState};
use crate::storage::query::historical::HistoricalQuery;
use crate::storage::reader::RecordReader;
use reduct_base::error::{ErrorCode, HttpError};

use std::collections::BTreeSet;

use std::sync::{Arc, RwLock};

pub struct ContinuousQuery {
    query: HistoricalQuery,
    next_start: u64,
    count: usize,
    options: QueryOptions,
}

impl ContinuousQuery {
    pub fn new(start: u64, options: QueryOptions) -> ContinuousQuery {
        if !options.continuous {
            panic!("Continuous query must be continuous");
        }

        ContinuousQuery {
            query: HistoricalQuery::new(start, u64::MAX, options.clone()),
            next_start: start,
            count: 0,
            options,
        }
    }
}

impl Query for ContinuousQuery {
    fn next(
        &mut self,
        block_indexes: &BTreeSet<u64>,
        block_manager: &mut BlockManager,
    ) -> Result<(Arc<RwLock<RecordReader>>, bool), HttpError> {
        match self.query.next(block_indexes, block_manager) {
            Ok((record, last)) => {
                self.next_start = record.read().unwrap().timestamp() + 1;
                self.count += 1;
                Ok((record, last))
            }
            Err(HttpError {
                status: ErrorCode::NoContent,
                ..
            }) => {
                self.query = HistoricalQuery::new(self.next_start, u64::MAX, self.options.clone());
                self.query.state = QueryState::Running(self.count);
                Err(HttpError {
                    status: ErrorCode::NoContent,
                    message: "No content".to_string(),
                })
            }
            Err(err) => Err(err),
        }
    }

    fn state(&self) -> &QueryState {
        self.query.state()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use prost_wkt_types::Timestamp;
    use rstest::rstest;
    use std::thread::sleep;
    use tempfile::tempdir;

    use reduct_base::error::ErrorCode;

    use crate::storage::block_manager::ManageBlock;
    use crate::storage::proto::{record::State as RecordState, Record};
    use crate::storage::query::base::tests::block_manager_and_index;
    use crate::storage::writer::Chunk;

    #[rstest]
    fn test_query(block_manager_and_index: (Arc<RwLock<BlockManager>>, BTreeSet<u64>)) {
        let (block_manager, block_indexes) = block_manager_and_index;
        let mut block_manager = block_manager.write().unwrap();

        let mut query = ContinuousQuery::new(
            900,
            QueryOptions {
                ttl: std::time::Duration::from_millis(100),
                continuous: true,
                ..QueryOptions::default()
            },
        );
        {
            let (reader, _) = query.next(&block_indexes, &mut block_manager).unwrap();
            assert_eq!(reader.read().unwrap().timestamp(), 1000);
        }
        assert_eq!(
            query.next(&block_indexes, &mut block_manager).err(),
            Some(HttpError {
                status: ErrorCode::NoContent,
                message: "No content".to_string(),
            })
        );
        assert_eq!(
            query.next(&block_indexes, &mut block_manager).err(),
            Some(HttpError {
                status: ErrorCode::NoContent,
                message: "No content".to_string(),
            })
        );
        assert_eq!(query.state(), &QueryState::Running(1));

        sleep(std::time::Duration::from_millis(200));
        assert_eq!(query.state(), &QueryState::Expired);
    }
}
