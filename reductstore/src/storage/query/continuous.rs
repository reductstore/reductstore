// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::storage::block_manager::BlockManager;
use crate::storage::query::base::{Query, QueryOptions, QueryState};
use crate::storage::query::historical::HistoricalQuery;
use crate::storage::reader::RecordReader;
use reduct_base::error::{HttpError, HttpStatus};

use std::collections::BTreeSet;

use std::sync::{Arc, RwLock};

pub struct ContinuousQuery {
    query: HistoricalQuery,
    next_start: u64,
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
                Ok((record, last))
            }
            Err(HttpError {
                status: HttpStatus::NoContent,
                ..
            }) => {
                self.query = HistoricalQuery::new(self.next_start, u64::MAX, self.options.clone());
                Err(HttpError {
                    status: HttpStatus::NoContent,
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

    use std::thread::sleep;
    use tempfile::tempdir;

    use crate::storage::block_manager::ManageBlock;
    use crate::storage::proto::{record::State as RecordState, Record};
    use crate::storage::writer::Chunk;
    use reduct_base::error::HttpStatus;

    #[test]
    fn test_query() {
        let (block_manager, block_indexes) = setup();
        let mut block_manager = block_manager.write().unwrap();

        let mut query = ContinuousQuery::new(
            0,
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
                status: HttpStatus::NoContent,
                message: "No content".to_string(),
            })
        );
        assert_eq!(
            query.next(&block_indexes, &mut block_manager).err(),
            Some(HttpError {
                status: HttpStatus::NoContent,
                message: "No content".to_string(),
            })
        );
        assert_eq!(query.state(), &QueryState::Running);

        sleep(std::time::Duration::from_millis(200));
        assert_eq!(query.state(), &QueryState::Expired);
    }

    fn setup() -> (Arc<RwLock<BlockManager>>, BTreeSet<u64>) {
        let dir = tempdir().unwrap().into_path();
        let block_manager = BlockManager::new(dir);
        let mut block = block_manager.start(0, 10).unwrap();

        block.records.push(Record {
            timestamp: Some(Timestamp {
                seconds: 0,
                nanos: 1000000,
            }),
            begin: 0,
            end: 10,
            state: RecordState::Finished as i32,
            labels: vec![],
            content_type: "".to_string(),
        });

        block.latest_record_time = Some(Timestamp {
            seconds: 0,
            nanos: 0,
        });
        block.size = 10;
        block_manager.save(&block).unwrap();

        let bm_ref = Arc::new(RwLock::new(block_manager));
        {
            let writer = BlockManager::begin_write(Arc::clone(&bm_ref), &block, 0).unwrap();
            writer
                .write()
                .unwrap()
                .write(Chunk::Last(Bytes::from("0123456789")))
                .unwrap();
        }

        bm_ref.write().unwrap().finish(&block).unwrap();
        (Arc::clone(&bm_ref), BTreeSet::from([0]))
    }
}
