// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::core::status::{HttpError, HttpStatus};
use crate::storage::block_manager::BlockManager;
use crate::storage::query::base::{Query, QueryOptions, QueryState};
use crate::storage::query::historical::HistoricalQuery;
use crate::storage::reader::RecordReader;

use std::collections::BTreeSet;

use std::sync::{Arc, RwLock};

pub struct ContinuousQuery {
    query: HistoricalQuery,
    last_timestamp: u64,
    options: QueryOptions,
}

impl ContinuousQuery {
    pub fn new(start: u64, options: QueryOptions) -> ContinuousQuery {
        if !options.continuous {
            panic!("Continuous query must be continuous");
        }

        ContinuousQuery {
            query: HistoricalQuery::new(start, u64::MAX, options.clone()),
            last_timestamp: 0,
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
                self.last_timestamp = record.read().unwrap().timestamp();
                Ok((record, last))
            }
            Err(HttpError {
                status: HttpStatus::NoContent,
                ..
            }) => {
                self.query =
                    HistoricalQuery::new(self.last_timestamp + 1, u64::MAX, self.options.clone());
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
    use prost_wkt_types::Timestamp;
    use std::thread::sleep;
    use tempfile::tempdir;

    use crate::core::status::HttpStatus;
    use crate::storage::block_manager::ManageBlock;
    use crate::storage::proto::{record::State as RecordState, Record};

    #[test]
    fn test_query() {
        let (mut block_manager, block_indexes) = setup();

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
            assert_eq!(reader.read().unwrap().timestamp(), 0);
        }
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

    fn setup() -> (BlockManager, BTreeSet<u64>) {
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
            labels: vec![],
            content_type: "".to_string(),
        });

        block.latest_record_time = Some(Timestamp {
            seconds: 0,
            nanos: 0,
        });
        block.size = 10;
        block_manager.save(&block).unwrap();

        {
            let writer = block_manager.begin_write(&block, 0).unwrap();
            writer.write().unwrap().write(b"0123456789", true).unwrap();
        }

        block_manager.finish(&block).unwrap();
        (block_manager, BTreeSet::from([0]))
    }
}
