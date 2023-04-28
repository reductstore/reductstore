// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;
use std::time::Instant;

use crate::core::status::HTTPError;
use crate::storage::block_manager::{BlockManager, ManageBlock};
use crate::storage::proto::{record::State as RecordState, ts_to_us, Block, Record};
use crate::storage::query::base::{Query, QueryOptions, QueryState};
use crate::storage::reader::RecordReader;

pub struct HistoricalQuery {
    start_time: u64,
    stop_time: u64,
    next_record: usize,
    block: Option<Block>,
    last_update: Instant,
    options: QueryOptions,
    state: QueryState,
}

impl HistoricalQuery {
    pub fn new(start_time: u64, stop_time: u64, options: QueryOptions) -> HistoricalQuery {
        HistoricalQuery {
            start_time,
            stop_time,
            next_record: 0,
            block: None,
            last_update: Instant::now(),
            options,
            state: QueryState::Running,
        }
    }
}

impl Query for HistoricalQuery {
    fn next<'a>(
        &mut self,
        block_index: &BTreeSet<u64>,
        block_manager: &mut BlockManager,
    ) -> Result<(Rc<RefCell<RecordReader>>, bool), HTTPError> {
        self.last_update = Instant::now();

        let find_first_block = |start| -> u64 {
            let start_block_id = block_index.range(&start..).next();
            if start_block_id.is_some() && start <= *start_block_id.unwrap() {
                start
            } else {
                block_index.range(..&start).rev().next().unwrap().clone()
            }
        };

        let check_next_block = |start, stop| -> bool {
            let start = find_first_block(start);
            let next_block_id = block_index.range(start..stop).next();
            if let Some(next_block_id) = next_block_id {
                next_block_id >= &stop
            } else {
                true
            }
        };

        if let Some(block) = self.block.as_ref() {
            // We have a block, so we can just read the next record.
            let record_reader = block_manager.begin_read(block, self.next_record)?;
            self.next_record += 1;
            let last = if self.next_record >= block.records.len() {
                self.start_time = ts_to_us(block.latest_record_time.as_ref().unwrap()) + 1;
                self.block = None;
                self.next_record = 0;
                check_next_block(self.start_time, self.stop_time)
            } else {
                false
            };

            return Ok((record_reader, last));
        }

        let mut records: Vec<Record> = Vec::new();
        let mut block = Block::default();
        let start = find_first_block(self.start_time);
        for block_id in block_index.range(start..self.stop_time) {
            block = block_manager.load(*block_id)?;

            if block.invalid {
                continue;
            }

            let found_records: Vec<Record> = block
                .records
                .iter()
                .filter(|record| {
                    let ts = ts_to_us(record.timestamp.as_ref().unwrap());
                    ts >= self.start_time
                        && ts < self.stop_time
                        && record.state == RecordState::Finished as i32
                })
                .filter(|record| {
                    if self.options.include.is_empty() {
                        true
                    } else {
                        self.options.include.iter().all(|(key, value)| {
                            record
                                .labels
                                .iter()
                                .any(|label| label.name == *key && label.value == *value)
                        })
                    }
                })
                .filter(|record| {
                    if self.options.exclude.is_empty() {
                        true
                    } else {
                        !self.options.exclude.iter().all(|(key, value)| {
                            record
                                .labels
                                .iter()
                                .any(|label| label.name == *key && label.value == *value)
                        })
                    }
                })
                .map(|record| record.clone())
                .collect();

            records.extend(found_records);

            if !records.is_empty() || self.options.include.is_empty() {
                break;
            }
        }

        if records.is_empty() {
            self.state = QueryState::Done;
            return Err(HTTPError::no_content("No content"));
        }

        records.sort_by_key(|rec| ts_to_us(rec.timestamp.as_ref().unwrap()));
        let record = &records[0];
        self.start_time = ts_to_us(record.timestamp.as_ref().unwrap()) + 1;

        let last = if records.len() > 1 {
            // Only one record in current block check next one
            self.block = Some(block.clone());
            self.next_record = 1;
            false
        } else {
            check_next_block(self.start_time, self.stop_time)
        };

        let record_idx = block
            .records
            .iter()
            .position(|v| v.timestamp == record.timestamp)
            .unwrap();
        Ok((block_manager.begin_read(&block, record_idx)?, last))
    }

    fn state(&self) -> &QueryState {
        if self.last_update.elapsed() > self.options.ttl {
            &QueryState::Expired
        } else {
            &self.state
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::reader::DataChunk;
    use std::collections::HashMap;
    use std::time::Duration;
    use tempfile::tempdir;

    #[test]
    fn test_state() {
        let mut query = HistoricalQuery::new(0, 5, QueryOptions::default());
        assert_eq!(query.state(), &QueryState::Running);
        query.last_update = Instant::now() - Duration::from_secs(61);
        assert_eq!(query.state(), &QueryState::Expired);
    }

    #[test]
    fn test_query_ok_1_rec() {
        let mut query = HistoricalQuery::new(0, 5, QueryOptions::default());

        let (mut block_manager, index) = setup();
        {
            let (reader, _) = query.next(&index, &mut block_manager).unwrap();
            assert_eq!(
                reader.borrow_mut().read().unwrap(),
                DataChunk {
                    data: Vec::from("0123456789"),
                    last: true,
                }
            );
        }
        {
            let res = query.next(&index, &mut block_manager);
            assert!(res.is_err());
            assert_eq!(res.err().unwrap().status, HTTPStatus::NoContent);
            assert_eq!(query.state(), &QueryState::Done);
        }
    }

    #[test]
    fn test_query_ok_2_recs() {
        let mut query = HistoricalQuery::new(0, 1000, QueryOptions::default());

        let (mut block_manager, index) = setup();
        {
            let (reader, _) = query.next(&index, &mut block_manager).unwrap();
            assert_eq!(
                reader.borrow_mut().read().unwrap(),
                DataChunk {
                    data: Vec::from("0123456789"),
                    last: true,
                }
            );
        }
        {
            let (reader, _) = query.next(&index, &mut block_manager).unwrap();
            assert_eq!(
                reader.borrow_mut().read().unwrap(),
                DataChunk {
                    data: Vec::from("0123456789"),
                    last: true,
                }
            );
        }

        assert_eq!(
            query.next(&index, &mut block_manager).err(),
            Some(HTTPError {
                status: HTTPStatus::NoContent,
                message: "No content".to_string(),
            })
        );
        assert_eq!(query.state(), &QueryState::Done);
    }

    #[test]
    fn test_query_ok_3_recs() {
        let mut query = HistoricalQuery::new(0, 1001, QueryOptions::default());

        let (mut block_manager, index) = setup();
        {
            let (reader, _) = query.next(&index, &mut block_manager).unwrap();
            assert_eq!(
                reader.borrow_mut().read().unwrap(),
                DataChunk {
                    data: Vec::from("0123456789"),
                    last: true,
                }
            );
        }
        {
            let (reader, _) = query.next(&index, &mut block_manager).unwrap();
            assert_eq!(
                reader.borrow_mut().read().unwrap(),
                DataChunk {
                    data: Vec::from("0123456789"),
                    last: true,
                }
            );
        }
        {
            let (reader, _) = query.next(&index, &mut block_manager).unwrap();
            assert_eq!(
                reader.borrow_mut().read().unwrap(),
                DataChunk {
                    data: Vec::from("0123456789"),
                    last: true,
                }
            );
        }
        assert_eq!(
            query.next(&index, &mut block_manager).err(),
            Some(HTTPError {
                status: HTTPStatus::NoContent,
                message: "No content".to_string(),
            })
        );
        assert_eq!(query.state(), &QueryState::Done);
    }

    #[test]
    fn test_query_include() {
        let mut query = HistoricalQuery::new(
            0,
            1001,
            QueryOptions {
                include: HashMap::from([
                    ("block".to_string(), "2".to_string()),
                    ("record".to_string(), "1".to_string()),
                ]),
                ..QueryOptions::default()
            },
        );
        let (mut block_manager, index) = setup();
        {
            let (reader, _) = query.next(&index, &mut block_manager).unwrap();
            assert_eq!(
                reader.borrow().labels(),
                &HashMap::from([
                    ("block".to_string(), "2".to_string()),
                    ("record".to_string(), "1".to_string()),
                ])
            );
        }

        assert_eq!(
            query.next(&index, &mut block_manager).err(),
            Some(HTTPError {
                status: HTTPStatus::NoContent,
                message: "No content".to_string(),
            })
        );
        assert_eq!(query.state(), &QueryState::Done);
    }

    #[test]
    fn test_query_exclude() {
        let mut query = HistoricalQuery::new(
            0,
            1001,
            QueryOptions {
                exclude: HashMap::from([
                    ("block".to_string(), "1".to_string()),
                    ("record".to_string(), "1".to_string()),
                ]),
                ..QueryOptions::default()
            },
        );
        let (mut block_manager, index) = setup();
        {
            let (reader, _) = query.next(&index, &mut block_manager).unwrap();
            assert_eq!(
                reader.borrow().labels(),
                &HashMap::from([
                    ("block".to_string(), "1".to_string()),
                    ("record".to_string(), "2".to_string()),
                ])
            );
        }
        {
            let (reader, _) = query.next(&index, &mut block_manager).unwrap();
            assert_eq!(
                reader.borrow().labels(),
                &HashMap::from([
                    ("block".to_string(), "2".to_string()),
                    ("record".to_string(), "1".to_string()),
                ])
            );
        }

        assert_eq!(
            query.next(&index, &mut block_manager).err(),
            Some(HTTPError {
                status: HTTPStatus::NoContent,
                message: "No content".to_string(),
            })
        );
        assert_eq!(query.state(), &QueryState::Done);
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
        block_manager.save(&block).unwrap();

        {
            let writer = block_manager.begin_write(&block, 0).unwrap();

            writer.borrow_mut().write(b"0123456789", true).unwrap();
        }

        {
            let writer = block_manager.begin_write(&block, 1).unwrap();
            writer.borrow_mut().write(b"0123456789", true).unwrap();
        }

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
        block_manager.save(&block).unwrap();

        {
            let writer = block_manager.begin_write(&block, 0).unwrap();
            writer.borrow_mut().write(b"0123456789", true).unwrap();
        }

        block_manager.finish(&block).unwrap();
        (block_manager, BTreeSet::from([0, 1000]))
    }
}
