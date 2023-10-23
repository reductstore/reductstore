// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use std::collections::BTreeSet;

use async_trait::async_trait;
use bytes::Bytes;
use log::error;
use std::sync::{Arc, RwLock};
use std::time::Instant;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc::Sender;

use crate::storage::block_manager::{
    find_first_block, BlockManager, ManageBlock, DEFAULT_MAX_READ_CHUNK,
};
use crate::storage::bucket::RecordReader;
use crate::storage::proto::{record::State as RecordState, ts_to_us, us_to_ts, Block, Record};
use crate::storage::query::base::{Query, QueryOptions, QueryState};
use reduct_base::error::ReductError;

pub struct HistoricalQuery {
    start_time: u64,
    stop_time: u64,
    block: Option<Block>,
    last_update: Instant,
    options: QueryOptions,
    pub(in crate::storage::query) state: QueryState,
}

impl HistoricalQuery {
    pub fn new(start_time: u64, stop_time: u64, options: QueryOptions) -> HistoricalQuery {
        HistoricalQuery {
            start_time,
            stop_time,
            block: None,
            last_update: Instant::now(),
            options,
            state: QueryState::Running(0),
        }
    }
}

#[async_trait]
impl Query for HistoricalQuery {
    async fn next(
        &mut self,
        block_index: &BTreeSet<u64>,
        block_manager: &mut BlockManager,
    ) -> Result<RecordReader, ReductError> {
        self.last_update = Instant::now();

        let check_next_block = |start, stop| -> bool {
            let start = find_first_block(block_index, start);
            let next_block_id = block_index.range(start..stop).next();
            if let Some(next_block_id) = next_block_id {
                next_block_id >= &stop
            } else {
                true
            }
        };

        let filter_records = |block: &Block| -> Vec<Record> {
            block
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
                .collect()
        };

        let mut records: Vec<Record> = Vec::new();
        let mut block = Block::default();
        let start = find_first_block(block_index, &self.start_time);
        for block_id in block_index.range(start..self.stop_time) {
            block = if let Some(block) = &self.block {
                if block.begin_time == Some(us_to_ts(block_id)) {
                    block.clone()
                } else {
                    block_manager.load(*block_id)?
                }
            } else {
                block_manager.load(*block_id)?
            };

            if block.invalid {
                continue;
            }

            let found_records = filter_records(&block);
            records.extend(found_records);
            if !records.is_empty() {
                break;
            }
        }

        if records.is_empty() {
            self.state = QueryState::Done;
            return Err(ReductError::no_content("No content"));
        }

        records.sort_by_key(|rec| ts_to_us(rec.timestamp.as_ref().unwrap()));
        let record = &records[0];
        self.start_time = ts_to_us(record.timestamp.as_ref().unwrap()) + 1;

        let last = if records.len() > 1 {
            // Only one record in current block check next one
            self.block = Some(block.clone());
            false
        } else {
            self.block = None;
            check_next_block(&self.start_time, self.stop_time)
        };

        let record_idx = block
            .records
            .iter()
            .position(|v| v.timestamp == record.timestamp)
            .unwrap();

        if let QueryState::Running(idx) = &mut self.state {
            *idx += 1;
        }

        let file = block_manager.begin_read(&block, record_idx).await?;
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            let mut file = file;
            let mut offset = 0;
            let mut buf = vec![0; DEFAULT_MAX_READ_CHUNK as usize];
            loop {
                let read = file.read(&mut buf).await.unwrap();
                if read == 0 {
                    break;
                }
                offset += read;
                if let Err(e) = tx.send(Ok(Bytes::from(buf[..read].to_vec()))).await {
                    error!("Failed to send record chunk: {}", e);
                    break;
                }
            }
        });
        Ok(RecordReader::new(rx, record.clone(), last))
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
    use crate::storage::proto::record;

    use bytes::Bytes;

    use crate::storage::proto::record::Label;
    use reduct_base::error::ErrorCode;
    use rstest::rstest;
    use std::collections::HashMap;
    use std::time::Duration;

    use crate::storage::query::base::tests::block_manager_and_index;

    #[rstest]
    fn test_state() {
        let mut query = HistoricalQuery::new(0, 5, QueryOptions::default());
        assert_eq!(query.state(), &QueryState::Running(0));
        query.last_update = Instant::now() - Duration::from_secs(61);
        assert_eq!(query.state(), &QueryState::Expired);
    }

    #[rstest]
    #[tokio::test]
    async fn test_query_ok_1_rec(#[future] block_manager_and_index: (BlockManager, BTreeSet<u64>)) {
        let mut query = HistoricalQuery::new(0, 5, QueryOptions::default());

        let (mut block_manager, index) = block_manager_and_index.await;
        {
            let mut reader = query.next(&index, &mut block_manager).await.unwrap();
            assert_eq!(
                reader.rx().recv().await.unwrap(),
                Ok(Bytes::from("0123456789"))
            );
            assert!(reader.rx().recv().await.is_none())
        }
        {
            let res = query.next(&index, &mut block_manager).await;
            assert!(res.is_err());
            assert_eq!(res.err().unwrap().status, ErrorCode::NoContent);
            assert_eq!(query.state(), &QueryState::Done);
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_query_ok_2_recs(
        #[future] block_manager_and_index: (BlockManager, BTreeSet<u64>),
    ) {
        let mut query = HistoricalQuery::new(0, 1000, QueryOptions::default());

        let (mut block_manager, index) = block_manager_and_index.await;
        {
            let mut reader = query.next(&index, &mut block_manager).await.unwrap();
            assert_eq!(
                reader.rx().recv().await.unwrap(),
                Ok(Bytes::from("0123456789"))
            );
            assert!(reader.rx().recv().await.is_none())
        }
        {
            let mut reader = query.next(&index, &mut block_manager).await.unwrap();
            assert_eq!(
                reader.rx().recv().await.unwrap(),
                Ok(Bytes::from("0123456789"))
            );
            assert!(reader.rx().recv().await.is_none())
        }

        assert_eq!(
            query.next(&index, &mut block_manager).await.err(),
            Some(ReductError {
                status: ErrorCode::NoContent,
                message: "No content".to_string(),
            })
        );
        assert_eq!(query.state(), &QueryState::Done);
    }

    #[rstest]
    #[tokio::test]
    async fn test_query_ok_3_recs(
        #[future] block_manager_and_index: (BlockManager, BTreeSet<u64>),
    ) {
        let mut query = HistoricalQuery::new(0, 1001, QueryOptions::default());

        let (mut block_manager, index) = block_manager_and_index.await;
        {
            let mut reader = query.next(&index, &mut block_manager).await.unwrap();

            assert_eq!(
                reader.rx().recv().await.unwrap(),
                Ok(Bytes::from("0123456789"))
            );
            assert!(reader.rx().recv().await.is_none())
        }
        {
            let mut reader = query.next(&index, &mut block_manager).await.unwrap();
            assert_eq!(
                reader.rx().recv().await.unwrap(),
                Ok(Bytes::from("0123456789"))
            );
            assert!(reader.rx().recv().await.is_none())
        }
        {
            let mut reader = query.next(&index, &mut block_manager).await.unwrap();
            assert_eq!(
                reader.rx().recv().await.unwrap(),
                Ok(Bytes::from("0123456789"))
            );
            assert!(reader.rx().recv().await.is_none())
        }
        assert_eq!(
            query.next(&index, &mut block_manager).await.err(),
            Some(ReductError {
                status: ErrorCode::NoContent,
                message: "No content".to_string(),
            })
        );
        assert_eq!(query.state(), &QueryState::Done);
    }

    #[rstest]
    #[tokio::test]
    async fn test_query_include(#[future] block_manager_and_index: (BlockManager, BTreeSet<u64>)) {
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
        let (mut block_manager, index) = block_manager_and_index.await;
        {
            let mut reader = query.next(&index, &mut block_manager).await.unwrap();
            assert_eq!(
                reader.labels(),
                &vec![
                    Label {
                        name: "block".to_string(),
                        value: "2".to_string(),
                    },
                    Label {
                        name: "record".to_string(),
                        value: "1".to_string(),
                    },
                ]
            );
        }

        assert_eq!(
            query.next(&index, &mut block_manager).await.err(),
            Some(ReductError {
                status: ErrorCode::NoContent,
                message: "No content".to_string(),
            })
        );
        assert_eq!(query.state(), &QueryState::Done);
    }

    #[rstest]
    #[tokio::test]
    async fn test_query_exclude(#[future] block_manager_and_index: (BlockManager, BTreeSet<u64>)) {
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

        let (mut block_manager, index) = block_manager_and_index.await;
        {
            let reader = query.next(&index, &mut block_manager).await.unwrap();
            assert_eq!(
                reader.labels(),
                &vec![
                    Label {
                        name: "block".to_string(),
                        value: "1".to_string(),
                    },
                    Label {
                        name: "record".to_string(),
                        value: "2".to_string(),
                    },
                ]
            );
        }
        {
            let reader = query.next(&index, &mut block_manager).await.unwrap();
            assert_eq!(
                reader.labels(),
                &vec![
                    Label {
                        name: "block".to_string(),
                        value: "2".to_string(),
                    },
                    Label {
                        name: "record".to_string(),
                        value: "1".to_string(),
                    },
                ]
            );
        }

        assert_eq!(
            query.next(&index, &mut block_manager).await.err(),
            Some(ReductError {
                status: ErrorCode::NoContent,
                message: "No content".to_string(),
            })
        );
        assert_eq!(query.state(), &QueryState::Done);
    }

    #[rstest]
    #[tokio::test]
    async fn test_ignoring_errored_records(
        #[future] block_manager_and_index: (BlockManager, BTreeSet<u64>),
    ) {
        let mut query = HistoricalQuery::new(0, 5, QueryOptions::default());

        let (mut block_manager, index) = block_manager_and_index.await;

        let mut block = block_manager.load(*index.get(&0u64).unwrap()).unwrap();
        block.records[0].state = record::State::Errored as i32;
        block_manager.save(block).unwrap();

        assert_eq!(
            query.next(&index, &mut block_manager).await.err(),
            Some(ReductError {
                status: ErrorCode::NoContent,
                message: "No content".to_string(),
            })
        );
    }
}
