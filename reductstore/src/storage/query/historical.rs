// Copyright 2023-2024 ReductStore
// Licensed under the Business Source License 1.1

use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use tokio::sync::RwLock;

use reduct_base::error::ReductError;

use crate::storage::block_manager::{find_first_block, spawn_read_task, BlockManager, ManageBlock};
use crate::storage::bucket::RecordReader;
use crate::storage::proto::{record::State as RecordState, ts_to_us, Block, Record};
use crate::storage::query::base::{Query, QueryOptions, QueryState};
use crate::storage::query::filters::{
    ExcludeLabelFilter, IncludeLabelFilter, RecordFilter, RecordStateFilter, TimeRangeFilter,
};

pub struct HistoricalQuery {
    /// The start time of the query.
    start_time: u64,
    /// The stop time of the query.
    stop_time: u64,
    /// The records from the current block that have not been read yet.
    records_from_current_block: VecDeque<Record>,
    /// The current block that is being read. Cached to avoid loading the same block multiple times.
    current_block: Option<Block>,
    /// The time of the last update. We use this to check if the query has expired.
    last_update: Instant,
    /// The query options.
    options: QueryOptions,

    /// Filters
    filters: Vec<Box<dyn RecordFilter + Send + Sync>>,
    pub(in crate::storage::query) state: QueryState,
}

impl HistoricalQuery {
    pub fn new(start_time: u64, stop_time: u64, options: QueryOptions) -> HistoricalQuery {
        let mut filters: Vec<Box<dyn RecordFilter + Send + Sync>> = vec![
            Box::new(TimeRangeFilter::new(start_time, stop_time)),
            Box::new(RecordStateFilter::new(RecordState::Finished)),
        ];

        if !options.include.is_empty() {
            filters.push(Box::new(IncludeLabelFilter::new(options.include.clone())));
        }

        if !options.exclude.is_empty() {
            filters.push(Box::new(ExcludeLabelFilter::new(options.exclude.clone())));
        }

        HistoricalQuery {
            start_time,
            stop_time,
            records_from_current_block: VecDeque::new(),
            current_block: None,
            last_update: Instant::now(),
            options,
            filters,
            state: QueryState::Running(0),
        }
    }
}

#[async_trait]
impl Query for HistoricalQuery {
    async fn next(
        &mut self,
        block_indexes: &BTreeSet<u64>,
        block_manager: Arc<RwLock<BlockManager>>,
    ) -> Result<RecordReader, ReductError> {
        self.last_update = Instant::now();

        let records = &mut self.records_from_current_block.clone();
        if records.is_empty() {
            let start = if let Some(block) = &self.current_block {
                ts_to_us(block.records.last().unwrap().timestamp.as_ref().unwrap())
            } else {
                self.start_time
            };
            let first_block_id = find_first_block(block_indexes, &start);
            for block_id in block_indexes.range(first_block_id..self.stop_time) {
                let block = block_manager.write().await.load(*block_id).await?;

                if block.invalid {
                    continue;
                }

                self.current_block = Some(block);
                let mut found_records = self.filter_records_from_current_block();
                found_records.sort_by_key(|rec| ts_to_us(rec.timestamp.as_ref().unwrap()));
                records.extend(found_records);
                if !records.is_empty() {
                    break;
                }
            }
        }

        if records.is_empty() {
            self.state = QueryState::Done;
            return Err(ReductError::no_content("No content"));
        }

        let record = records.pop_front().unwrap();
        let block = self.current_block.as_ref().unwrap();
        let record_idx = block
            .records
            .iter()
            .position(|v| v.timestamp == record.timestamp)
            .unwrap();

        if let QueryState::Running(idx) = &mut self.state {
            *idx += 1;
        }

        let rx = spawn_read_task(Arc::clone(&block_manager), block, record_idx).await?;
        Ok(RecordReader::new(rx, record.clone(), false))
    }

    fn state(&self) -> &QueryState {
        if self.last_update.elapsed() > self.options.ttl {
            &QueryState::Expired
        } else {
            &self.state
        }
    }
}

impl HistoricalQuery {
    fn filter_records_from_current_block(&mut self) -> Vec<Record> {
        self.current_block
            .as_ref()
            .unwrap()
            .records
            .iter()
            .filter(|record| self.filters.iter_mut().all(|filter| filter.filter(record)))
            .map(|record| record.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use bytes::Bytes;
    use rstest::rstest;

    use reduct_base::error::ErrorCode;

    use crate::storage::proto::record::Label;
    use crate::storage::proto::{record, us_to_ts};
    use crate::storage::query::base::tests::block_manager_and_index;

    use super::*;

    #[rstest]
    fn test_state() {
        let mut query = HistoricalQuery::new(0, 5, QueryOptions::default());
        assert_eq!(query.state(), &QueryState::Running(0));
        query.last_update = Instant::now() - Duration::from_secs(61);
        assert_eq!(query.state(), &QueryState::Expired);
    }

    #[rstest]
    #[tokio::test]
    async fn test_query_ok_1_rec(
        #[future] block_manager_and_index: (Arc<RwLock<BlockManager>>, BTreeSet<u64>),
    ) {
        let mut query = HistoricalQuery::new(0, 5, QueryOptions::default());
        let records = read_to_vector(&mut query, block_manager_and_index.await).await;

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0.timestamp, Some(us_to_ts(&0)));
        assert_eq!(records[0].1, "0123456789");
        assert_eq!(query.state(), &QueryState::Done);
    }

    #[rstest]
    #[tokio::test]
    async fn test_query_ok_2_recs(
        #[future] block_manager_and_index: (Arc<RwLock<BlockManager>>, BTreeSet<u64>),
    ) {
        let mut query = HistoricalQuery::new(0, 1000, QueryOptions::default());
        let records = read_to_vector(&mut query, block_manager_and_index.await).await;

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].0.timestamp, Some(us_to_ts(&0)));
        assert_eq!(records[0].1, "0123456789");
        assert_eq!(records[1].0.timestamp, Some(us_to_ts(&5)));
        assert_eq!(records[1].1, "0123456789");
        assert_eq!(query.state(), &QueryState::Done);
    }

    #[rstest]
    #[tokio::test]
    async fn test_query_ok_3_recs(
        #[future] block_manager_and_index: (Arc<RwLock<BlockManager>>, BTreeSet<u64>),
    ) {
        let mut query = HistoricalQuery::new(0, 1001, QueryOptions::default());
        let records = read_to_vector(&mut query, block_manager_and_index.await).await;

        assert_eq!(records.len(), 3);
        assert_eq!(records[0].0.timestamp, Some(us_to_ts(&0)));
        assert_eq!(records[0].1, "0123456789");
        assert_eq!(records[1].0.timestamp, Some(us_to_ts(&5)));
        assert_eq!(records[1].1, "0123456789");
        assert_eq!(records[2].0.timestamp, Some(us_to_ts(&1000)));
        assert_eq!(records[2].1, "0123456789");
        assert_eq!(query.state(), &QueryState::Done);
    }

    #[rstest]
    #[tokio::test]
    async fn test_query_include(
        #[future] block_manager_and_index: (Arc<RwLock<BlockManager>>, BTreeSet<u64>),
    ) {
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
        let records = read_to_vector(&mut query, block_manager_and_index.await).await;

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0.timestamp, Some(us_to_ts(&1000)));
        assert_eq!(
            records[0].0.labels,
            vec![
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
        assert_eq!(records[0].1, "0123456789");
        assert_eq!(query.state(), &QueryState::Done);
    }

    #[rstest]
    #[tokio::test]
    async fn test_query_exclude(
        #[future] block_manager_and_index: (Arc<RwLock<BlockManager>>, BTreeSet<u64>),
    ) {
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

        let records = read_to_vector(&mut query, block_manager_and_index.await).await;

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].0.timestamp, Some(us_to_ts(&5)));
        assert_eq!(
            records[0].0.labels,
            vec![
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
        assert_eq!(records[0].1, "0123456789");
        assert_eq!(query.state(), &QueryState::Done);
    }

    #[rstest]
    #[tokio::test]
    async fn test_ignoring_errored_records(
        #[future] block_manager_and_index: (Arc<RwLock<BlockManager>>, BTreeSet<u64>),
    ) {
        let mut query = HistoricalQuery::new(0, 5, QueryOptions::default());

        let (block_manager, index) = block_manager_and_index.await;

        let mut block = block_manager
            .read()
            .await
            .load(*index.get(&0u64).unwrap())
            .await
            .unwrap();
        block.records[0].state = record::State::Errored as i32;
        block_manager.write().await.save(block).await.unwrap();

        assert_eq!(
            query.next(&index, block_manager.clone()).await.err(),
            Some(ReductError {
                status: ErrorCode::NoContent,
                message: "No content".to_string(),
            })
        );
    }

    async fn read_to_vector(
        query: &mut HistoricalQuery,
        block_manager_and_index: (Arc<RwLock<BlockManager>>, BTreeSet<u64>),
    ) -> Vec<(Record, String)> {
        let (block_manager, index) = block_manager_and_index;
        let mut records = Vec::new();
        loop {
            match query.next(&index, block_manager.clone()).await {
                Ok(mut reader) => {
                    let mut content = String::new();
                    while let Some(chunk) = reader.rx().recv().await {
                        content
                            .push_str(String::from_utf8(chunk.unwrap().to_vec()).unwrap().as_str())
                    }
                    records.push((reader.record().clone(), content));
                }
                Err(err) => {
                    assert_eq!(err.status, ErrorCode::NoContent);
                    break;
                }
            }
        }
        records
    }
}
