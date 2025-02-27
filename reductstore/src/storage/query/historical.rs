// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use std::collections::VecDeque;
use std::sync::{Arc, RwLock};

use reduct_base::error::ReductError;

use crate::storage::block_manager::{BlockManager, BlockRef};
use crate::storage::entry::RecordReader;
use crate::storage::proto::record::Label;
use crate::storage::proto::{record::State as RecordState, ts_to_us, Record};
use crate::storage::query::base::{Query, QueryOptions};
use crate::storage::query::condition::Parser;
use crate::storage::query::filters::{
    EachNFilter, EachSecondFilter, ExcludeLabelFilter, FilterPoint, IncludeLabelFilter,
    RecordFilter, RecordStateFilter, TimeRangeFilter, WhenFilter,
};

impl FilterPoint for Record {
    fn timestamp(&self) -> i64 {
        ts_to_us(self.timestamp.as_ref().unwrap()) as i64
    }

    fn labels(&self) -> &Vec<Label> {
        &self.labels
    }

    fn state(&self) -> &i32 {
        &self.state
    }
}

pub struct HistoricalQuery {
    /// The start time of the query.
    start_time: u64,
    /// The stop time of the query.
    stop_time: u64,
    /// The records from the current block that have not been read yet.
    records_from_current_block: VecDeque<Record>,
    /// The current block that is being read. Cached to avoid loading the same block multiple times.
    current_block: Option<BlockRef>,
    /// Filters
    filters: Vec<Box<dyn RecordFilter<Record> + Send + Sync>>,
    /// Request only metadata without the content.
    only_metadata: bool,
    /// Strict mode
    strict: bool,
}

impl HistoricalQuery {
    pub fn try_new(
        start_time: u64,
        stop_time: u64,
        options: QueryOptions,
    ) -> Result<Self, ReductError> {
        let mut filters: Vec<Box<dyn RecordFilter<Record> + Send + Sync>> = vec![
            Box::new(TimeRangeFilter::new(start_time, stop_time)),
            Box::new(RecordStateFilter::new(RecordState::Finished)),
        ];

        if !options.include.is_empty() {
            filters.push(Box::new(IncludeLabelFilter::new(options.include.clone())));
        }

        if !options.exclude.is_empty() {
            filters.push(Box::new(ExcludeLabelFilter::new(options.exclude.clone())));
        }

        if let Some(each_s) = options.each_s {
            filters.push(Box::new(EachSecondFilter::new(each_s)));
        }

        if let Some(each_n) = options.each_n {
            filters.push(Box::new(EachNFilter::new(each_n)));
        }

        if let Some(when) = options.when {
            let parser = Parser::new();
            let condition = parser.parse(&when)?;
            filters.push(Box::new(WhenFilter::new(condition)));
        }

        Ok(HistoricalQuery {
            start_time,
            stop_time,
            records_from_current_block: VecDeque::new(),
            current_block: None,
            filters,
            only_metadata: options.only_metadata,
            strict: options.strict,
        })
    }
}

impl Query for HistoricalQuery {
    fn next(
        &mut self,
        block_manager: Arc<RwLock<BlockManager>>,
    ) -> Result<RecordReader, ReductError> {
        if self.records_from_current_block.is_empty() {
            let start = if let Some(block) = &self.current_block {
                let block = block.read()?;
                block.latest_record_time()
            } else {
                self.start_time
            };

            let block_range = {
                let mut bm = block_manager.write()?;
                let first_block = {
                    if let Ok(block) = bm.find_block(start) {
                        block.read()?.block_id()
                    } else {
                        0
                    }
                };
                bm.index()
                    .tree()
                    .range(first_block..self.stop_time)
                    .map(|k| *k)
                    .collect::<Vec<u64>>()
            };

            for block_id in block_range {
                let mut bm = block_manager.write()?;
                let block_ref = bm.load_block(block_id)?;

                self.current_block = Some(block_ref);
                let mut found_records = self.filter_records_from_current_block()?;
                found_records.sort_by_key(|rec| ts_to_us(rec.timestamp.as_ref().unwrap()));
                self.records_from_current_block.extend(found_records);
                if !self.records_from_current_block.is_empty() {
                    break;
                }
            }
        }

        if self.records_from_current_block.is_empty() {
            return Err(ReductError::no_content("No content"));
        }

        let record = self.records_from_current_block.pop_front().unwrap();
        let block = self.current_block.as_ref().unwrap();

        if self.only_metadata {
            Ok(RecordReader::form_record(record.clone(), false))
        } else {
            RecordReader::try_new(
                Arc::clone(&block_manager),
                block.clone(),
                ts_to_us(&record.timestamp.unwrap()),
                false,
            )
        }
    }
}

impl HistoricalQuery {
    fn filter_records_from_current_block(&mut self) -> Result<Vec<Record>, ReductError> {
        let block = self.current_block.as_ref().unwrap().read()?;
        let mut filtered_records = Vec::new();
        for record in block.record_index().values() {
            let mut include_record = true;
            for filter in self.filters.iter_mut() {
                match filter.filter(record) {
                    Ok(false) => {
                        include_record = false;
                        break;
                    }
                    Ok(true) => {}
                    Err(err) => {
                        if self.strict {
                            // in strict mode, we return an error if a filter fails
                            return Err(err);
                        }

                        // in non-strict mode, we ignore the record with the failed filter
                        include_record = false;
                        break;
                    }
                }
            }
            if include_record {
                filtered_records.push(record.clone());
            }
        }

        Ok(filtered_records)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rstest::rstest;

    use crate::storage::proto::record::Label;
    use crate::storage::proto::{record, us_to_ts};
    use crate::storage::query::base::tests::block_manager;
    use reduct_base::error::ErrorCode;
    use reduct_base::{no_content, not_found};

    use super::*;

    #[rstest]
    fn test_query_ok_1_rec(block_manager: Arc<RwLock<BlockManager>>) {
        let mut query = HistoricalQuery::try_new(0, 5, QueryOptions::default()).unwrap();
        let records = read_to_vector(&mut query, block_manager);

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0.timestamp, Some(us_to_ts(&0)));
        assert_eq!(records[0].1, "0123456789");
    }

    #[rstest]
    fn test_query_ok_2_recs(block_manager: Arc<RwLock<BlockManager>>) {
        let mut query = HistoricalQuery::try_new(0, 1000, QueryOptions::default()).unwrap();
        let records = read_to_vector(&mut query, block_manager);

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].0.timestamp, Some(us_to_ts(&0)));
        assert_eq!(records[0].1, "0123456789");
        assert_eq!(records[1].0.timestamp, Some(us_to_ts(&5)));
        assert_eq!(records[1].1, "0123456789");
    }

    #[rstest]
    fn test_query_ok_3_recs(block_manager: Arc<RwLock<BlockManager>>) {
        let mut query = HistoricalQuery::try_new(0, 1001, QueryOptions::default()).unwrap();
        let records = read_to_vector(&mut query, block_manager);

        assert_eq!(records.len(), 3);
        assert_eq!(records[0].0.timestamp, Some(us_to_ts(&0)));
        assert_eq!(records[0].1, "0123456789");
        assert_eq!(records[1].0.timestamp, Some(us_to_ts(&5)));
        assert_eq!(records[1].1, "0123456789");
        assert_eq!(records[2].0.timestamp, Some(us_to_ts(&1000)));
        assert_eq!(records[2].1, "0123456789");
    }

    #[rstest]
    fn test_query_include(block_manager: Arc<RwLock<BlockManager>>) {
        let mut query = HistoricalQuery::try_new(
            0,
            1001,
            QueryOptions {
                include: HashMap::from([
                    ("block".to_string(), "2".to_string()),
                    ("record".to_string(), "1".to_string()),
                ]),
                ..QueryOptions::default()
            },
        )
        .unwrap();
        let records = read_to_vector(&mut query, block_manager);

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
    }

    #[rstest]
    fn test_query_exclude(block_manager: Arc<RwLock<BlockManager>>) {
        let mut query = HistoricalQuery::try_new(
            0,
            1001,
            QueryOptions {
                exclude: HashMap::from([
                    ("block".to_string(), "1".to_string()),
                    ("record".to_string(), "1".to_string()),
                ]),
                ..QueryOptions::default()
            },
        )
        .unwrap();

        let records = read_to_vector(&mut query, block_manager);

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
                Label {
                    name: "flag".to_string(),
                    value: "false".to_string(),
                },
            ]
        );
        assert_eq!(records[0].1, "0123456789");
    }

    #[rstest]
    fn test_ignoring_errored_records(block_manager: Arc<RwLock<BlockManager>>) {
        let mut query = HistoricalQuery::try_new(0, 5, QueryOptions::default()).unwrap();
        {
            let block_ref = block_manager.write().unwrap().load_block(0).unwrap();
            {
                let mut block = block_ref.write().unwrap();
                let mut record = block.get_record(0).unwrap().clone();
                record.state = record::State::Errored as i32;
                block.insert_or_update_record(record);
            }
            block_manager
                .write()
                .unwrap()
                .save_block(block_ref)
                .unwrap();
        }

        assert_eq!(
            query.next(block_manager.clone()).err(),
            Some(no_content!("No content"))
        );
    }

    #[rstest]
    fn test_each_s_filter(block_manager: Arc<RwLock<BlockManager>>) {
        let mut query = HistoricalQuery::try_new(
            0,
            1001,
            QueryOptions {
                each_s: Some(0.00001),
                ..QueryOptions::default()
            },
        )
        .unwrap();
        let records = read_to_vector(&mut query, block_manager);

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].0.timestamp, Some(us_to_ts(&0)));
        assert_eq!(records[1].0.timestamp, Some(us_to_ts(&1000)));
    }

    #[rstest]
    fn test_each_n_records(block_manager: Arc<RwLock<BlockManager>>) {
        let mut query = HistoricalQuery::try_new(
            0,
            1001,
            QueryOptions {
                each_n: Some(2),
                ..QueryOptions::default()
            },
        )
        .unwrap();
        let records = read_to_vector(&mut query, block_manager);

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].0.timestamp, Some(us_to_ts(&0)));
        assert_eq!(records[1].0.timestamp, Some(us_to_ts(&1000)));
    }

    #[rstest]
    fn test_when_filter(block_manager: Arc<RwLock<BlockManager>>) {
        let mut query = HistoricalQuery::try_new(
            0,
            1001,
            QueryOptions {
                when: Some(serde_json::from_str(r#"{"$and": ["&flag"]}"#).unwrap()),
                ..QueryOptions::default()
            },
        )
        .unwrap();
        let records = read_to_vector(&mut query, block_manager);

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0.timestamp, Some(us_to_ts(&0)));
    }

    #[rstest]
    fn test_when_filter_strict(block_manager: Arc<RwLock<BlockManager>>) {
        let mut query = HistoricalQuery::try_new(
            0,
            1001,
            QueryOptions {
                when: Some(serde_json::from_str(r#"{"$and": ["&NOT_EXIST"]}"#).unwrap()),
                strict: true,
                ..QueryOptions::default()
            },
        )
        .unwrap();
        assert_eq!(
            query.next(block_manager.clone()).err(),
            Some(not_found!("Reference 'NOT_EXIST' not found"))
        );
    }

    #[rstest]
    fn test_when_filter_non_strict(block_manager: Arc<RwLock<BlockManager>>) {
        let mut query = HistoricalQuery::try_new(
            0,
            1001,
            QueryOptions {
                when: Some(serde_json::from_str(r#"{"$and": ["&NOT_EXIST"]}"#).unwrap()),
                strict: false,
                ..QueryOptions::default()
            },
        )
        .unwrap();
        assert_eq!(
            query.next(block_manager.clone()).err(),
            Some(no_content!("No content")),
            "errored condition should be ignored in non-strict mode"
        );
    }

    fn read_to_vector(
        query: &mut HistoricalQuery,
        block_manager: Arc<RwLock<BlockManager>>,
    ) -> Vec<(Record, String)> {
        let mut records = Vec::new();
        loop {
            match query.next(block_manager.clone()) {
                Ok(mut reader) => {
                    let mut content = String::new();
                    while let Some(chunk) = reader.rx().blocking_recv() {
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
