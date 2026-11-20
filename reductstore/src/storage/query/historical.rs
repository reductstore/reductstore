// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::cfg::io::IoConfig;
use crate::core::sync::AsyncRwLock;
use crate::storage::block_manager::{BlockManager, BlockRef};
use crate::storage::entry::RecordReader;
use crate::storage::proto::record::State as RecordState;
use crate::storage::query::base::{Query, QueryOptions};
use crate::storage::query::condition::Parser;
use crate::storage::query::filters::{
    apply_filters_recursively, FilterRecord, RecordFilter, RecordStateFilter, TimeRangeFilter,
    WhenFilter,
};
use async_trait::async_trait;
use log::debug;
use reduct_base::error::ErrorCode;
use reduct_base::error::ReductError;
use reduct_base::io::RecordMeta;
use std::collections::{HashMap, VecDeque};
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::Arc;

impl FilterRecord for RecordMeta {
    fn state(&self) -> i32 {
        self.state()
    }

    fn timestamp(&self) -> u64 {
        self.timestamp()
    }

    fn labels(&self) -> HashMap<&String, &String> {
        self.labels().iter().collect()
    }

    fn set_labels(&mut self, labels: HashMap<String, String>) {
        *self.labels_mut() = labels;
    }

    fn computed_labels(&self) -> HashMap<&String, &String> {
        self.computed_labels().iter().collect()
    }
}

type Filter = Box<dyn RecordFilter<RecordMeta> + Send + Sync>;

pub struct HistoricalQuery {
    /// The start time of the query.
    start_time: u64,
    /// The stop time of the query.
    stop_time: u64,
    /// Entry name
    entry_name: String,
    /// The records from the current block that have not been read yet.
    records_from_current_blocks: VecDeque<RecordMeta>,
    /// The current block that is being read. Cached to avoid loading the same block multiple times.
    block_candidates: Vec<BlockRef>,
    /// Last block scanned, so subsequent reads do not revisit it.
    last_scanned_block_id: Option<u64>,
    /// Filters
    filters: Vec<Filter>,
    /// Request only metadata without the content.
    only_metadata: bool,
    /// Interrupted query
    is_interrupted: bool,
    /// Io Config
    io_config: IoConfig,
}

impl HistoricalQuery {
    pub fn try_new(
        entry_name: String,
        start_time: u64,
        stop_time: u64,
        options: QueryOptions,
        io_defaults: IoConfig,
    ) -> Result<Self, ReductError> {
        let mut filters: Vec<Filter> = vec![
            Box::new(TimeRangeFilter::new(start_time, stop_time)),
            Box::new(RecordStateFilter::new(RecordState::Finished)),
        ];

        // If extensions are requested, we need to fetch the content.
        let mut only_metadata = if options.ext.is_none() {
            options.only_metadata
        } else {
            false
        };
        let mut io_config = io_defaults;

        if let Some(when) = options.when {
            let parser = Parser::new();
            let (condition, directives) = parser.parse(when)?;
            if directives.contains_key("#ext") {
                only_metadata = false;
            }

            // get io config from directives and override defaults from configuraion
            let filter = WhenFilter::try_new(condition, directives, io_config, options.strict)?;

            io_config = filter.io_config().clone();
            filters.push(Box::new(filter));
        }

        Ok(HistoricalQuery {
            entry_name,
            start_time,
            stop_time,
            records_from_current_blocks: VecDeque::new(),
            block_candidates: Vec::new(),
            last_scanned_block_id: None,
            filters,
            only_metadata,
            is_interrupted: false,
            io_config,
        })
    }

    async fn get_records_from_nearest_overlapped_blocks(
        &mut self,
        block_manager: &Arc<AsyncRwLock<BlockManager>>,
    ) -> Result<bool, ReductError> {
        self.block_candidates.clear();
        let block_candidate_ids = {
            let first_block = if let Some(last_id) = self.last_scanned_block_id {
                let bm = block_manager.read().await?;
                match bm
                    .index()
                    .active_tree()
                    .range((Excluded(last_id), Unbounded))
                    .next()
                    .copied()
                {
                    Some(id) if id < self.stop_time => id,
                    _ => return Err(ReductError::no_content("No content")),
                }
            } else {
                let mut bm = block_manager.write().await?;
                match bm.find_block_candidate(self.start_time).await {
                    Ok(block) => block.read().await?.block_id(),
                    Err(err) if err.status() == ErrorCode::TooEarly => {
                        debug!(
                            "Skip query start block lookup for '{}' due to transient replica state: {}",
                            self.entry_name, err
                        );
                        0
                    }
                    Err(_) => 0,
                }
            };

            let bm = block_manager.read().await?;
            let mut block_candidates = Vec::new();
            if first_block < self.stop_time {
                let active_tree = bm.index().active_tree();
                let last_timestamp_index = bm.index().last_record_index();

                let block_range: Vec<_> = active_tree
                    .range(first_block..=self.stop_time)
                    .cloned()
                    .collect();
                let mut group_end = 0;
                for block_id in block_range {
                    // store the first block candidate to start reading from
                    if block_candidates.is_empty() {
                        block_candidates.push(block_id);
                        group_end = *last_timestamp_index.get(&block_id).unwrap();
                        continue;
                    }

                    if group_end > block_id {
                        block_candidates.push(block_id);
                        group_end = group_end.max(*last_timestamp_index.get(&block_id).unwrap());
                    } else {
                        break;
                    }
                }
            }
            block_candidates
        };

        if block_candidate_ids.is_empty() {
            return Err(ReductError::no_content("No content"));
        }

        let mut block_candidates = Vec::new();
        for block_id in block_candidate_ids {
            let mut bm = block_manager.write().await?;
            let block_result = match bm.find_cached_block(block_id) {
                Some(block_ref) => Ok(block_ref),
                None => bm.load_block(block_id).await,
            };
            let block_ref = match block_result {
                Ok(block_ref) => block_ref,
                Err(err) if err.status() == ErrorCode::TooEarly => {
                    debug!(
                        "Skip transient replica block {} for query '{}': {}",
                        block_id, self.entry_name, err
                    );
                    return Err(ReductError::no_content("No content"));
                }
                Err(err) if err.status() == ErrorCode::NotFound => {
                    debug!(
                        "Skip stale block {} for query '{}': {}",
                        block_id, self.entry_name, err
                    );
                    continue;
                }
                Err(err) => return Err(err),
            };

            block_candidates.push(block_ref);
        }

        if let Some(block) = block_candidates.last() {
            self.last_scanned_block_id = Some(block.read().await?.block_id())
        }

        let found_records = self.filter_records_from_blocks(&block_candidates).await?;
        self.records_from_current_blocks.extend(found_records);
        self.block_candidates = block_candidates;

        Ok(!self.records_from_current_blocks.is_empty())
    }
}

#[async_trait]
impl Query for HistoricalQuery {
    async fn next(
        &mut self,
        block_manager: Arc<AsyncRwLock<BlockManager>>,
    ) -> Result<RecordReader, ReductError> {
        if self.records_from_current_blocks.is_empty() && !self.is_interrupted {
            while !self
                .get_records_from_nearest_overlapped_blocks(&block_manager)
                .await?
            {
                // Continue fetching records until we find some or determine there are none
            }
        }

        let Some(record) = self.records_from_current_blocks.pop_front() else {
            return Err(ReductError::no_content("No content"));
        };

        if self.only_metadata {
            Ok(RecordReader::form_record(&self.entry_name, record))
        } else {
            let block_id = record.block_id().ok_or_else(|| {
                reduct_base::internal_server_error!("Historical record has no block ID")
            })?;
            let mut block = None;
            for candidate in &self.block_candidates {
                if candidate.read().await?.block_id() == block_id {
                    block = Some(candidate.clone());
                    break;
                }
            }
            let block = match block {
                Some(block) => block,
                None => block_manager.write().await?.load_block(block_id).await?,
            };

            RecordReader::try_new(
                Arc::clone(&block_manager),
                block,
                record.timestamp(),
                Some(record),
                None,
            )
            .await
        }
    }

    fn io_settings(&self) -> &IoConfig {
        &self.io_config
    }
}

impl HistoricalQuery {
    async fn filter_records_from_blocks(
        &mut self,
        block_candidates: &Vec<BlockRef>,
    ) -> Result<Vec<RecordMeta>, ReductError> {
        let mut unfiltered_records = Vec::new();
        let mut filtered_records = Vec::new();
        for block in block_candidates.iter() {
            let blk = block.read().await?;
            for record in blk.record_index().values() {
                unfiltered_records.push(
                    RecordMeta::builder_from(record.clone().into())
                        .block_id(Some(blk.block_id()))
                        .build(),
                );
            }
        }

        // we need sorted data before applying filters
        unfiltered_records.sort_by_key(|record| record.timestamp());
        for record in unfiltered_records {
            match apply_filters_recursively(self.filters.as_mut_slice(), vec![record], 0)? {
                Some(records) => {
                    filtered_records.extend(records);
                }
                None => {
                    // If the filtering is interrupted, we stop processing further records.
                    self.is_interrupted = true;
                    break;
                }
            }

            if self.is_interrupted {
                break;
            }
        }

        Ok(filtered_records)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use std::collections::HashMap;

    use super::*;

    use crate::cfg::io::IoConfig;
    use crate::cfg::{Cfg, InstanceRole};
    use crate::core::file_cache::FILE_CACHE;
    use crate::storage::block_manager::block::Block;
    use crate::storage::block_manager::block_index::BlockIndex;
    use crate::storage::proto::record;
    use crate::storage::proto::{us_to_ts, Record};
    use crate::storage::query::base::tests::block_manager;
    use reduct_base::error::ErrorCode;
    use reduct_base::io::BoxedReadRecord;
    use reduct_base::io::ReadRecord;
    use reduct_base::{no_content, not_found};

    fn build_query(
        start_time: u64,
        stop_time: u64,
        options: QueryOptions,
    ) -> Result<HistoricalQuery, ReductError> {
        HistoricalQuery::try_new(
            "entry".to_string(),
            start_time,
            stop_time,
            options,
            IoConfig::default(),
        )
    }

    async fn add_blocks(block_manager: &Arc<AsyncRwLock<BlockManager>>, blocks: &[(u64, &[u64])]) {
        let mut bm = block_manager.write().await.unwrap();
        for &(block_id, timestamps) in blocks {
            let block_ref = bm.start_new_block(block_id, 10).await.unwrap();
            {
                let mut block = block_ref.write().await.unwrap();
                for &timestamp in timestamps {
                    block.insert_or_update_record(Record {
                        timestamp: Some(us_to_ts(&timestamp)),
                        begin: 0,
                        end: 0,
                        state: record::State::Finished as i32,
                        labels: vec![],
                        content_type: String::new(),
                    });
                }
            }
            bm.finish_block(block_ref).await.unwrap();
        }
    }

    async fn read_timestamps(
        query: &mut HistoricalQuery,
        block_manager: Arc<AsyncRwLock<BlockManager>>,
    ) -> Vec<u64> {
        read_to_vector(query, block_manager)
            .await
            .iter()
            .map(|(reader, _)| reader.meta().timestamp())
            .collect()
    }

    mod new {
        use super::*;
        use serde_json::json;

        #[rstest]
        fn override_only_metadata_if_ext_set() {
            let options = QueryOptions {
                only_metadata: true,
                ext: Some(json!({})),
                ..Default::default()
            };
            let query = build_query(0, 1000, options).unwrap();
            assert!(
                !query.only_metadata,
                "only_metadata should be false if ext is set to get content of records"
            );
        }

        #[rstest]
        fn when_directive_ext_overrides_only_metadata() {
            let options = QueryOptions {
                only_metadata: true,
                when: Some(json!({"$and": ["&flag"], "#ext": {}})),
                ..Default::default()
            };
            let query = build_query(0, 1000, options).unwrap();
            assert!(
                !query.only_metadata,
                "only_metadata should be false if #ext directive is set in when condition"
            );
        }
    }
    mod records {
        use super::*;

        #[test]
        fn test_set_labels() {
            let mut record: RecordMeta = Record {
                timestamp: Some(us_to_ts(&100)),
                begin: 0,
                end: 10,
                state: record::State::Finished as i32,
                labels: vec![],
                content_type: "text/plain".to_string(),
            }
            .into();

            let new_labels = HashMap::from([
                ("key1".to_string(), "value1".to_string()),
                ("key2".to_string(), "value2".to_string()),
            ]);

            FilterRecord::set_labels(&mut record, new_labels);

            assert_eq!(record.labels().len(), 2);
            assert_eq!(record.labels()[&"key1".to_string()], "value1");
            assert_eq!(record.labels()[&"key2".to_string()], "value2");
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn test_query_only_metadata_returns_entry_name(
            #[future] block_manager: Arc<AsyncRwLock<BlockManager>>,
        ) {
            let block_manager = block_manager.await;
            let mut query = build_query(
                0,
                5,
                QueryOptions {
                    only_metadata: true,
                    ..QueryOptions::default()
                },
            )
            .unwrap();
            let reader = query.next(block_manager.clone()).await.unwrap();

            assert_eq!(reader.meta().timestamp(), 0);
            assert_eq!(
                reader.meta().entry_name(),
                "entry",
                "entry_name should be returned for HEAD request (only_metadata=true)"
            );
        }

        #[rstest]
        #[case(5, &[0])]
        #[case(1000, &[0, 5])]
        #[case(1001, &[0, 5, 1000])]
        #[tokio::test(flavor = "multi_thread")]
        async fn reads_records_within_time_range(
            #[future] block_manager: Arc<AsyncRwLock<BlockManager>>,
            #[case] stop_time: u64,
            #[case] expected_timestamps: &[u64],
        ) {
            let block_manager = block_manager.await;
            let mut query = build_query(0, stop_time, QueryOptions::default()).unwrap();
            let records = read_to_vector(&mut query, block_manager).await;

            let timestamps: Vec<_> = records
                .iter()
                .map(|(reader, _)| reader.meta().timestamp())
                .collect();
            assert_eq!(timestamps, expected_timestamps);
            assert!(records.iter().all(|(_, content)| content == "0123456789"));
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn skips_filtered_block_to_return_record_from_next_block(
            #[future] block_manager: Arc<AsyncRwLock<BlockManager>>,
        ) {
            let block_manager = block_manager.await;
            let mut query = build_query(900, 1001, QueryOptions::default()).unwrap();

            let reader = query.next(block_manager.clone()).await.unwrap();
            assert_eq!(reader.meta().timestamp(), 1000);
            assert_eq!(
                query.next(block_manager).await.err(),
                Some(no_content!("No content"))
            );
        }
    }

    mod block_groups {
        use super::*;

        async fn query_timestamps(
            block_manager: Arc<AsyncRwLock<BlockManager>>,
            stop_time: u64,
            when: Option<serde_json::Value>,
        ) -> Vec<u64> {
            let mut query = build_query(
                1100,
                stop_time,
                QueryOptions {
                    only_metadata: true,
                    when,
                    ..Default::default()
                },
            )
            .unwrap();
            read_timestamps(&mut query, block_manager).await
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn queries_overlapping_and_non_overlapping_block_groups(
            #[future] block_manager: Arc<AsyncRwLock<BlockManager>>,
        ) {
            let block_manager = block_manager.await;
            add_blocks(
                &block_manager,
                &[
                    (1100, &[1100, 1180, 1230]),
                    (1150, &[1150, 1210]),
                    (1200, &[1200, 1250]),
                    (1400, &[1400]),
                    (1500, &[1500]),
                    (1700, &[1700, 1780]),
                    (1750, &[1750, 1790]),
                ],
            )
            .await;

            let timestamps = query_timestamps(block_manager, 1800, None).await;
            assert_eq!(
                timestamps,
                [1100, 1150, 1180, 1200, 1210, 1230, 1250, 1400, 1500, 1700, 1750, 1780, 1790,]
            );
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn queries_transitively_overlapping_blocks_in_timestamp_order(
            #[future] block_manager: Arc<AsyncRwLock<BlockManager>>,
        ) {
            let block_manager = block_manager.await;
            add_blocks(
                &block_manager,
                &[
                    (1100, &[1100, 1300]),
                    (1150, &[1150, 1160]),
                    (1250, &[1250, 1270]), // we have overlap with 3 blocks
                ],
            )
            .await;

            let timestamps = query_timestamps(block_manager, 1400, None).await;
            assert_eq!(timestamps, [1100, 1150, 1160, 1250, 1270, 1300]);
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn queries_overlap_extended_by_middle_block_in_timestamp_order(
            #[future] block_manager: Arc<AsyncRwLock<BlockManager>>,
        ) {
            let block_manager = block_manager.await;
            add_blocks(
                &block_manager,
                &[
                    (1100, &[1100, 1200]),
                    (1150, &[1150, 1300]),
                    (1250, &[1250, 1270]),
                ],
            )
            .await;

            let timestamps = query_timestamps(block_manager, 1400, None).await;
            assert_eq!(timestamps, [1100, 1150, 1200, 1250, 1270, 1300]);
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn applies_each_n_to_timestamp_order_across_overlapping_blocks(
            #[future] block_manager: Arc<AsyncRwLock<BlockManager>>,
        ) {
            let block_manager = block_manager.await;
            add_blocks(&block_manager, &[(1100, &[1100, 1300]), (1150, &[1150])]).await;

            let timestamps =
                query_timestamps(block_manager, 1400, Some(serde_json::json!({"$each_n": 2})))
                    .await;
            assert_eq!(timestamps, [1150]);
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn applies_limit_to_timestamp_order_across_overlapping_blocks(
            #[future] block_manager: Arc<AsyncRwLock<BlockManager>>,
        ) {
            let block_manager = block_manager.await;
            add_blocks(&block_manager, &[(1100, &[1100, 1300]), (1150, &[1150])]).await;

            let timestamps = query_timestamps(
                block_manager,
                1400,
                Some(serde_json::json!({"$limit": [2]})),
            )
            .await;
            assert_eq!(timestamps, [1100, 1150]);
        }
    }

    mod boundaries {
        use super::*;

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn does_not_scan_block_at_exclusive_stop_time(
            #[future] block_manager: Arc<AsyncRwLock<BlockManager>>,
        ) {
            let block_manager = block_manager.await;
            let mut query = build_query(1000, 1000, QueryOptions::default()).unwrap();

            assert_eq!(
                query.next(block_manager).await.err(),
                Some(no_content!("No content"))
            );
            assert_eq!(query.last_scanned_block_id, None);
        }
    }

    mod replica {
        use super::*;

        async fn replica_from_source(
            source: &Arc<AsyncRwLock<BlockManager>>,
            missing_descriptor: Option<u64>,
        ) -> Arc<AsyncRwLock<BlockManager>> {
            let (path, bucket, entry) = {
                let mut bm = source.write().await.unwrap();
                bm.save_cache_on_disk().await.unwrap();
                (
                    bm.path().clone(),
                    bm.bucket_name().to_string(),
                    bm.entry_name().to_string(),
                )
            };
            if let Some(block_id) = missing_descriptor {
                FILE_CACHE
                    .remove(&path.join(format!("{block_id}.meta")))
                    .await
                    .unwrap();
            }

            let index = BlockIndex::try_load(path.clone().join("index"))
                .await
                .unwrap();
            Arc::new(AsyncRwLock::new(
                BlockManager::build(
                    path,
                    index,
                    bucket,
                    entry,
                    Arc::new(Cfg {
                        role: InstanceRole::Replica,
                        ..Default::default()
                    }),
                    Default::default(),
                )
                .await
                .unwrap(),
            ))
        }

        async fn restore_descriptor(
            source: &Arc<AsyncRwLock<BlockManager>>,
            replica: &Arc<AsyncRwLock<BlockManager>>,
            block_id: u64,
        ) {
            let block_meta = {
                let mut bm = source.write().await.unwrap();
                let block = bm.load_block(block_id).await.unwrap();
                bm.finish_block(block).await.unwrap();
                bm.index().get_block(block_id).unwrap().clone()
            };
            replica
                .write()
                .await
                .unwrap()
                .index_mut()
                .insert_or_update_with_crc(block_meta.clone(), block_meta.crc64.unwrap());
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn test_query_skips_missing_block_descriptor_on_replica(
            #[future] block_manager: Arc<AsyncRwLock<BlockManager>>,
        ) {
            let source = block_manager.await;
            let replica = replica_from_source(&source, Some(1000)).await;

            let mut query = build_query(0, 1001, QueryOptions::default()).unwrap();
            let records = read_to_vector(&mut query, replica).await;

            assert_eq!(records.len(), 2);
            assert_eq!(records[0].0.meta().timestamp(), 0);
            assert_eq!(records[1].0.meta().timestamp(), 5);
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn retries_transient_replica_block_after_descriptor_becomes_available(
            #[future] block_manager: Arc<AsyncRwLock<BlockManager>>,
        ) {
            let source = block_manager.await;
            let replica = replica_from_source(&source, Some(1000)).await;

            let mut query = build_query(0, 1001, QueryOptions::default()).unwrap();
            assert_eq!(
                query
                    .next(replica.clone())
                    .await
                    .unwrap()
                    .meta()
                    .timestamp(),
                0
            );
            assert_eq!(
                query
                    .next(replica.clone())
                    .await
                    .unwrap()
                    .meta()
                    .timestamp(),
                5
            );
            assert_eq!(
                query.next(replica.clone()).await.err(),
                Some(no_content!("No content"))
            );

            restore_descriptor(&source, &replica, 1000).await;

            assert_eq!(query.next(replica).await.unwrap().meta().timestamp(), 1000);
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn waits_for_transient_overlapping_block_before_returning_group(
            #[future] block_manager: Arc<AsyncRwLock<BlockManager>>,
        ) {
            let source = block_manager.await;
            add_blocks(&source, &[(3, &[3])]).await;
            let replica = replica_from_source(&source, Some(3)).await;

            let mut query = build_query(
                0,
                1001,
                QueryOptions {
                    only_metadata: true,
                    ..Default::default()
                },
            )
            .unwrap();
            assert_eq!(
                query.next(replica.clone()).await.err(),
                Some(no_content!("No content"))
            );

            restore_descriptor(&source, &replica, 3).await;

            let timestamps = read_timestamps(&mut query, replica).await;
            assert_eq!(timestamps, [0, 3, 5, 1000]);
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn test_query_does_not_reload_index_on_crc_mismatch_replica(
            #[future] block_manager: Arc<AsyncRwLock<BlockManager>>,
        ) {
            let source = block_manager.await;
            let replica_block_manager = replica_from_source(&source, None).await;

            // Simulate stale in-memory CRC while index on disk is already updated.
            {
                let mut bm = replica_block_manager.write().await.unwrap();
                bm.index_mut()
                    .insert_or_update_with_crc(Block::new(1000), 1);
            }

            let mut query = build_query(1000, 1001, QueryOptions::default()).unwrap();
            let err = query.next(replica_block_manager).await.err().unwrap();

            assert_eq!(err.status(), ErrorCode::NoContent);
        }
    }

    mod filtering {
        use super::*;

        #[rstest]
        #[case::separate_block_groups(false)]
        #[case::overlapping_blocks(true)]
        #[tokio::test(flavor = "multi_thread")]
        async fn reads_context_before_content_across_block_boundaries(
            #[future] block_manager: Arc<AsyncRwLock<BlockManager>>,
            #[case] overlapping: bool,
        ) {
            let block_manager = block_manager.await;
            if overlapping {
                let mut bm = block_manager.write().await.unwrap();
                let block_ref = bm.load_block(0).await.unwrap();
                {
                    let mut block = block_ref.write().await.unwrap();
                    let mut record = block.get_record(0).unwrap().clone();
                    record.timestamp = Some(us_to_ts(&2000));
                    block.insert_or_update_record(record);
                }
                bm.save_block(block_ref).await.unwrap();
            }
            let mut query = build_query(
                0,
                1001,
                QueryOptions {
                    only_metadata: false,
                    when: Some(serde_json::json!({
                        "#ctx_before": "4s",
                        "&block": {"$eq": 2}
                    })),
                    ..QueryOptions::default()
                },
            )
            .unwrap();

            // Only timestamp 1000 matches; timestamps 0 and 5 belong to the previous block.
            for timestamp in [0, 5, 1000] {
                let mut reader = query
                    .next(block_manager.clone())
                    .await
                    .expect("context record must be read from its original block");
                assert_eq!(reader.meta().timestamp(), timestamp);
                assert_eq!(
                    reader.meta().block_id(),
                    Some(if timestamp == 1000 { 1000 } else { 0 })
                );

                let mut content = Vec::new();
                while let Some(chunk) = reader.read_chunk() {
                    content.extend_from_slice(&chunk.unwrap());
                }
                assert_eq!(content, b"0123456789");
            }

            assert_eq!(
                query.next(block_manager).await.err(),
                Some(no_content!("No content"))
            );
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn test_query_skips_corrupted_block(
            #[future] block_manager: Arc<AsyncRwLock<BlockManager>>,
        ) {
            let block_manager = block_manager.await;
            block_manager
                .write()
                .await
                .unwrap()
                .mark_block_corrupted(0)
                .await
                .unwrap();

            let mut query = build_query(0, 1001, QueryOptions::default()).unwrap();
            let records = read_to_vector(&mut query, block_manager).await;

            assert_eq!(records.len(), 1);
            assert_eq!(records[0].0.meta().timestamp(), 1000);
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn test_query_stops_when_current_block_latest_time_reaches_next_block(
            #[future] block_manager: Arc<AsyncRwLock<BlockManager>>,
        ) {
            let block_manager = block_manager.await;

            {
                let block_ref = block_manager
                    .write()
                    .await
                    .unwrap()
                    .load_block(0)
                    .await
                    .unwrap();
                block_ref
                    .write()
                    .await
                    .unwrap()
                    .insert_or_update_record(Record {
                        timestamp: Some(us_to_ts(&1000)),
                        begin: 0,
                        end: 10,
                        state: record::State::Finished as i32,
                        labels: vec![],
                        content_type: "".to_string(),
                    });
            }

            let mut query = build_query(0, 500, QueryOptions::default()).unwrap();

            assert_eq!(
                query
                    .next(block_manager.clone())
                    .await
                    .unwrap()
                    .meta()
                    .timestamp(),
                0
            );
            assert_eq!(
                query
                    .next(block_manager.clone())
                    .await
                    .unwrap()
                    .meta()
                    .timestamp(),
                5
            );
            assert_eq!(
                query.next(block_manager).await.err(),
                Some(no_content!("No content"))
            );
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn test_ignoring_errored_records(
            #[future] block_manager: Arc<AsyncRwLock<BlockManager>>,
        ) {
            let block_manager = block_manager.await;
            let mut query = build_query(0, 5, QueryOptions::default()).unwrap();
            {
                let block_ref = block_manager
                    .write()
                    .await
                    .unwrap()
                    .load_block(0)
                    .await
                    .unwrap();
                {
                    let mut block = block_ref.write().await.unwrap();
                    let mut record = block.get_record(0).unwrap().clone();
                    record.state = record::State::Errored as i32;
                    block.insert_or_update_record(record);
                }
                block_manager
                    .write()
                    .await
                    .unwrap()
                    .save_block(block_ref)
                    .await
                    .unwrap();
            }

            assert_eq!(
                query.next(block_manager.clone()).await.err(),
                Some(no_content!("No content"))
            );
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn test_each_n_records(#[future] block_manager: Arc<AsyncRwLock<BlockManager>>) {
            let block_manager = block_manager.await;
            let mut query = build_query(
                0,
                1001,
                QueryOptions {
                    when: Some(serde_json::json!({"$each_n": 2})),
                    ..QueryOptions::default()
                },
            )
            .unwrap();
            let records = read_to_vector(&mut query, block_manager).await;

            assert_eq!(records.len(), 1);
            assert_eq!(records[0].0.meta().timestamp(), 5);
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn test_when_filter(#[future] block_manager: Arc<AsyncRwLock<BlockManager>>) {
            let block_manager = block_manager.await;
            let mut query = build_query(
                0,
                1001,
                QueryOptions {
                    when: Some(serde_json::from_str(r#"{"$and": ["&flag"]}"#).unwrap()),
                    ..QueryOptions::default()
                },
            )
            .unwrap();
            let records = read_to_vector(&mut query, block_manager).await;

            assert_eq!(records.len(), 1);
            assert_eq!(records[0].0.meta().timestamp(), 0);
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn test_when_filter_strict(#[future] block_manager: Arc<AsyncRwLock<BlockManager>>) {
            let block_manager = block_manager.await;
            let mut query = build_query(
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
                query.next(block_manager.clone()).await.err(),
                Some(not_found!("Reference 'NOT_EXIST' not found"))
            );
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn test_when_with_interruption(
            #[future] block_manager: Arc<AsyncRwLock<BlockManager>>,
        ) {
            let block_manager = block_manager.await;
            let mut query = build_query(
                0,
                1001,
                QueryOptions {
                    when: Some(serde_json::from_str(r#"{"$limit": [1]}"#).unwrap()),
                    strict: true,
                    ..QueryOptions::default()
                },
            )
            .unwrap();

            let records = read_to_vector(&mut query, block_manager).await;

            assert_eq!(records.len(), 1);
            assert_eq!(records[0].0.meta().timestamp(), 0);
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn test_when_filter_non_strict(
            #[future] block_manager: Arc<AsyncRwLock<BlockManager>>,
        ) {
            let block_manager = block_manager.await;
            let mut query = build_query(
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
                query.next(block_manager.clone()).await.err(),
                Some(no_content!("No content")),
                "errored condition should be ignored in non-strict mode"
            );
        }
    }

    async fn read_to_vector(
        query: &mut HistoricalQuery,
        block_manager: Arc<AsyncRwLock<BlockManager>>,
    ) -> Vec<(BoxedReadRecord, String)> {
        let mut records: Vec<(BoxedReadRecord, String)> = Vec::new();
        loop {
            match query.next(block_manager.clone()).await {
                Ok(mut reader) => {
                    let mut content = String::new();
                    while let Some(chunk) = reader.read_chunk() {
                        content
                            .push_str(String::from_utf8(chunk.unwrap().to_vec()).unwrap().as_str())
                    }
                    records.push((Box::new(reader), content));
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
