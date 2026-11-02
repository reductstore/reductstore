// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

pub mod base;
pub(crate) mod condition;
mod continuous;
pub(crate) mod filters;
mod historical;

use crate::cfg::io::IoConfig;
use crate::core::sync::AsyncRwLock;
use crate::storage::block_manager::BlockManager;
use crate::storage::entry::RecordReader;
use crate::storage::query::base::{Query, QueryOptions};
use log::{debug, trace};
use reduct_base::error::ErrorCode::{NoContent, TooEarly};
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;
use std::cmp::{max, min};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;
use tokio::time::sleep;

pub(crate) type QueryRx = Receiver<Result<RecordReader, ReductError>>;
type QueryFactory =
    Arc<dyn Fn() -> Result<Box<dyn Query + Send + Sync>, ReductError> + Send + Sync>;

pub(in crate::storage) struct ReplicaQueryRecovery {
    pub(in crate::storage) rebuild: QueryFactory,
}

const QUERY_BUFFER_SIZE: usize = 64;
const MIN_FULL_CHANNEL_SLEEP: Duration = Duration::from_micros(10);
const MAX_FULL_CHANNEL_SLEEP: Duration = Duration::from_millis(10);

pub(crate) fn next_query_id() -> u64 {
    static QUERY_ID: AtomicU64 = AtomicU64::new(1); // start with 1 because 0 may confuse with false
    QUERY_ID.fetch_add(1, Ordering::SeqCst)
}

/// Build a query.
pub(in crate::storage) fn build_query(
    entry_name: String,
    start: u64,
    stop: u64,
    options: QueryOptions,
    io_defaults: IoConfig,
) -> Result<Box<dyn Query + Send + Sync>, ReductError> {
    if start > stop && !options.continuous {
        return Err(unprocessable_entity!("Start time must be before stop time",));
    }

    Ok(if options.continuous {
        Box::new(continuous::ContinuousQuery::try_new(
            entry_name,
            start,
            options,
            io_defaults,
        )?)
    } else {
        Box::new(historical::HistoricalQuery::try_new(
            entry_name,
            start,
            stop,
            options,
            io_defaults,
        )?)
    })
}

/// Spawn a query task.
///
/// # Arguments
///
/// * `id` - The id of the query, for logging purpose.
/// * `task_group` - The task group of the query.
/// * `query` - The query.
/// * `options` - The query options.
/// * `block_manager` - The block manager.
pub(super) fn spawn_query_task(
    id: u64,
    task_group: String,
    mut query: Box<dyn Query + Send + Sync>,
    options: QueryOptions,
    block_manager: Arc<AsyncRwLock<BlockManager>>,
    recovery: Option<ReplicaQueryRecovery>,
) -> (QueryRx, JoinHandle<()>) {
    let (tx, rx) = tokio::sync::mpsc::channel(QUERY_BUFFER_SIZE);

    // we spawn a new task to run the query outside hierarchical task group to avoid deadlocks
    let handle = tokio::spawn(async move {
        trace!("Query task for '{}' id={} running", task_group, id);
        let mut watcher = QueryWatcher::new();
        let mut emitted_record = false;
        let mut recovery_attempts = 0;

        loop {
            let group = task_group.clone();

            if tx.is_closed() {
                debug!("Query '{}' id={} task channel closed", group, id);
                break;
            }

            if watcher.expired(options.ttl) && !options.continuous {
                debug!("Query '{}' id={} task expired", group, id);
                break;
            }

            if tx.capacity() == 0 {
                trace!("Query '{}' id={} task channel full", group, id);
                // we increase the sleep time for the next iteration
                // to avoid flooding the channel but still be responsive
                let timeout = watcher.full_channel();
                sleep(timeout).await;
            }

            let next_result = query.next(block_manager.clone()).await;
            if next_result
                .as_ref()
                .is_err_and(|err| err.status == TooEarly)
                && !emitted_record
                && recovery_attempts < 3
                && recovery.is_some()
            {
                recovery_attempts += 1;
                let recovery = recovery.as_ref().unwrap();
                match BlockManager::reload_replica_index(&block_manager).await {
                    Ok(_)
                    | Err(ReductError {
                        status: TooEarly, ..
                    }) => {}
                    Err(err) => {
                        if tx.send(Err(err)).await.is_err() {
                            debug!("Query '{}' id={} task channel closed", group, id);
                        }
                        break;
                    }
                }

                match (recovery.rebuild)() {
                    Ok(rebuilt) => query = rebuilt,
                    Err(err) => {
                        if tx.send(Err(err)).await.is_err() {
                            debug!("Query '{}' id={} task channel closed", group, id);
                        }
                        break;
                    }
                }
                tokio::task::yield_now().await;
                continue;
            }
            let query_err = next_result.as_ref().err().cloned();
            let sent_record = next_result.is_ok();

            let send_result = tx.send(next_result).await;

            if let Err(err) = send_result {
                debug!("Error sending query '{}' id={} result: {}", group, id, err);
                break;
            }

            // notify the watcher that we sent a result
            watcher.send_success();
            emitted_record |= sent_record;

            if let Some(err) = query_err {
                if err.status == NoContent && options.continuous {
                    // continuous query will never be done
                    // but we don't want to flood the channel and wait for the receiver
                    sleep(options.ttl / 4).await;
                    continue;
                }

                trace!("Query task done for '{}' id={}", group, id);
                break;
            }
        }
    });
    (rx, handle)
}

/// A wrapper for timeout and expiry time logic
struct QueryWatcher {
    full_channel_sleep: Duration,
    last_send_time: Instant,
}

impl QueryWatcher {
    fn new() -> Self {
        Self {
            full_channel_sleep: MIN_FULL_CHANNEL_SLEEP,
            last_send_time: Instant::now(),
        }
    }

    /// Check if the query is expired.
    fn expired(&self, ttl: Duration) -> bool {
        self.last_send_time.elapsed() > ttl
    }

    /// Get the sleep time for the next iteration.
    fn full_channel(&mut self) -> Duration {
        let sleep = self.full_channel_sleep;
        self.full_channel_sleep += MIN_FULL_CHANNEL_SLEEP;
        self.full_channel_sleep = min(self.full_channel_sleep, MAX_FULL_CHANNEL_SLEEP);
        sleep
    }

    /// Notify that a result has been sent.
    fn send_success(&mut self) {
        if self.full_channel_sleep > MIN_FULL_CHANNEL_SLEEP {
            self.full_channel_sleep -= MIN_FULL_CHANNEL_SLEEP;
        }
        self.full_channel_sleep = max(self.full_channel_sleep, MIN_FULL_CHANNEL_SLEEP);
        self.last_send_time = Instant::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::cfg::Cfg;
    use crate::core::file_cache::build_test_file_cache;

    use crate::storage::block_manager::block_index::BlockIndex;
    use crate::storage::proto::Record;
    use prost_wkt_types::Timestamp;
    use reduct_base::io::ReadRecord;
    use rstest::*;
    use std::sync::atomic::AtomicUsize;
    use test_log::test as log_test;
    use tokio::time::timeout;

    #[log_test(rstest)]
    fn test_bad_start_stop() {
        let options = QueryOptions::default();
        assert_eq!(
            build_query(
                "entry".to_string(),
                10,
                5,
                options.clone(),
                IoConfig::default(),
            )
            .err()
            .unwrap(),
            ReductError::unprocessable_entity("Start time must be before stop time")
        );

        assert!(build_query(
            "entry".to_string(),
            10,
            10,
            options.clone(),
            IoConfig::default(),
        )
        .is_ok());
        assert!(build_query(
            "entry".to_string(),
            10,
            11,
            options.clone(),
            IoConfig::default(),
        )
        .is_ok());
    }

    #[rstest]
    fn test_ignore_stop_for_continuous() {
        let options = QueryOptions {
            continuous: true,
            ..Default::default()
        };
        assert!(build_query(
            "entry".to_string(),
            10,
            5,
            options.clone(),
            IoConfig::default(),
        )
        .is_ok());
    }

    #[log_test(rstest)]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_task_expired(#[future] block_manager: Arc<AsyncRwLock<BlockManager>>) {
        let block_manager = block_manager.await;
        let options = QueryOptions {
            ttl: Duration::from_millis(50),
            ..Default::default()
        };

        let query = build_query(
            "entry".to_string(),
            0,
            5,
            options.clone(),
            IoConfig::default(),
        )
        .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let (rx, handle) = spawn_query_task(
            0,
            "bucket/entry".to_string(),
            query,
            options,
            block_manager.clone(),
            None,
        );
        assert!(rx.is_empty());
        assert!(timeout(Duration::from_millis(1000), handle)
            .await
            .unwrap()
            .is_ok());
    }

    #[log_test(rstest)]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_task_ok(#[future] block_manager: Arc<AsyncRwLock<BlockManager>>) {
        let block_manager = block_manager.await;
        let options = QueryOptions::default();
        let query = build_query(
            "entry".to_string(),
            0,
            5,
            options.clone(),
            IoConfig::default(),
        )
        .unwrap();

        let (mut rx, handle) = spawn_query_task(
            0,
            "bucket/entry".to_string(),
            query,
            options,
            block_manager.clone(),
            None,
        );
        assert_eq!(rx.recv().await.unwrap().unwrap().meta().timestamp(), 0);
        assert_eq!(rx.recv().await.unwrap().unwrap().meta().timestamp(), 1);
        assert_eq!(rx.recv().await.unwrap().err().unwrap().status, NoContent);
        assert!(timeout(Duration::from_millis(1000), handle)
            .await
            .unwrap()
            .is_ok());
    }

    #[log_test(rstest)]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_task_continuous_ok(
        #[future] block_manager: Arc<AsyncRwLock<BlockManager>>,
    ) {
        let block_manager = block_manager.await;
        let options = QueryOptions {
            ttl: Duration::from_millis(50),
            continuous: true,
            ..Default::default()
        };
        let query = build_query(
            "entry".to_string(),
            0,
            5,
            options.clone(),
            IoConfig::default(),
        )
        .unwrap();
        let (mut rx, handle) = spawn_query_task(
            0,
            "bucket/entry".to_string(),
            query,
            options,
            block_manager.clone(),
            None,
        );
        assert_eq!(rx.recv().await.unwrap().unwrap().meta().timestamp(), 0);
        assert_eq!(rx.recv().await.unwrap().unwrap().meta().timestamp(), 1);
        assert_eq!(rx.recv().await.unwrap().err().unwrap().status, NoContent);

        block_manager
            .write()
            .await
            .unwrap()
            .load_block(0)
            .await
            .unwrap()
            .write()
            .await
            .unwrap()
            .insert_or_update_record(Record {
                timestamp: Some(Timestamp {
                    seconds: 0,
                    nanos: 2000,
                }),
                begin: 0,
                end: 10,
                state: 1,
                labels: vec![],
                content_type: "".to_string(),
            });

        assert_eq!(rx.recv().await.unwrap().unwrap().meta().timestamp(), 2);
        assert_eq!(rx.recv().await.unwrap().err().unwrap().status, NoContent);
        assert!(
            timeout(Duration::from_millis(1000), handle).await.is_err(),
            "never done"
        );
    }

    #[log_test(rstest)]
    #[tokio::test]
    async fn test_query_task_err(#[future] block_manager: Arc<AsyncRwLock<BlockManager>>) {
        let block_manager = block_manager.await;
        let options = QueryOptions::default();
        let query = build_query(
            "entry".to_string(),
            0,
            10,
            options.clone(),
            IoConfig::default(),
        )
        .unwrap();

        let (rx, handle) = spawn_query_task(
            0,
            "bucket/entry".to_string(),
            query,
            options,
            block_manager.clone(),
            None,
        );
        drop(rx); // drop the receiver to simulate a closed channel
        assert!(timeout(Duration::from_millis(1000), handle)
            .await
            .unwrap()
            .is_ok());
    }

    struct PanickingQuery {
        io: IoConfig,
    }

    struct TooEarlyQuery {
        io: IoConfig,
    }

    #[async_trait::async_trait]
    impl Query for TooEarlyQuery {
        async fn next(
            &mut self,
            _block_manager: Arc<AsyncRwLock<BlockManager>>,
        ) -> Result<RecordReader, ReductError> {
            Err(ReductError::too_early("replica is catching up"))
        }

        fn io_settings(&self) -> &IoConfig {
            &self.io
        }
    }

    struct OneRecordThenTooEarlyQuery {
        io: IoConfig,
        sent_record: bool,
    }

    #[async_trait::async_trait]
    impl Query for OneRecordThenTooEarlyQuery {
        async fn next(
            &mut self,
            _block_manager: Arc<AsyncRwLock<BlockManager>>,
        ) -> Result<RecordReader, ReductError> {
            if self.sent_record {
                return Err(ReductError::too_early("replica is catching up"));
            }
            self.sent_record = true;
            Ok(RecordReader::form_record(
                "entry",
                Record {
                    timestamp: Some(Timestamp {
                        seconds: 0,
                        nanos: 0,
                    }),
                    begin: 0,
                    end: 0,
                    state: 1,
                    labels: vec![],
                    content_type: String::new(),
                },
            ))
        }

        fn io_settings(&self) -> &IoConfig {
            &self.io
        }
    }

    #[rstest]
    #[tokio::test]
    async fn query_rebuilds_after_pre_output_too_early(
        #[future] block_manager: Arc<AsyncRwLock<BlockManager>>,
    ) {
        let block_manager = block_manager.await;
        let rebuilds = Arc::new(AtomicUsize::new(0));
        let recovery = ReplicaQueryRecovery {
            rebuild: Arc::new({
                let rebuilds = Arc::clone(&rebuilds);
                move || {
                    rebuilds.fetch_add(1, Ordering::SeqCst);
                    build_query(
                        "entry".to_string(),
                        0,
                        5,
                        QueryOptions::default(),
                        IoConfig::default(),
                    )
                }
            }),
        };

        let (mut rx, handle) = spawn_query_task(
            0,
            "bucket/entry".to_string(),
            Box::new(TooEarlyQuery {
                io: IoConfig::default(),
            }),
            QueryOptions::default(),
            Arc::clone(&block_manager),
            Some(recovery),
        );

        assert_eq!(rx.recv().await.unwrap().unwrap().meta().timestamp(), 0);
        assert_eq!(rx.recv().await.unwrap().unwrap().meta().timestamp(), 1);
        assert_eq!(rx.recv().await.unwrap().err().unwrap().status, NoContent);
        assert_eq!(rebuilds.load(Ordering::SeqCst), 1);
        handle.await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn query_propagates_too_early_without_replica_recovery(
        #[future] block_manager: Arc<AsyncRwLock<BlockManager>>,
    ) {
        let (mut rx, handle) = spawn_query_task(
            0,
            "bucket/entry".to_string(),
            Box::new(TooEarlyQuery {
                io: IoConfig::default(),
            }),
            QueryOptions::default(),
            block_manager.await,
            None,
        );

        assert_eq!(rx.recv().await.unwrap().err().unwrap().status, TooEarly);
        handle.await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn query_propagates_rebuild_error_after_replica_recovery(
        #[future] block_manager: Arc<AsyncRwLock<BlockManager>>,
    ) {
        let recovery = ReplicaQueryRecovery {
            rebuild: Arc::new(|| Err(ReductError::internal_server_error("rebuild failed"))),
        };
        let (mut rx, handle) = spawn_query_task(
            0,
            "bucket/entry".to_string(),
            Box::new(TooEarlyQuery {
                io: IoConfig::default(),
            }),
            QueryOptions::default(),
            block_manager.await,
            Some(recovery),
        );

        assert_eq!(
            rx.recv().await.unwrap().err().unwrap().status,
            reduct_base::error::ErrorCode::InternalServerError
        );
        handle.await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn query_rebuilds_after_busy_replica_reload(
        #[future] block_manager: Arc<AsyncRwLock<BlockManager>>,
    ) {
        let block_manager = block_manager.await;
        let _guard = block_manager.write().await.unwrap();
        let rebuilds = Arc::new(AtomicUsize::new(0));
        let recovery = ReplicaQueryRecovery {
            rebuild: Arc::new({
                let rebuilds = Arc::clone(&rebuilds);
                move || {
                    rebuilds.fetch_add(1, Ordering::SeqCst);
                    Err(ReductError::internal_server_error("rebuild failed"))
                }
            }),
        };
        let (mut rx, handle) = spawn_query_task(
            0,
            "bucket/entry".to_string(),
            Box::new(TooEarlyQuery {
                io: IoConfig::default(),
            }),
            QueryOptions::default(),
            Arc::clone(&block_manager),
            Some(recovery),
        );

        assert_eq!(
            rx.recv().await.unwrap().err().unwrap().status,
            reduct_base::error::ErrorCode::InternalServerError
        );
        assert_eq!(rebuilds.load(Ordering::SeqCst), 1);
        handle.await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn query_propagates_fatal_replica_reload_error() {
        let path = tempfile::tempdir()
            .unwrap()
            .keep()
            .join("bucket")
            .join("entry");
        let file_cache = build_test_file_cache();
        let index = BlockIndex::new(path.join("index"), Arc::clone(&file_cache));
        index.save().await.unwrap();
        let block_manager = Arc::new(AsyncRwLock::new(
            BlockManager::build(
                path.clone(),
                index,
                "bucket".to_string(),
                "entry".to_string(),
                Arc::new(Cfg {
                    role: crate::cfg::InstanceRole::Replica,
                    data_path: path.clone(),
                    ..Default::default()
                }),
                Default::default(),
                file_cache,
            )
            .await
            .unwrap(),
        ));
        std::fs::write(crate::storage::entry::publication::path(&path), [0xff]).unwrap();
        let rebuilds = Arc::new(AtomicUsize::new(0));
        let recovery = ReplicaQueryRecovery {
            rebuild: Arc::new({
                let rebuilds = Arc::clone(&rebuilds);
                move || {
                    rebuilds.fetch_add(1, Ordering::SeqCst);
                    Ok(Box::new(TooEarlyQuery {
                        io: IoConfig::default(),
                    }))
                }
            }),
        };
        let (mut rx, handle) = spawn_query_task(
            0,
            "bucket/entry".to_string(),
            Box::new(TooEarlyQuery {
                io: IoConfig::default(),
            }),
            QueryOptions::default(),
            block_manager,
            Some(recovery),
        );

        assert_eq!(
            rx.recv().await.unwrap().err().unwrap().status,
            reduct_base::error::ErrorCode::InternalServerError
        );
        assert_eq!(rebuilds.load(Ordering::SeqCst), 0);
        handle.await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn query_stops_after_three_pre_output_recoveries(
        #[future] block_manager: Arc<AsyncRwLock<BlockManager>>,
    ) {
        let block_manager = block_manager.await;
        let rebuilds = Arc::new(AtomicUsize::new(0));
        let recovery = ReplicaQueryRecovery {
            rebuild: Arc::new({
                let rebuilds = Arc::clone(&rebuilds);
                move || {
                    rebuilds.fetch_add(1, Ordering::SeqCst);
                    Ok(Box::new(TooEarlyQuery {
                        io: IoConfig::default(),
                    }))
                }
            }),
        };

        let (mut rx, handle) = spawn_query_task(
            0,
            "bucket/entry".to_string(),
            Box::new(TooEarlyQuery {
                io: IoConfig::default(),
            }),
            QueryOptions::default(),
            block_manager,
            Some(recovery),
        );

        assert_eq!(rx.recv().await.unwrap().err().unwrap().status, TooEarly);
        assert_eq!(rebuilds.load(Ordering::SeqCst), 3);
        handle.await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn query_does_not_rebuild_after_emitting_a_record(
        #[future] block_manager: Arc<AsyncRwLock<BlockManager>>,
    ) {
        let block_manager = block_manager.await;
        let rebuilds = Arc::new(AtomicUsize::new(0));
        let recovery = ReplicaQueryRecovery {
            rebuild: Arc::new({
                let rebuilds = Arc::clone(&rebuilds);
                move || {
                    rebuilds.fetch_add(1, Ordering::SeqCst);
                    Ok(Box::new(TooEarlyQuery {
                        io: IoConfig::default(),
                    }))
                }
            }),
        };

        let (mut rx, handle) = spawn_query_task(
            0,
            "bucket/entry".to_string(),
            Box::new(OneRecordThenTooEarlyQuery {
                io: IoConfig::default(),
                sent_record: false,
            }),
            QueryOptions::default(),
            block_manager,
            Some(recovery),
        );

        assert_eq!(rx.recv().await.unwrap().unwrap().meta().timestamp(), 0);
        assert_eq!(rx.recv().await.unwrap().err().unwrap().status, TooEarly);
        assert_eq!(rebuilds.load(Ordering::SeqCst), 0);
        handle.await.unwrap();
    }

    #[async_trait::async_trait]
    impl Query for PanickingQuery {
        async fn next(
            &mut self,
            _block_manager: Arc<AsyncRwLock<BlockManager>>,
        ) -> Result<RecordReader, ReductError> {
            panic!("force JoinError from spawn_blocking");
        }

        fn io_settings(&self) -> &IoConfig {
            &self.io
        }
    }

    #[log_test(rstest)]
    #[tokio::test]
    async fn test_query_task_blocking_error(
        #[future] block_manager: Arc<AsyncRwLock<BlockManager>>,
    ) {
        let block_manager = block_manager.await;
        let options = QueryOptions::default();
        let query: Box<dyn Query + Send + Sync> = Box::new(PanickingQuery {
            io: IoConfig::default(),
        });

        assert_eq!(
            query.io_settings().clone(),
            IoConfig::default(),
            "for code coverage"
        );

        let (mut rx, handle) = spawn_query_task(
            42,
            "bucket/entry".to_string(),
            query,
            options,
            block_manager,
            None,
        );

        // spawn_blocking panic should close the channel without hanging the task
        assert!(rx.recv().await.is_none());
        assert!(timeout(Duration::from_millis(1000), handle)
            .await
            .unwrap()
            .is_err());
    }

    #[fixture]
    async fn block_manager() -> Arc<AsyncRwLock<BlockManager>> {
        let path = tempfile::tempdir()
            .unwrap()
            .keep()
            .join("bucket")
            .join("entry");
        let file_cache = build_test_file_cache();
        let mut block_manager = BlockManager::build(
            path.clone(),
            BlockIndex::new(path.join("index"), Arc::clone(&file_cache)),
            "bucket".to_string(),
            "entry".to_string(),
            Cfg::default().into(),
            Default::default(),
            file_cache,
        )
        .await
        .unwrap();
        let block_ref = block_manager.start_new_block(0, 10).await.unwrap();
        block_ref
            .write()
            .await
            .unwrap()
            .insert_or_update_record(Record {
                timestamp: Some(Timestamp {
                    seconds: 0,
                    nanos: 0,
                }),
                begin: 0,
                end: 10,
                state: 1,
                labels: vec![],
                content_type: "".to_string(),
            });

        block_ref
            .write()
            .await
            .unwrap()
            .insert_or_update_record(Record {
                timestamp: Some(Timestamp {
                    seconds: 0,
                    nanos: 1000,
                }),
                begin: 0,
                end: 10,
                state: 1,
                labels: vec![],
                content_type: "".to_string(),
            });

        block_manager.finish_block(block_ref).await.unwrap();
        Arc::new(AsyncRwLock::new(block_manager))
    }

    mod task_watcher {
        use super::*;

        #[rstest]
        fn test_sleep_timeout() {
            let mut watcher = QueryWatcher::new();
            let mut sleep = watcher.full_channel();
            assert_eq!(sleep, MIN_FULL_CHANNEL_SLEEP);

            sleep = watcher.full_channel();
            assert_eq!(sleep, MIN_FULL_CHANNEL_SLEEP * 2);

            watcher.send_success();
            watcher.send_success();
            sleep = watcher.full_channel();
            assert_eq!(sleep, MIN_FULL_CHANNEL_SLEEP);
        }

        #[rstest]
        fn test_sleep_timeout_overflow() {
            let mut watcher = QueryWatcher {
                full_channel_sleep: MAX_FULL_CHANNEL_SLEEP,
                last_send_time: Instant::now(),
            };
            let mut sleep = watcher.full_channel();
            assert_eq!(sleep, MAX_FULL_CHANNEL_SLEEP);

            watcher.send_success();
            sleep = watcher.full_channel();
            assert_eq!(sleep, MAX_FULL_CHANNEL_SLEEP - MIN_FULL_CHANNEL_SLEEP);
        }

        #[rstest]
        fn test_sleep_timeout_underflow() {
            let mut watcher = QueryWatcher::new();

            watcher.send_success();
            let sleep = watcher.full_channel();
            assert_eq!(sleep, MIN_FULL_CHANNEL_SLEEP);
        }

        #[rstest]
        fn test_expired() {
            let watcher = QueryWatcher::new();
            assert!(!watcher.expired(Duration::from_millis(100)));
            std::thread::sleep(Duration::from_millis(100));
            assert!(watcher.expired(Duration::from_millis(100)));
        }
    }
}
