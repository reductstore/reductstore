// Copyright 2023-2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

pub mod base;
pub(crate) mod condition;
mod continuous;
pub(crate) mod filters;
mod historical;
mod limited;

use crate::cfg::io::IoConfig;
use crate::core::sync::AsyncRwLock;
use crate::storage::block_manager::BlockManager;
use crate::storage::entry::RecordReader;
use crate::storage::query::base::{Query, QueryOptions};
use log::{debug, trace};
use reduct_base::error::ErrorCode::NoContent;
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

const QUERY_BUFFER_SIZE: usize = 64;
const MIN_FULL_CHANNEL_SLEEP: Duration = Duration::from_micros(10);
const MAX_FULL_CHANNEL_SLEEP: Duration = Duration::from_millis(10);

pub(crate) fn next_query_id() -> u64 {
    static QUERY_ID: AtomicU64 = AtomicU64::new(1); // start with 1 because 0 may confuse with false
    QUERY_ID.fetch_add(1, Ordering::SeqCst)
}

/// Build a query.
pub(in crate::storage) fn build_query(
    start: u64,
    stop: u64,
    options: QueryOptions,
    io_defaults: IoConfig,
) -> Result<Box<dyn Query + Send + Sync>, ReductError> {
    if start > stop && !options.continuous {
        return Err(unprocessable_entity!("Start time must be before stop time",));
    }

    Ok(if let Some(_) = options.limit {
        Box::new(limited::LimitedQuery::try_new(
            start,
            stop,
            options,
            io_defaults,
        )?)
    } else if options.continuous {
        Box::new(continuous::ContinuousQuery::try_new(
            start,
            options,
            io_defaults,
        )?)
    } else {
        Box::new(historical::HistoricalQuery::try_new(
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
) -> (QueryRx, JoinHandle<()>) {
    let (tx, rx) = tokio::sync::mpsc::channel(QUERY_BUFFER_SIZE);

    // we spawn a new task to run the query outside hierarchical task group to avoid deadlocks
    let handle = tokio::spawn(async move {
        trace!("Query task for '{}' id={} running", task_group, id);
        let mut watcher = QueryWatcher::new();

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
            let query_err = next_result.as_ref().err().cloned();

            let send_result = tx.send(next_result).await;

            if let Err(err) = send_result {
                debug!("Error sending query '{}' id={} result: {}", group, id, err);
                break;
            }

            // notify the watcher that we sent a result
            watcher.send_success();

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
    use crate::backend::Backend;
    use crate::cfg::Cfg;
    use crate::core::file_cache::FILE_CACHE;
    use crate::storage::block_manager::block_index::BlockIndex;
    use crate::storage::proto::Record;
    use prost_wkt_types::Timestamp;
    use reduct_base::io::ReadRecord;
    use rstest::*;
    use test_log::test as log_test;
    use tokio::time::timeout;

    #[log_test(rstest)]
    fn test_bad_start_stop() {
        let options = QueryOptions::default();
        assert_eq!(
            build_query(10, 5, options.clone(), IoConfig::default())
                .err()
                .unwrap(),
            ReductError::unprocessable_entity("Start time must be before stop time")
        );

        assert!(build_query(10, 10, options.clone(), IoConfig::default()).is_ok());
        assert!(build_query(10, 11, options.clone(), IoConfig::default()).is_ok());
    }

    #[rstest]
    fn test_ignore_stop_for_continuous() {
        let options = QueryOptions {
            continuous: true,
            ..Default::default()
        };
        assert!(build_query(10, 5, options.clone(), IoConfig::default()).is_ok());
    }

    #[log_test(rstest)]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_task_expired(block_manager: Arc<AsyncRwLock<BlockManager>>) {
        let options = QueryOptions {
            ttl: Duration::from_millis(50),
            ..Default::default()
        };

        let query = build_query(0, 5, options.clone(), IoConfig::default()).unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let (rx, handle) = spawn_query_task(
            0,
            "bucket/entry".to_string(),
            query,
            options,
            block_manager.clone(),
        );
        assert!(rx.is_empty());
        assert!(timeout(Duration::from_millis(1000), handle)
            .await
            .unwrap()
            .is_ok());
    }

    #[log_test(rstest)]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_task_ok(block_manager: Arc<AsyncRwLock<BlockManager>>) {
        let options = QueryOptions::default();
        let query = build_query(0, 5, options.clone(), IoConfig::default()).unwrap();

        let (mut rx, handle) = spawn_query_task(
            0,
            "bucket/entry".to_string(),
            query,
            options,
            block_manager.clone(),
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
    async fn test_query_task_continuous_ok(block_manager: Arc<AsyncRwLock<BlockManager>>) {
        let options = QueryOptions {
            ttl: Duration::from_millis(50),
            continuous: true,
            ..Default::default()
        };
        let query = build_query(0, 5, options.clone(), IoConfig::default()).unwrap();
        let (mut rx, handle) = spawn_query_task(
            0,
            "bucket/entry".to_string(),
            query,
            options,
            block_manager.clone(),
        );
        assert_eq!(rx.recv().await.unwrap().unwrap().meta().timestamp(), 0);
        assert_eq!(rx.recv().await.unwrap().unwrap().meta().timestamp(), 1);
        assert_eq!(rx.recv().await.unwrap().err().unwrap().status, NoContent);

        block_manager
            .write()
            .await
            .unwrap()
            .load_block(0)
            .unwrap()
            .write()
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
    async fn test_query_task_err(block_manager: Arc<AsyncRwLock<BlockManager>>) {
        let options = QueryOptions::default();
        let query = build_query(0, 10, options.clone(), IoConfig::default()).unwrap();

        let (rx, handle) = spawn_query_task(
            0,
            "bucket/entry".to_string(),
            query,
            options,
            block_manager.clone(),
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
    async fn test_query_task_blocking_error(block_manager: Arc<AsyncRwLock<BlockManager>>) {
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
        );

        // spawn_blocking panic should close the channel without hanging the task
        assert!(rx.recv().await.is_none());
        assert!(timeout(Duration::from_millis(1000), handle)
            .await
            .unwrap()
            .is_err());
    }

    #[fixture]
    fn block_manager() -> Arc<AsyncRwLock<BlockManager>> {
        let path = tempfile::tempdir()
            .unwrap()
            .keep()
            .join("bucket")
            .join("entry");

        FILE_CACHE.set_storage_backend(
            Backend::builder()
                .local_data_path(path.clone())
                .try_build()
                .unwrap(),
        );

        let mut block_manager = BlockManager::new(
            path.clone(),
            BlockIndex::new(path.join("index")),
            Cfg::default().into(),
        );
        let block_ref = block_manager.start_new_block(0, 10).unwrap();
        block_ref.write().unwrap().insert_or_update_record(Record {
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

        block_ref.write().unwrap().insert_or_update_record(Record {
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

        block_manager.finish_block(block_ref).unwrap();
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
