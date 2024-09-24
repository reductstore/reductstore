// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

pub mod base;
mod continuous;
pub mod filters;
mod historical;
mod limited;

use crate::core::thread_pool::{shared_isolated, TaskHandle};
use crate::storage::block_manager::BlockManager;
use crate::storage::entry::RecordReader;
use crate::storage::query::base::{Query, QueryOptions};
use log::{trace, warn};
use reduct_base::error::ErrorCode::NoContent;
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;
use std::sync::{Arc, RwLock};
use std::thread::sleep;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;

pub(crate) type QueryRx = Receiver<Result<RecordReader, ReductError>>;

const QUERY_BUFFER_SIZE: usize = 64;

/// Build a query.
pub(in crate::storage) fn build_query(
    start: u64,
    stop: u64,
    options: QueryOptions,
) -> Result<Box<dyn Query + Send + Sync>, ReductError> {
    if start > stop && !options.continuous {
        return Err(unprocessable_entity!("Start time must be before stop time",));
    }

    Ok(if let Some(_) = options.limit {
        Box::new(limited::LimitedQuery::new(start, stop, options))
    } else if options.continuous {
        Box::new(continuous::ContinuousQuery::new(start, options))
    } else {
        Box::new(historical::HistoricalQuery::new(start, stop, options))
    })
}

pub(super) fn spawn_query_task(
    task_group: String,
    mut query: Box<dyn Query + Send + Sync>,
    options: QueryOptions,
    block_manager: Arc<RwLock<BlockManager>>,
) -> (QueryRx, TaskHandle<()>) {
    let (tx, rx) = tokio::sync::mpsc::channel(QUERY_BUFFER_SIZE);

    let handle = shared_isolated(&task_group.clone(), "spawn query task", move || {
        loop {
            let next_result = query.next(block_manager.clone());
            let query_err = next_result.as_ref().err().cloned();

            if tx.is_closed() {
                trace!("Query '{}' task channel closed", task_group);
                break;
            }

            let send_result = tx.blocking_send(next_result);

            if let Err(err) = send_result {
                warn!("Error sending query '{}' result: {}", task_group, err);
                break;
            }

            if let Some(err) = query_err {
                if err.status == NoContent && options.continuous {
                    // continuous query will never be done
                    // but we don't want to flood the channel and wait for the receiver
                    while tx.capacity() < tx.max_capacity() {
                        sleep(Duration::from_millis(10));
                    }
                } else {
                    trace!("Query task done for '{}'", task_group);
                    break;
                }
            }
        }
    });
    (rx, handle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::block_manager::block_index::BlockIndex;
    use crate::storage::proto::Record;
    use prost_wkt_types::Timestamp;
    use rstest::*;
    use tokio::time::timeout;

    #[rstest]
    fn test_bad_start_stop() {
        let options = QueryOptions::default();
        assert_eq!(
            build_query(10, 5, options.clone()).err().unwrap(),
            ReductError::unprocessable_entity("Start time must be before stop time")
        );

        assert!(build_query(10, 10, options.clone()).is_ok());
        assert!(build_query(10, 11, options.clone()).is_ok());
    }

    #[rstest]
    fn test_ignore_stop_for_continuous() {
        let options = QueryOptions {
            continuous: true,
            ..Default::default()
        };
        assert!(build_query(10, 5, options.clone()).is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn test_query_task_expired(block_manager: Arc<RwLock<BlockManager>>) {
        let options = QueryOptions {
            ttl: Duration::from_millis(50),
            ..Default::default()
        };

        let query = build_query(0, 5, options.clone()).unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let (rx, handle) = spawn_query_task(
            "bucket/entry".to_string(),
            query,
            options,
            block_manager.clone(),
        );
        assert!(rx.is_empty());
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(handle.is_finished());
    }

    #[rstest]
    #[tokio::test]
    async fn test_query_task_ok(block_manager: Arc<RwLock<BlockManager>>) {
        let options = QueryOptions::default();
        let query = build_query(0, 5, options.clone()).unwrap();

        let (mut rx, handle) = spawn_query_task(
            "bucket/entry".to_string(),
            query,
            options,
            block_manager.clone(),
        );
        assert_eq!(rx.recv().await.unwrap().unwrap().timestamp(), 0);
        assert_eq!(rx.recv().await.unwrap().unwrap().timestamp(), 1);
        assert_eq!(rx.recv().await.unwrap().err().unwrap().status, NoContent);
        assert!(timeout(Duration::from_millis(1000), handle).await.is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn test_query_task_continuous_ok(block_manager: Arc<RwLock<BlockManager>>) {
        let options = QueryOptions {
            ttl: Duration::from_millis(50),
            continuous: true,
            ..Default::default()
        };
        let query = build_query(0, 5, options.clone()).unwrap();
        let (mut rx, handle) = spawn_query_task(
            "bucket/entry".to_string(),
            query,
            options,
            block_manager.clone(),
        );
        assert_eq!(rx.recv().await.unwrap().unwrap().timestamp(), 0);
        assert_eq!(rx.recv().await.unwrap().unwrap().timestamp(), 1);
        assert_eq!(rx.recv().await.unwrap().err().unwrap().status, NoContent);

        block_manager
            .write()
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

        assert_eq!(rx.recv().await.unwrap().unwrap().timestamp(), 2);
        assert_eq!(rx.recv().await.unwrap().err().unwrap().status, NoContent);
        assert!(
            timeout(Duration::from_millis(1000), handle).await.is_err(),
            "never done"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_query_task_err(block_manager: Arc<RwLock<BlockManager>>) {
        let options = QueryOptions::default();
        let query = build_query(0, 10, options.clone()).unwrap();

        let (rx, handle) = spawn_query_task(
            "bucket/entry".to_string(),
            query,
            options,
            block_manager.clone(),
        );
        drop(rx); // drop the receiver to simulate a closed channel
        assert!(timeout(Duration::from_millis(1000), handle).await.is_ok());
    }

    #[fixture]
    fn block_manager() -> Arc<RwLock<BlockManager>> {
        let path = tempfile::tempdir()
            .unwrap()
            .into_path()
            .join("bucket")
            .join("entry");

        let mut block_manager =
            BlockManager::new(path.clone(), BlockIndex::new(path.join("index")));
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
        Arc::new(RwLock::new(block_manager))
    }
}
