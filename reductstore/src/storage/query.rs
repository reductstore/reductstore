// Copyright 2023 ReductSoftware UG
// Licensed under the Business Source License 1.1

pub mod base;
mod continuous;
pub mod filters;
mod historical;
mod limited;

use crate::storage::block_manager::BlockManager;
use crate::storage::bucket::RecordReader;
use crate::storage::query::base::{Query, QueryOptions, QueryState};
use crate::storage::storage::IO_OPERATION_TIMEOUT;
use log::warn;
use reduct_base::error::ErrorCode::NoContent;
use reduct_base::error::ReductError;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

const QUERY_BUFFER_SIZE: usize = 64;

/// Build a query.
pub(in crate::storage) fn build_query(
    start: u64,
    stop: u64,
    options: QueryOptions,
) -> Result<Box<dyn Query + Send + Sync>, ReductError> {
    if start > stop && !options.continuous {
        return Err(ReductError::unprocessable_entity(
            "Start time must be before stop time",
        ));
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
    mut query: Box<dyn Query + Send + Sync>,
    block_manager: Arc<RwLock<BlockManager>>,
) -> (Receiver<Result<RecordReader, ReductError>>, JoinHandle<()>) {
    let (tx, rx) = tokio::sync::mpsc::channel(QUERY_BUFFER_SIZE);

    let handle = tokio::spawn(async move {
        loop {
            if query.state() == &QueryState::Expired {
                break;
            }

            let next_result = query.next(block_manager.clone()).await;
            let query_err = next_result.as_ref().err().cloned();

            let send_result = timeout(IO_OPERATION_TIMEOUT, tx.send(next_result)).await;

            if let Err(err) = send_result {
                warn!("Error sending query result: {}", err);
                break;
            }

            if let Some(err) = query_err {
                if err.status == NoContent && query.state() != &QueryState::Done {
                    // continuous query will never be done
                    // but we don't want to flood the channel and wait for the receiver
                    while tx.capacity() < tx.max_capacity() && query.state() != &QueryState::Expired
                    {
                        sleep(Duration::from_millis(10)).await;
                    }
                } else {
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
    use crate::storage::block_manager::ManageBlock;
    use crate::storage::proto::Record;
    use prost_wkt_types::Timestamp;
    use rstest::*;

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
    async fn test_query_task_expired(#[future] block_manager: Arc<RwLock<BlockManager>>) {
        let options = QueryOptions {
            ttl: Duration::from_millis(50),
            ..Default::default()
        };

        let query = build_query(0, 5, options.clone()).unwrap();
        sleep(Duration::from_millis(100)).await;

        let (rx, handle) = spawn_query_task(query, block_manager.await.clone());
        assert!(rx.is_empty());
        assert!(handle.await.is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn test_query_task_ok(#[future] block_manager: Arc<RwLock<BlockManager>>) {
        let query = build_query(0, 5, QueryOptions::default()).unwrap();

        let (mut rx, handle) = spawn_query_task(query, block_manager.await.clone());
        assert_eq!(rx.recv().await.unwrap().unwrap().timestamp(), 0);
        assert_eq!(rx.recv().await.unwrap().unwrap().timestamp(), 1);
        assert_eq!(rx.recv().await.unwrap().err().unwrap().status, NoContent);
        assert!(timeout(Duration::from_millis(1000), handle).await.is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn test_query_task_continuous_ok(#[future] block_manager: Arc<RwLock<BlockManager>>) {
        let options = QueryOptions {
            ttl: Duration::from_millis(50),
            continuous: true,
            ..Default::default()
        };
        let query = build_query(0, 5, options).unwrap();
        let block_manager = block_manager.await;
        let (mut rx, handle) = spawn_query_task(query, block_manager.clone());
        assert_eq!(rx.recv().await.unwrap().unwrap().timestamp(), 0);
        assert_eq!(rx.recv().await.unwrap().unwrap().timestamp(), 1);
        assert_eq!(rx.recv().await.unwrap().err().unwrap().status, NoContent);

        block_manager
            .write()
            .await
            .load(0)
            .await
            .unwrap()
            .write()
            .await
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
        assert!(timeout(Duration::from_millis(1000), handle).await.is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn test_query_task_err(#[future] block_manager: Arc<RwLock<BlockManager>>) {
        let query = build_query(0, 10, QueryOptions::default()).unwrap();

        let (mut rx, handle) = spawn_query_task(query, block_manager.await.clone());
        drop(rx); // drop the receiver to simulate a closed channel
        assert!(timeout(Duration::from_millis(1000), handle).await.is_ok());
    }

    #[fixture]
    async fn block_manager() -> Arc<RwLock<BlockManager>> {
        let path = tempfile::tempdir().unwrap().into_path();

        let mut block_manager =
            BlockManager::new(path.clone(), BlockIndex::new(path.join("index")));
        let block_ref = block_manager.start(0, 10).await.unwrap();
        block_ref.write().await.insert_or_update_record(Record {
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

        block_ref.write().await.insert_or_update_record(Record {
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

        block_manager.finish(block_ref).await.unwrap();
        Arc::new(RwLock::new(block_manager))
    }
}
