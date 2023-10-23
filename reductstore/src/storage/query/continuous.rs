// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::storage::block_manager::BlockManager;
use crate::storage::query::base::{Query, QueryOptions, QueryState};
use crate::storage::query::historical::HistoricalQuery;
use reduct_base::error::{ErrorCode, ReductError};

use std::collections::BTreeSet;

use crate::storage::bucket::RecordReader;
use crate::storage::proto::Record;
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc::Sender;

pub struct ContinuousQuery {
    query: HistoricalQuery,
    next_start: u64,
    count: usize,
    options: QueryOptions,
}

impl ContinuousQuery {
    pub fn new(start: u64, options: QueryOptions) -> ContinuousQuery {
        if !options.continuous {
            panic!("Continuous query must be continuous");
        }

        ContinuousQuery {
            query: HistoricalQuery::new(start, u64::MAX, options.clone()),
            next_start: start,
            count: 0,
            options,
        }
    }
}

#[async_trait]
impl Query for ContinuousQuery {
    async fn next(
        &mut self,
        block_indexes: &BTreeSet<u64>,
        block_manager: &mut BlockManager,
    ) -> Result<RecordReader, ReductError> {
        match self.query.next(block_indexes, block_manager).await {
            Ok(reader) => {
                self.next_start = reader.timestamp() + 1;
                self.count += 1;
                Ok(reader)
            }
            Err(ReductError {
                status: ErrorCode::NoContent,
                ..
            }) => {
                self.query = HistoricalQuery::new(self.next_start, u64::MAX, self.options.clone());
                self.query.state = QueryState::Running(self.count);
                Err(ReductError {
                    status: ErrorCode::NoContent,
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

    use rstest::rstest;
    use std::thread::sleep;

    use reduct_base::error::ErrorCode;

    use crate::storage::query::base::tests::block_manager_and_index;

    #[rstest]
    #[tokio::test]
    async fn test_query(#[future] block_manager_and_index: (BlockManager, BTreeSet<u64>)) {
        let (mut block_manager, block_indexes) = block_manager_and_index.await;

        let mut query = ContinuousQuery::new(
            900,
            QueryOptions {
                ttl: std::time::Duration::from_millis(100),
                continuous: true,
                ..QueryOptions::default()
            },
        );
        {
            let reader = query
                .next(&block_indexes, &mut block_manager)
                .await
                .unwrap();
            assert_eq!(reader.timestamp(), 1000);
        }
        assert_eq!(
            query.next(&block_indexes, &mut block_manager).await.err(),
            Some(ReductError {
                status: ErrorCode::NoContent,
                message: "No content".to_string(),
            })
        );
        assert_eq!(
            query.next(&block_indexes, &mut block_manager).await.err(),
            Some(ReductError {
                status: ErrorCode::NoContent,
                message: "No content".to_string(),
            })
        );
        assert_eq!(query.state(), &QueryState::Running(1));

        sleep(std::time::Duration::from_millis(200));
        assert_eq!(query.state(), &QueryState::Expired);
    }
}
