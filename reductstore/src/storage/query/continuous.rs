// Copyright 2023 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::block_manager::BlockManager;
use crate::storage::query::base::{Query, QueryOptions};
use crate::storage::query::historical::HistoricalQuery;
use reduct_base::error::{ErrorCode, ReductError};

use crate::storage::bucket::RecordReader;

use async_trait::async_trait;

use std::sync::Arc;

use tokio::sync::RwLock;

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
        block_manager: Arc<RwLock<BlockManager>>,
    ) -> Result<RecordReader, ReductError> {
        match self.query.next(block_manager).await {
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
                Err(ReductError {
                    status: ErrorCode::NoContent,
                    message: "No content".to_string(),
                })
            }
            Err(err) => Err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use reduct_base::error::ErrorCode;
    use rstest::rstest;
    use tokio::time::sleep;

    use crate::storage::query::base::tests::block_manager;

    #[rstest]
    #[tokio::test]
    async fn test_query(#[future] block_manager: Arc<RwLock<BlockManager>>) {
        let block_manager = block_manager.await;
        let mut query = ContinuousQuery::new(
            900,
            QueryOptions {
                ttl: std::time::Duration::from_millis(100),
                continuous: true,
                ..QueryOptions::default()
            },
        );
        {
            let reader = query.next(block_manager.clone()).await.unwrap();
            assert_eq!(reader.timestamp(), 1000);
        }
        assert_eq!(
            query.next(block_manager.clone()).await.err(),
            Some(ReductError {
                status: ErrorCode::NoContent,
                message: "No content".to_string(),
            })
        );
        assert_eq!(
            query.next(block_manager).await.err(),
            Some(ReductError {
                status: ErrorCode::NoContent,
                message: "No content".to_string(),
            })
        );
    }
}
