// Copyright 2023 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::block_manager::BlockManager;
use crate::storage::query::base::{Query, QueryOptions};
use crate::storage::query::historical::HistoricalQuery;
use async_trait::async_trait;

use crate::storage::entry::RecordReader;
use reduct_base::error::{ErrorCode, ReductError};
use std::sync::Arc;
use tokio::sync::RwLock;

/// A query that is limited to a certain number of records.
pub(crate) struct LimitedQuery {
    query: HistoricalQuery,
    limit_count: u64,
}

impl LimitedQuery {
    pub fn new(start: u64, stop: u64, options: QueryOptions) -> LimitedQuery {
        LimitedQuery {
            query: HistoricalQuery::new(start, stop, options.clone()),
            limit_count: options.limit.unwrap(),
        }
    }
}

#[async_trait]
impl Query for LimitedQuery {
    async fn next(
        &mut self,
        block_manager: Arc<RwLock<BlockManager>>,
    ) -> Result<RecordReader, ReductError> {
        if self.limit_count == 0 {
            return Err(ReductError {
                status: ErrorCode::NoContent,
                message: "No content".to_string(),
            });
        }

        self.limit_count -= 1;
        let reader = self.query.next(block_manager).await;
        if self.limit_count == 0 {
            reader.map(|mut r| {
                r.set_last(true);
                r
            })
        } else {
            reader
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::query::base::tests::block_manager;
    use reduct_base::error::ErrorCode;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_limit(#[future] block_manager: Arc<RwLock<BlockManager>>) {
        let block_manager = block_manager.await;
        let mut query = LimitedQuery::new(
            0,
            u64::MAX,
            QueryOptions {
                limit: Some(1),
                ..Default::default()
            },
        );

        let reader = query.next(block_manager.clone()).await.unwrap();
        assert_eq!(reader.timestamp(), 0);
        assert!(reader.last());

        assert_eq!(
            query.next(block_manager).await.err(),
            Some(ReductError {
                status: ErrorCode::NoContent,
                message: "No content".to_string(),
            })
        );
    }
}
