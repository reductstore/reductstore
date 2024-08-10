// Copyright 2023 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::block_manager::BlockManager;
use crate::storage::bucket::RecordReader;
use crate::storage::query::base::QueryState::Running;
use crate::storage::query::base::{Query, QueryOptions, QueryState};
use crate::storage::query::historical::HistoricalQuery;
use async_trait::async_trait;

use reduct_base::error::{ErrorCode, ReductError};
use std::sync::Arc;
use tokio::sync::RwLock;

/// A query that is limited to a certain number of records.
pub(crate) struct LimitedQuery {
    query: HistoricalQuery,
    options: QueryOptions,
}

impl LimitedQuery {
    pub fn new(start: u64, stop: u64, options: QueryOptions) -> LimitedQuery {
        LimitedQuery {
            query: HistoricalQuery::new(start, stop, options.clone()),
            options,
        }
    }
}

#[async_trait]
impl Query for LimitedQuery {
    async fn next(
        &mut self,
        block_manager: Arc<RwLock<BlockManager>>,
    ) -> Result<RecordReader, ReductError> {
        // TODO: It could be done better, maybe it make senses to move the limit into HistoricalQuery instead of manipulating the state here.
        if let Running(count) = self.state() {
            if *count == self.options.limit.unwrap() {
                self.query.state = QueryState::Done;
                return Err(ReductError {
                    status: ErrorCode::NoContent,
                    message: "No content".to_string(),
                });
            }
        }

        let reader = self.query.next(block_manager).await?;

        if let Running(count) = self.state() {
            if *count == self.options.limit.unwrap() {
                let record = reader.record().clone();
                return Ok(RecordReader::new(reader.into_rx(), record, true));
            }
        }

        Ok(reader)
    }

    fn state(&self) -> &QueryState {
        self.query.state()
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
    async fn test_limit(
        #[future] block_manager_and_index: (Arc<RwLock<BlockManager>>, BTreeSet<u64>),
    ) {
        let (block_manager, block_indexes) = block_manager_and_index.await;
        let mut query = LimitedQuery::new(
            0,
            u64::MAX,
            QueryOptions {
                limit: Some(1),
                ..Default::default()
            },
        );

        let reader = query
            .next(&block_indexes, block_manager.clone())
            .await
            .unwrap();
        assert_eq!(reader.timestamp(), 0);
        assert!(reader.last());

        assert_eq!(
            query.next(&block_indexes, block_manager).await.err(),
            Some(ReductError {
                status: ErrorCode::NoContent,
                message: "No content".to_string(),
            })
        );
    }
}
