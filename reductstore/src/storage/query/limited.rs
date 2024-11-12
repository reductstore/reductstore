// Copyright 2023 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::block_manager::BlockManager;
use crate::storage::query::base::{Query, QueryOptions};
use crate::storage::query::historical::HistoricalQuery;

use crate::storage::entry::RecordReader;
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::no_content;
use std::sync::{Arc, RwLock};

/// A query that is limited to a certain number of records.
pub(crate) struct LimitedQuery {
    query: HistoricalQuery,
    limit_count: u64,
}

impl LimitedQuery {
    pub fn try_new(start: u64, stop: u64, options: QueryOptions) -> Result<Self, ReductError> {
        Ok(LimitedQuery {
            query: HistoricalQuery::try_new(start, stop, options.clone())?,
            limit_count: options.limit.unwrap(),
        })
    }
}

impl Query for LimitedQuery {
    fn next(
        &mut self,
        block_manager: Arc<RwLock<BlockManager>>,
    ) -> Result<RecordReader, ReductError> {
        if self.limit_count == 0 {
            return Err(no_content!("No content"));
        }

        self.limit_count -= 1;
        let reader = self.query.next(block_manager);
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
    fn test_limit(block_manager: Arc<RwLock<BlockManager>>) {
        let mut query = LimitedQuery::try_new(
            0,
            u64::MAX,
            QueryOptions {
                limit: Some(1),
                ..Default::default()
            },
        )
        .unwrap();

        let reader = query.next(block_manager.clone()).unwrap();
        assert_eq!(reader.timestamp(), 0);
        assert!(reader.last());

        assert_eq!(
            query.next(block_manager).err(),
            Some(ReductError {
                status: ErrorCode::NoContent,
                message: "No content".to_string(),
            })
        );
    }
}
