// Copyright 2023 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::block_manager::BlockManager;
use crate::storage::query::base::{Query, QueryOptions};
use crate::storage::query::historical::HistoricalQuery;
use reduct_base::error::{ErrorCode, ReductError};

use crate::storage::entry::RecordReader;
use reduct_base::io::ReadRecord;
use std::sync::{Arc, RwLock};

pub struct ContinuousQuery {
    query: HistoricalQuery,
    next_start: u64,
    count: usize,
    options: QueryOptions,
}

impl ContinuousQuery {
    pub fn try_new(start: u64, options: QueryOptions) -> Result<Self, ReductError> {
        if !options.continuous {
            panic!("Continuous query must be continuous");
        }

        Ok(ContinuousQuery {
            query: HistoricalQuery::try_new(start, u64::MAX, options.clone())?,
            next_start: start,
            count: 0,
            options,
        })
    }
}
impl Query for ContinuousQuery {
    fn next(
        &mut self,
        block_manager: Arc<RwLock<BlockManager>>,
    ) -> Result<RecordReader, ReductError> {
        match self.query.next(block_manager) {
            Ok(reader) => {
                self.next_start = reader.timestamp() + 1;
                self.count += 1;
                Ok(reader)
            }
            Err(ReductError {
                status: ErrorCode::NoContent,
                ..
            }) => {
                self.query =
                    HistoricalQuery::try_new(self.next_start, u64::MAX, self.options.clone())?;
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

    use crate::storage::query::base::tests::block_manager;

    #[rstest]
    fn test_query(block_manager: Arc<RwLock<BlockManager>>) {
        let mut query = ContinuousQuery::try_new(
            900,
            QueryOptions {
                ttl: std::time::Duration::from_millis(100),
                continuous: true,
                ..QueryOptions::default()
            },
        )
        .unwrap();
        {
            let reader = query.next(block_manager.clone()).unwrap();
            assert_eq!(reader.timestamp(), 1000);
        }
        assert_eq!(
            query.next(block_manager.clone()).err(),
            Some(ReductError {
                status: ErrorCode::NoContent,
                message: "No content".to_string(),
            })
        );
        assert_eq!(
            query.next(block_manager).err(),
            Some(ReductError {
                status: ErrorCode::NoContent,
                message: "No content".to_string(),
            })
        );
    }
}
