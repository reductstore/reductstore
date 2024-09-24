// Copyright 2023 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::replication::remote_bucket::client_wrapper::BoxedClientApi;
use crate::replication::remote_bucket::states::bucket_available::BucketAvailableState;
use crate::replication::remote_bucket::states::RemoteBucketState;

use log::error;

use crate::replication::remote_bucket::ErrorRecordMap;
use crate::replication::Transaction;
use crate::storage::entry::RecordReader;
use reduct_base::error::ReductError;
use tokio::time::{Duration, Instant};

pub(in crate::replication::remote_bucket) struct BucketUnavailableState {
    client: BoxedClientApi,
    bucket_name: String,
    init_time: Instant,
    timeout: Duration,
    last_result: Result<ErrorRecordMap, ReductError>,
}

impl RemoteBucketState for BucketUnavailableState {
    fn write_batch(
        self: Box<Self>,
        entry: &str,
        records: Vec<(RecordReader, Transaction)>,
    ) -> Box<dyn RemoteBucketState + Sync + Send> {
        if self.init_time.elapsed() > self.timeout {
            let bucket = self.client.get_bucket(&self.bucket_name);
            return match bucket {
                Ok(bucket) => {
                    let new_state = Box::new(BucketAvailableState::new(self.client, bucket));
                    new_state.write_batch(entry, records)
                }
                Err(err) => {
                    error!(
                        "Failed to get remote bucket {}{}: {}",
                        self.client.url(),
                        self.bucket_name,
                        err
                    );
                    Box::new(BucketUnavailableState::new(
                        self.client,
                        self.bucket_name,
                        err,
                    ))
                }
            };
        }

        let mut me = *self;
        me.last_result = Ok(ErrorRecordMap::new());
        Box::new(me)
    }

    fn is_available(&self) -> bool {
        false
    }

    fn last_result(&self) -> &Result<ErrorRecordMap, ReductError> {
        &self.last_result
    }
}

impl BucketUnavailableState {
    pub fn new(client: BoxedClientApi, bucket_name: String, error: ReductError) -> Self {
        Self {
            client,
            bucket_name,
            init_time: Instant::now(),
            timeout: Duration::new(5, 0),
            last_result: Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::remote_bucket::tests::{
        bucket, client, MockReductBucketApi, MockReductClientApi,
    };
    use mockall::predicate;
    use reduct_base::error::{ErrorCode, ReductError};
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_write_record_timeout(mut client: MockReductClientApi) {
        client.expect_get_bucket().times(0);

        let client = Box::new(client);
        let bucket_name = "bucket".to_string();
        let error = ReductError::new(ErrorCode::ConnectionError, "test");
        let state = Box::new(BucketUnavailableState::new(
            client,
            bucket_name,
            error.clone(),
        ));

        let state = state.write_batch("entry", vec![]);
        assert_eq!(state.last_result(), &Ok(ErrorRecordMap::new()));
        assert!(!state.is_available());
    }

    #[rstest]
    #[tokio::test]
    async fn test_bucket_err(mut client: MockReductClientApi) {
        client
            .expect_get_bucket()
            .with(predicate::eq("test_bucket"))
            .return_once(move |_| Err(ReductError::not_found("")));

        let state = state_without_timeout(client);
        let state = state.write_batch("test_entry", vec![]);
        assert_eq!(state.last_result(), &Err(ReductError::not_found("")));
        assert!(!state.is_available());
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_record_ok(
        mut client: MockReductClientApi,
        mut bucket: MockReductBucketApi,
    ) {
        bucket
            .expect_write_batch()
            .with(predicate::eq("test_entry"), predicate::always())
            .return_once(|_, _| Ok(ErrorRecordMap::new()));
        client
            .expect_get_bucket()
            .with(predicate::eq("test_bucket"))
            .return_once(move |_| Ok(Box::new(bucket)));

        let state = state_without_timeout(client);
        let state = state.write_batch("test_entry", vec![]);
        assert!(state.last_result().is_ok());
        assert!(state.is_available());
    }

    fn state_without_timeout(client: MockReductClientApi) -> Box<BucketUnavailableState> {
        Box::new(BucketUnavailableState {
            client: Box::new(client),
            bucket_name: "test_bucket".to_string(),
            init_time: Instant::now() - Duration::new(61, 0),
            timeout: Duration::new(0, 0),
            last_result: Ok(ErrorRecordMap::new()),
        })
    }
}
