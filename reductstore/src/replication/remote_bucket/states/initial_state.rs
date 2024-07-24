// Copyright 2023 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::replication::remote_bucket::client_wrapper::{create_client, BoxedClientApi};
use crate::replication::remote_bucket::states::bucket_available::BucketAvailableState;
use crate::replication::remote_bucket::states::bucket_unavailable::BucketUnavailableState;
use crate::replication::remote_bucket::states::RemoteBucketState;
use crate::replication::remote_bucket::ErrorRecordMap;
use crate::replication::Transaction;
use crate::storage::bucket::RecordReader;
use async_trait::async_trait;
use log::error;
use reduct_base::error::ReductError;

/// Initial state of the remote bucket.
pub(in crate::replication::remote_bucket) struct InitialState {
    client: BoxedClientApi,
    bucket_name: String,
    last_result: Result<ErrorRecordMap, ReductError>,
}

impl InitialState {
    pub fn new(url: &str, bucket: &str, api_token: &str) -> Self {
        let client = create_client(url, api_token);
        Self {
            client,
            bucket_name: bucket.to_string(),
            last_result: Ok(ErrorRecordMap::new()),
        }
    }
}

#[async_trait]
impl RemoteBucketState for InitialState {
    async fn write_batch(
        self: Box<Self>,
        entry: &str,
        records: Vec<(RecordReader, Transaction)>,
    ) -> Box<dyn RemoteBucketState + Sync + Send> {
        // Try to get the bucket.
        let bucket = self.client.get_bucket(&self.bucket_name).await;
        match bucket {
            Ok(bucket) => {
                // Bucket is available, transition to the available state and write the record.
                let new_state = Box::new(BucketAvailableState::new(self.client, bucket));
                new_state.write_batch(entry, records).await
            }
            Err(err) => {
                // Bucket is unavailable, transition to the unavailable state.
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
        }
    }

    fn last_result(&self) -> &Result<ErrorRecordMap, ReductError> {
        &self.last_result
    }
    fn is_available(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::remote_bucket::tests::{
        bucket, client, MockReductBucketApi, MockReductClientApi,
    };
    use mockall::predicate;
    use reduct_base::error::ReductError;
    use rstest::rstest;

    #[rstest]
    fn test_initial_state() {
        let url = "http://localhost:8080";
        let bucket_name = "test_bucket";
        let api_token = "test_token";
        let state = InitialState::new(url, bucket_name, api_token);
        assert_eq!(state.last_result(), &Ok(ErrorRecordMap::new()));
    }

    #[rstest]
    #[tokio::test]
    async fn test_bucket_available(
        mut client: MockReductClientApi,
        mut bucket: MockReductBucketApi,
    ) {
        bucket
            .expect_write_batch()
            .with(
                predicate::eq("test_entry"),
                predicate::always(), // TODO: check the records
            )
            .return_once(|_, _| Ok(ErrorRecordMap::new()));
        client
            .expect_get_bucket()
            .return_once(move |_| Ok(Box::new(bucket)));

        let state = Box::new(InitialState {
            client: Box::new(client),
            bucket_name: "test_bucket".to_string(),
            last_result: Ok(ErrorRecordMap::new()),
        });

        let state = state.write_batch("test_entry", vec![]).await;

        assert_eq!(state.last_result(), &Ok(ErrorRecordMap::new()));
        assert_eq!(state.is_available(), true);
    }

    #[rstest]
    #[tokio::test]
    async fn test_bucket_unavailable(mut client: MockReductClientApi) {
        client
            .expect_get_bucket()
            .return_once(move |_| Err(ReductError::bad_request("test error")));

        let state = Box::new(InitialState {
            client: Box::new(client),
            bucket_name: "test_bucket".to_string(),
            last_result: Ok(ErrorRecordMap::new()),
        });

        let state = state.write_batch("test_entry", vec![]).await;

        assert_eq!(
            state.last_result(),
            &Err(ReductError::bad_request("test error"))
        );
        assert_eq!(state.is_available(), false);
    }
}
