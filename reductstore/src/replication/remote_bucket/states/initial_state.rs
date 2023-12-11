// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::replication::remote_bucket::client_wrapper::{
    create_client, BoxedClientApi, ReductClientApi,
};
use crate::replication::remote_bucket::states::bucket_available::BucketAvailableState;
use crate::replication::remote_bucket::states::bucket_unavailable::BucketUnavailableState;
use crate::replication::remote_bucket::states::RemoteBucketState;
use crate::storage::bucket::RecordRx;
use async_trait::async_trait;
use log::error;
use reduct_base::Labels;
use url::Url;

/// Initial state of the remote bucket.
pub(in crate::replication::remote_bucket) struct InitialState {
    client: BoxedClientApi,
    bucket_name: String,
}

impl InitialState {
    pub fn new(url: &str, bucket: &str, api_token: &str) -> Self {
        let client = create_client(url, api_token);
        Self {
            client,
            bucket_name: bucket.to_string(),
        }
    }
}

#[async_trait]
impl RemoteBucketState for InitialState {
    async fn write_record(
        self: Box<Self>,
        entry: &str,
        timestamp: u64,
        labels: Labels,
        content_type: &str,
        content_length: u64,
        rx: RecordRx,
    ) -> Box<dyn RemoteBucketState + Sync + Send> {
        // Try to get the bucket.
        let bucket = self.client.get_bucket(&self.bucket_name).await;
        match bucket {
            Ok(bucket) => {
                // Bucket is available, transition to the available state and write the record.
                let new_state = Box::new(BucketAvailableState::new(self.client, bucket));
                new_state
                    .write_record(entry, timestamp, labels, content_type, content_length, rx)
                    .await
            }
            Err(err) => {
                // Bucket is unavailable, transition to the unavailable state.
                error!(
                    "Failed to get remote bucket {}/{}: {}",
                    self.client.url(),
                    self.bucket_name,
                    err
                );
                Box::new(BucketUnavailableState::new(self.client, self.bucket_name))
            }
        }
    }

    fn ok(&self) -> bool {
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
        assert_eq!(state.ok(), false);
    }

    #[rstest]
    #[tokio::test]
    async fn test_bucket_available(
        mut client: MockReductClientApi,
        mut bucket: MockReductBucketApi,
    ) {
        bucket
            .expect_write_record()
            .with(
                predicate::eq("test_entry"),
                predicate::eq(0),
                predicate::eq(Labels::new()),
                predicate::eq("text/plain"),
                predicate::eq(0),
                predicate::always(),
            )
            .return_once(|_, _, _, _, _, _| Ok(()));
        client
            .expect_get_bucket()
            .return_once(move |_| Ok(Box::new(bucket)));

        let state = Box::new(InitialState {
            client: Box::new(client),
            bucket_name: "test_bucket".to_string(),
        });

        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let state = state
            .write_record("test_entry", 0, Labels::new(), "text/plain", 0, rx)
            .await;

        assert_eq!(state.ok(), true);
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
        });

        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let state = state
            .write_record("test_entry", 0, Labels::new(), "text/plain", 0, rx)
            .await;

        assert_eq!(state.ok(), false);
    }
}
