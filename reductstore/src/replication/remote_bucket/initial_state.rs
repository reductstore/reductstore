// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::replication::remote_bucket::client_wrapper::{
    create_client, BoxedClientApi, ReductClientApi,
};
use crate::replication::remote_bucket::state::{
    BucketAvailableState, BucketUnavailableState, RemoteBucketState,
};
use crate::storage::bucket::RecordRx;
use async_trait::async_trait;
use log::error;
use reduct_base::Labels;
use reduct_rs::ReductClient;
use url::Url;

/// Initial state of the remote bucket.
pub(super) struct InitialState {
    client: BoxedClientApi,
    bucket_name: String,
}

impl InitialState {
    pub fn new(url: Url, bucket: &str, api_token: &str) -> Self {
        let client = create_client(url.as_str(), api_token);
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
        let bucket = self.client.get_bucket(&self.bucket_name).await;
        match bucket {
            Ok(bucket) => {
                let new_state = Box::new(BucketAvailableState::new(self.client, bucket));
                new_state
                    .write_record(entry, timestamp, labels, content_type, content_length, rx)
                    .await
            }
            Err(err) => {
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
    use rstest::rstest;

    #[rstest]
    fn test_initial_state() {
        let url = Url::parse("http://localhost:8080").unwrap();
        let bucket_name = "test_bucket";
        let api_token = "test_token";
        let state = InitialState::new(url, bucket_name, api_token);
        assert_eq!(state.ok(), false);
    }

    // TODO: Add more tests with mocks
}
