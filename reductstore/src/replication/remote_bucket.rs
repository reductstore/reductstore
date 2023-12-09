// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

mod client_wrapper;
mod states;

use crate::replication::remote_bucket::states::{InitialState, RemoteBucketState};
use crate::storage::bucket::RecordReader;
use crate::storage::proto::ts_to_us;
use async_trait::async_trait;
use futures_util::TryStream;
use reduct_base::error::ReductError;
use reduct_base::Labels;
use url::Url;

pub(super) struct RemoteBucketImpl {
    state: Option<Box<dyn RemoteBucketState + Send + Sync>>,
    path: String,
}

#[async_trait]
pub(crate) trait RemoteBucket {
    async fn write_record<S>(
        &self,
        entry: String,
        timestamp: u64,
        labels: Labels,
        content_type: String,
        content_length: u64,
        data: S,
    ) -> Result<(), ReductError>
    where
        S: TryStream + Send + Sync + 'static,
        S::Error: Into<Box<dyn std::error::Error + Send + Sync>>;
}

impl RemoteBucketImpl {
    pub fn new(url: Url, bucket_name: &str, api_token: &str) -> Self {
        Self {
            path: format!("{}/{}", url, bucket_name),
            state: Some(Box::new(InitialState::new(url, bucket_name, api_token))),
        }
    }

    pub async fn write_record(
        &mut self,
        entry_name: &str,
        record: RecordReader,
    ) -> Result<(), ReductError> {
        let state = self.state.take().unwrap();
        let labels = Labels::from_iter(
            record
                .labels()
                .iter()
                .map(|label| (label.name.clone(), label.value.clone())),
        );
        let rc = record.record().clone();
        self.state = Some(
            state
                .write_record(
                    entry_name,
                    ts_to_us(&rc.timestamp.unwrap()),
                    labels,
                    rc.content_type.as_str(),
                    rc.end - rc.begin,
                    record.into_rx(),
                )
                .await,
        );
        if self.state.as_ref().unwrap().ok() {
            Ok(())
        } else {
            Err(ReductError::internal_server_error(&format!(
                "Remote bucket {} is not available",
                self.path
            )))
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::replication::remote_bucket::client_wrapper::{
        BoxedBucketApi, ReductBucketApi, ReductClientApi,
    };
    use crate::storage::bucket::RecordRx;
    use async_trait::async_trait;
    use mockall::mock;
    use rstest::fixture;

    mock! {
        pub(super) ReductClientApi {}

        #[async_trait]
        impl ReductClientApi for ReductClientApi {
             async fn get_bucket(
                &self,
                bucket_name: &str,
            ) -> Result<BoxedBucketApi, ReductError>;

            fn url(&self) -> &str;
        }
    }

    mock! {
        pub(super) ReductBucketApi {}

        #[async_trait]
        impl ReductBucketApi for ReductBucketApi {
            async fn write_record(
                &self,
                entry: &str,
                timestamp: u64,
                labels: Labels,
                content_type: &str,
                content_length: u64,
                rx: RecordRx,
            ) -> Result<(), ReductError>;

            fn server_url(&self) -> &str;

            fn name(&self) -> &str;
        }
    }

    #[fixture]
    pub(super) fn bucket() -> MockReductBucketApi {
        let mut bucket = MockReductBucketApi::new();
        bucket
            .expect_server_url()
            .return_const("http://localhost:8080".to_string());
        bucket.expect_name().return_const("test".to_string());
        bucket
    }

    #[fixture]
    pub(super) fn client() -> MockReductClientApi {
        let mut client = MockReductClientApi::new();
        client
            .expect_url()
            .return_const("http://localhost:8080".to_string());
        client
    }
}
