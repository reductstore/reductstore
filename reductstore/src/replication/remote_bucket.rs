// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

mod client_wrapper;
mod states;

use crate::replication::remote_bucket::states::{InitialState, RemoteBucketState};
use crate::storage::bucket::RecordReader;
use crate::storage::proto::ts_to_us;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;

use reduct_base::error::ReductError;
use reduct_base::Labels;

struct RemoteBucketImpl {
    state: Option<Box<dyn RemoteBucketState + Send + Sync>>,
    path: String,
    is_active: bool,
}

#[async_trait]
pub(crate) trait RemoteBucket {
    async fn write_record(
        &mut self,
        entry_name: &str,
        record: RecordReader,
    ) -> Result<(), ReductError>;

    fn is_active(&self) -> bool;
}

impl RemoteBucketImpl {
    pub fn new(url: &str, bucket_name: &str, api_token: &str) -> Self {
        Self {
            path: format!("{}/{}", url, bucket_name),
            state: Some(Box::new(InitialState::new(url, bucket_name, api_token))),
            is_active: false,
        }
    }
}

#[async_trait]
impl RemoteBucket for RemoteBucketImpl {
    async fn write_record(
        &mut self,
        entry_name: &str,
        record: RecordReader,
    ) -> Result<(), ReductError> {
        let labels = Labels::from_iter(
            record
                .labels()
                .iter()
                .map(|label| (label.name.clone(), label.value.clone())),
        );
        let rc = record.record().clone();
        self.state = Some(
            self.state
                .take()
                .unwrap()
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

        let state = self.state.as_ref().unwrap();
        self.is_active = state.is_available();
        state.last_result().clone()
    }

    fn is_active(&self) -> bool {
        self.is_active
    }
}

pub(super) fn create_remote_bucket(
    url: &str,
    bucket_name: &str,
    api_token: &str,
) -> Arc<RwLock<dyn RemoteBucket + Send + Sync>> {
    Arc::new(RwLock::new(RemoteBucketImpl::new(
        url,
        bucket_name,
        api_token,
    )))
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::replication::remote_bucket::client_wrapper::{
        BoxedBucketApi, ReductBucketApi, ReductClientApi,
    };
    use crate::storage::bucket::RecordRx;
    use crate::storage::proto::Record;
    use async_trait::async_trait;
    use mockall::{mock, predicate};
    use prost_wkt_types::Timestamp;
    use rstest::{fixture, rstest};

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

    mock! {
        State{}

        #[async_trait]
        impl RemoteBucketState for State {
            async fn write_record(
                self: Box<Self>,
                entry: &str,
                timestamp: u64,
                labels: Labels,
                content_type: &str,
                content_length: u64,
                rx: RecordRx,
            ) -> Box<dyn RemoteBucketState + Sync + Send>;

            fn last_result(&self) -> &Result<(), ReductError>;

            fn is_available(&self) -> bool;
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

    #[rstest]
    #[tokio::test]
    async fn test_write_record_ok() {
        let mut first_state = MockState::new();
        let mut second_state = MockState::new();
        second_state.expect_last_result().return_const(Ok(()));
        second_state.expect_is_available().return_const(true);

        first_state
            .expect_write_record()
            .with(
                predicate::eq("test"),
                predicate::eq(0),
                predicate::eq(Labels::default()),
                predicate::eq(""),
                predicate::eq(0),
                predicate::always(),
            )
            .return_once(move |_, _, _, _, _, _| Box::new(second_state));

        let mut remote_bucket = create_dst_bucket(first_state);
        write_record(&mut remote_bucket).await.unwrap();
        assert!(remote_bucket.is_active());
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_record_err() {
        let mut first_state = MockState::new();
        let mut second_state = MockState::new();
        second_state
            .expect_last_result()
            .return_const(Err(ReductError::new(ErrorCode::ConnectionError, "test")));
        second_state.expect_is_available().return_const(false);

        first_state
            .expect_write_record()
            .return_once(move |_, _, _, _, _, _| Box::new(second_state));

        let mut remote_bucket = create_dst_bucket(first_state);
        assert_eq!(
            write_record(&mut remote_bucket).await.unwrap_err(),
            ReductError::new(ErrorCode::ConnectionError, "test")
        );
        assert!(!remote_bucket.is_active());
    }

    fn create_dst_bucket(first_state: MockState) -> RemoteBucketImpl {
        let mut remote_bucket = RemoteBucketImpl::new("http://localhost:8080", "test", "api_token");
        remote_bucket.state = Some(Box::new(first_state));
        remote_bucket
    }

    async fn write_record(remote_bucket: &mut RemoteBucketImpl) -> Result<(), ReductError> {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let mut rec = Record::default();
        rec.timestamp = Some(Timestamp::default());
        let record = RecordReader::new(rx, rec, false);
        remote_bucket.write_record("test", record).await
    }
}
