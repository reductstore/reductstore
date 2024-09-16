// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

mod client_wrapper;
mod states;

use crate::replication::remote_bucket::states::{InitialState, RemoteBucketState};
use std::collections::BTreeMap;

use async_trait::async_trait;
use std::sync::{Arc, RwLock};

use crate::replication::Transaction;
use crate::storage::entry::RecordReader;
use reduct_base::error::ReductError;

struct RemoteBucketImpl {
    state: Option<Box<dyn RemoteBucketState + Send + Sync>>,
    is_active: bool,
}

pub(super) type ErrorRecordMap = BTreeMap<u64, ReductError>;

pub(crate) trait RemoteBucket {
    fn write_batch(
        &mut self,
        entry_name: &str,
        records: Vec<(RecordReader, Transaction)>,
    ) -> Result<ErrorRecordMap, ReductError>;

    fn is_active(&self) -> bool;
}

impl RemoteBucketImpl {
    pub fn new(url: &str, bucket_name: &str, api_token: &str) -> Self {
        Self {
            state: Some(Box::new(InitialState::new(url, bucket_name, api_token))),
            is_active: false,
        }
    }
}

impl RemoteBucket for RemoteBucketImpl {
    fn write_batch(
        &mut self,
        entry_name: &str,
        records: Vec<(RecordReader, Transaction)>,
    ) -> Result<ErrorRecordMap, ReductError> {
        self.state = Some(self.state.take().unwrap().write_batch(entry_name, records));

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

    use crate::storage::proto::Record;
    use async_trait::async_trait;
    use mockall::{mock, predicate};
    use prost_wkt_types::Timestamp;
    use reduct_base::error::ErrorCode;
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
            async fn write_batch(
                &self,
                entry: &str,
                records: Vec<RecordReader>,
            ) -> Result<ErrorRecordMap, ReductError>;

                        async fn update_batch(
                &self,
                entry: &str,
                records: &Vec<RecordReader>,
            ) -> Result<ErrorRecordMap, ReductError>;

            fn server_url(&self) -> &str;

            fn name(&self) -> &str;
        }
    }

    mock! {
        State{}

        #[async_trait]
        impl RemoteBucketState for State {
            async fn write_batch(
                self: Box<Self>,
                entry: &str,
                records: Vec<(RecordReader, Transaction)>,
            ) -> Box<dyn RemoteBucketState + Sync + Send>;

            fn last_result(&self) -> &Result<ErrorRecordMap, ReductError>;

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
        second_state
            .expect_last_result()
            .return_const(Ok(ErrorRecordMap::new()));
        second_state.expect_is_available().return_const(true);

        first_state
            .expect_write_batch()
            .with(predicate::eq("test"), predicate::always())
            .return_once(move |_, _| Box::new(second_state));

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
            .expect_write_batch()
            .return_once(move |_, _| Box::new(second_state));

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

    async fn write_record(
        remote_bucket: &mut RemoteBucketImpl,
    ) -> Result<ErrorRecordMap, ReductError> {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let mut rec = Record::default();
        rec.timestamp = Some(Timestamp::default());
        let record = RecordReader::form_record_with_rx(rx, rec, false);
        remote_bucket
            .write_batch("test", vec![(record, Transaction::WriteRecord(0))])
            .await
    }
}
