// Copyright 2023-2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

mod client_wrapper;
mod states;

use crate::replication::remote_bucket::states::{InitialState, RemoteBucketState};
use crate::replication::Transaction;
use async_trait::async_trait;
use reduct_base::error::ReductError;
use reduct_base::io::BoxedReadRecord;
use std::collections::BTreeMap;

struct RemoteBucketImpl {
    state: Option<Box<dyn RemoteBucketState + Send + Sync>>,
    is_active: bool,
}

pub(super) type ErrorRecordMap = BTreeMap<u64, ReductError>;

#[async_trait]
pub(crate) trait RemoteBucket {
    async fn write_batch(
        &mut self,
        entry_name: &str,
        records: Vec<(BoxedReadRecord, Transaction)>,
    ) -> Result<ErrorRecordMap, ReductError>;

    async fn probe_availability(&mut self);

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

#[async_trait]
impl RemoteBucket for RemoteBucketImpl {
    async fn write_batch(
        &mut self,
        entry_name: &str,
        records: Vec<(BoxedReadRecord, Transaction)>,
    ) -> Result<ErrorRecordMap, ReductError> {
        self.state = Some(
            self.state
                .take()
                .unwrap()
                .write_batch(entry_name, records)
                .await,
        );
        let state = self.state.as_ref().unwrap();
        self.is_active = state.is_available();
        state.last_result().clone()
    }

    async fn probe_availability(&mut self) {
        self.state = Some(self.state.take().unwrap().probe().await);
        self.is_active = self.state.as_ref().unwrap().is_available();
    }

    fn is_active(&self) -> bool {
        self.is_active
    }
}

pub(super) fn create_remote_bucket(
    url: &str,
    bucket_name: &str,
    api_token: Option<String>,
) -> Box<dyn RemoteBucket + Send + Sync> {
    Box::new(RemoteBucketImpl::new(
        url,
        bucket_name,
        api_token.as_deref().unwrap_or(""),
    ))
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::replication::remote_bucket::client_wrapper::{
        BoxedBucketApi, ReductBucketApi, ReductClientApi,
    };

    use crate::storage::proto::Record;

    use crate::replication::remote_bucket::client_wrapper::tests::MockRecordReader;

    use mockall::{mock, predicate};
    use prost_wkt_types::Timestamp;
    use reduct_base::error::ErrorCode;
    use reduct_base::io::BoxedReadRecord;
    use rstest::{fixture, rstest};

    mock! {
        pub(super) ReductClientApi {}

        impl ReductClientApi for ReductClientApi {
             fn get_bucket(
                &self,
                bucket_name: &str,
            ) -> Result<BoxedBucketApi, ReductError>;

            fn url(&self) -> &str;
        }
    }

    mock! {
        pub(super) ReductBucketApi {}

        impl ReductBucketApi for ReductBucketApi {
            fn write_batch(
                &self,
                entry: &str,
                records: Vec<BoxedReadRecord>,
            ) -> Result<ErrorRecordMap, ReductError>;

            fn update_batch(
                &self,
                entry: &str,
                records: &Vec<BoxedReadRecord>,
            ) -> Result<ErrorRecordMap, ReductError>;

            fn server_url(&self) -> &str;

            fn name(&self) -> &str;
        }
    }

    mock! {
        State{}

        impl RemoteBucketState for State {
            fn write_batch(
                self: Box<Self>,
                entry: &str,
                records: Vec<(BoxedReadRecord, Transaction)>,
            ) -> Box<dyn RemoteBucketState + Sync + Send>;

            fn probe(self: Box<Self>) -> Box<dyn RemoteBucketState + Sync + Send>;

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
    fn test_write_record_ok() {
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
        write_record(&mut remote_bucket).unwrap();
        assert!(remote_bucket.is_active());
    }

    #[rstest]
    fn test_write_record_err() {
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
            write_record(&mut remote_bucket).unwrap_err(),
            ReductError::new(ErrorCode::ConnectionError, "test")
        );
        assert!(!remote_bucket.is_active());
    }

    #[rstest]
    fn test_probe_availability() {
        let mut first_state = MockState::new();
        let mut second_state = MockState::new();
        second_state.expect_is_available().return_const(true);

        first_state
            .expect_probe()
            .return_once(move || Box::new(second_state));

        let mut remote_bucket = create_dst_bucket(first_state);
        remote_bucket.probe_availability();
        assert!(remote_bucket.is_active());
    }

    fn create_dst_bucket(first_state: MockState) -> RemoteBucketImpl {
        let mut remote_bucket = RemoteBucketImpl::new("http://localhost:8080", "test", "api_token");
        remote_bucket.state = Some(Box::new(first_state));
        remote_bucket
    }

    fn write_record(remote_bucket: &mut RemoteBucketImpl) -> Result<ErrorRecordMap, ReductError> {
        let (_tx, rx) = crossbeam_channel::unbounded();
        let mut rec = Record::default();
        rec.timestamp = Some(Timestamp::default());
        let record = MockRecordReader::form_record_with_rx(rx, rec);
        remote_bucket.write_batch("test", vec![(record, Transaction::WriteRecord(0))])
    }
}
