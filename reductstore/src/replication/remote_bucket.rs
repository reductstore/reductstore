// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

mod client_wrapper;
mod states;

use crate::replication::remote_bucket::states::{InitialState, RemoteBucketState};
use crate::replication::Transaction;
use async_trait::async_trait;
use reduct_base::error::ReductError;
use reduct_base::io::BoxedReadRecord;
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Clone, Debug, Default)]
pub(super) struct RemoteBucketConfig {
    pub(super) url: String,
    pub(super) bucket_name: String,
    pub(super) api_token: String,
    pub(super) verify_ssl: bool,
    pub(super) ca_path: Option<PathBuf>,
}

pub(super) struct RemoteBucketBuilder {
    config: RemoteBucketConfig,
}

impl RemoteBucketBuilder {
    pub fn new() -> Self {
        Self {
            config: RemoteBucketConfig {
                verify_ssl: true,
                ..Default::default()
            },
        }
    }

    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.config.url = url.into();
        self
    }

    pub fn bucket_name(mut self, bucket_name: impl Into<String>) -> Self {
        self.config.bucket_name = bucket_name.into();
        self
    }

    pub fn api_token(mut self, api_token: impl Into<String>) -> Self {
        self.config.api_token = api_token.into();
        self
    }

    pub fn verify_ssl(mut self, verify_ssl: bool) -> Self {
        self.config.verify_ssl = verify_ssl;
        self
    }

    pub fn ca_path(mut self, ca_path: Option<PathBuf>) -> Self {
        self.config.ca_path = ca_path;
        self
    }

    fn build_config(self) -> RemoteBucketConfig {
        self.config
    }

    pub fn build(self) -> Result<Box<dyn RemoteBucket + Send + Sync>, ReductError> {
        Ok(Box::new(RemoteBucketImpl::new(self.build_config())?))
    }
}

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
    fn new(config: RemoteBucketConfig) -> Result<Self, ReductError> {
        Ok(Self {
            state: Some(Box::new(InitialState::new(&config)?)),
            is_active: false,
        })
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

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::replication::remote_bucket::client_wrapper::{
        BoxedBucketApi, ReductBucketApi, ReductClientApi,
    };
    use crate::storage::proto::Record;
    use async_trait::async_trait;

    use crate::replication::remote_bucket::client_wrapper::tests::MockRecordReader;

    use mockall::{mock, predicate};
    use prost_wkt_types::Timestamp;
    use reduct_base::error::ErrorCode;
    use reduct_base::io::BoxedReadRecord;
    use rstest::{fixture, rstest};

    mock! {
        pub(super) ReductClientApi {}

        #[async_trait]
        impl ReductClientApi for ReductClientApi {
            async fn get_bucket(&self, bucket_name: &str) -> Result<BoxedBucketApi, ReductError>;

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
                records: Vec<BoxedReadRecord>,
            ) -> Result<ErrorRecordMap, ReductError>;

            async fn update_batch(
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

        #[async_trait]
        impl RemoteBucketState for State {
            async fn write_batch(
                self: Box<Self>,
                entry: &str,
                records: Vec<(BoxedReadRecord, Transaction)>,
            ) -> Box<dyn RemoteBucketState + Sync + Send>;

            async fn probe(self: Box<Self>) -> Box<dyn RemoteBucketState + Sync + Send>;

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

    #[rstest]
    #[tokio::test]
    async fn test_probe_availability() {
        let mut first_state = MockState::new();
        let mut second_state = MockState::new();
        second_state.expect_is_available().return_const(true);

        first_state
            .expect_probe()
            .return_once(move || Box::new(second_state));

        let mut remote_bucket = create_dst_bucket(first_state);
        remote_bucket.probe_availability().await;
        assert!(remote_bucket.is_active());
    }

    fn create_dst_bucket(first_state: MockState) -> RemoteBucketImpl {
        let mut remote_bucket = RemoteBucketImpl::new(
            RemoteBucketBuilder::new()
                .url("http://localhost:8080")
                .bucket_name("test")
                .api_token("api_token")
                .build_config(),
        )
        .unwrap();
        remote_bucket.state = Some(Box::new(first_state));
        remote_bucket
    }

    async fn write_record(
        remote_bucket: &mut RemoteBucketImpl,
    ) -> Result<ErrorRecordMap, ReductError> {
        let (_tx, rx) = crossbeam_channel::unbounded();
        let mut rec = Record::default();
        rec.timestamp = Some(Timestamp::default());
        let record = MockRecordReader::form_record_with_rx(rx, rec);
        remote_bucket
            .write_batch("test", vec![(record, Transaction::WriteRecord(0))])
            .await
    }
}
