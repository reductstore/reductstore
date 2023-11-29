// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

mod state;

use crate::replication::remote_bucket::state::{InitialState, RemoteBucketState};
use async_trait::async_trait;
use futures_util::TryStream;
use reduct_base::error::ReductError;
use reduct_base::Labels;
use reduct_rs::ReductClient;
use serde::de::Unexpected::Option;
use std::rc::Rc;
use url::Url;

struct RemoteBucketImpl {
    state: Box<dyn RemoteBucketState>,
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
            state: Box::new(InitialState::new(url, bucket_name, api_token)),
        }
    }
}
