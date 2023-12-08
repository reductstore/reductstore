// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

mod initial_state;
mod state;

use crate::replication::remote_bucket::initial_state::InitialState;
use crate::replication::remote_bucket::state::RemoteBucketState;
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
        mut record: RecordReader,
    ) -> Result<(), ReductError> {
        let state = self.state.take().unwrap();
        let labels = Labels::from_iter(
            record
                .labels()
                .iter()
                .map(|label| (label.name.clone(), label.value.clone())),
        );
        let mut rc = record.record().clone();
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
