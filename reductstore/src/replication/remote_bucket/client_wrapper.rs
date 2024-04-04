// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::storage::bucket::RecordRx;
use async_trait::async_trait;

use reduct_base::error::{ErrorCode, IntEnum, ReductError};
use reduct_base::Labels;
use reqwest::header::HeaderValue;
use reqwest::{Body, Client, Response};

use tokio_stream::wrappers::ReceiverStream;

// A wrapper around the Reduct client API to make it easier to mock.
#[async_trait]
pub(super) trait ReductClientApi {
    async fn get_bucket(&self, bucket_name: &str) -> Result<BoxedBucketApi, ReductError>;

    fn url(&self) -> &str;
}

pub(super) type BoxedClientApi = Box<dyn ReductClientApi + Sync + Send>;

// A wrapper around the Reduct bucket API to make it easier to mock.
#[async_trait]
pub(super) trait ReductBucketApi {
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

pub(super) type BoxedBucketApi = Box<dyn ReductBucketApi + Sync + Send>;

struct ReductClient {
    client: Client,
    server_url: String,
}

static API_PATH: &str = "api/v1";

impl ReductClient {
    fn new(url: &str, api_token: &str) -> Self {
        let mut headers = reqwest::header::HeaderMap::new();
        if !api_token.is_empty() {
            let mut value = HeaderValue::from_str(&format!("Bearer {}", api_token)).unwrap();
            value.set_sensitive(true);
            headers.insert(reqwest::header::AUTHORIZATION, value);
        }

        let client = Client::builder()
            .default_headers(headers)
            .timeout(std::time::Duration::from_secs(10))
            .danger_accept_invalid_certs(true)
            .http1_only()
            .build()
            .unwrap(); // TODO: Handle error

        Self {
            client,
            server_url: url.to_string(),
        }
    }
}

struct BucketWrapper {
    server_url: String,
    bucket_name: String,
    client: Client,
}

impl BucketWrapper {
    fn new(server_url: String, bucket_name: String, client: Client) -> Self {
        Self {
            server_url,
            bucket_name,
            client,
        }
    }
}

fn check_response(response: Result<Response, reqwest::Error>) -> Result<(), ReductError> {
    let map_error = |error: reqwest::Error| -> ReductError {
        let status = if error.is_connect() {
            ErrorCode::ConnectionError
        } else if error.is_timeout() {
            ErrorCode::Timeout
        } else {
            ErrorCode::Unknown
        };

        ReductError::new(status, &error.to_string())
    };

    let response = response.map_err(map_error)?;
    if response.status().is_success() {
        return Ok(());
    }

    let status =
        ErrorCode::from_int(response.status().as_u16() as i16).unwrap_or(ErrorCode::Unknown);

    let error_msg = response
        .headers()
        .get("x-reduct-error")
        .unwrap_or(&HeaderValue::from_str("Unknown").unwrap())
        .to_str()
        .unwrap()
        .to_string();

    Err(ReductError::new(status, &error_msg))
}

#[async_trait]
impl ReductClientApi for ReductClient {
    async fn get_bucket(&self, bucket_name: &str) -> Result<BoxedBucketApi, ReductError> {
        let resp = self
            .client
            .head(&format!(
                "{}{}/b/{}",
                self.server_url, API_PATH, bucket_name
            ))
            .send()
            .await;
        check_response(resp)?;

        Ok(Box::new(BucketWrapper::new(
            self.server_url.clone(),
            bucket_name.to_string(),
            self.client.clone(),
        )))
    }

    fn url(&self) -> &str {
        self.server_url.as_str()
    }
}

#[async_trait]
impl ReductBucketApi for BucketWrapper {
    async fn write_record(
        &self,
        entry: &str,
        timestamp: u64,
        labels: Labels,
        mut content_type: &str,
        content_length: u64,
        rx: RecordRx,
    ) -> Result<(), ReductError> {
        let stream = ReceiverStream::new(rx);

        let mut request = self
            .client
            .request(
                reqwest::Method::POST,
                &format!(
                    "{}{}/b/{}/{}",
                    self.server_url, API_PATH, self.bucket_name, entry
                ),
            )
            .query(&[("ts", timestamp.to_string())]);

        for (key, value) in labels {
            request = request.header(&format!("x-reduct-label-{}", key), value);
        }

        if content_type.is_empty() {
            content_type = "application/octet-stream";
        }

        let response = request
            .header(reqwest::header::CONTENT_TYPE, content_type)
            .header(reqwest::header::CONTENT_LENGTH, content_length.to_string())
            .body(Body::wrap_stream(stream))
            .send()
            .await;

        check_response(response)
    }

    fn server_url(&self) -> &str {
        &self.server_url
    }

    fn name(&self) -> &str {
        &self.bucket_name
    }
}

/// Create a new Reduct client wrapper.
pub(super) fn create_client(url: &str, api_token: &str) -> BoxedClientApi {
    Box::new(ReductClient::new(url, api_token))
}
