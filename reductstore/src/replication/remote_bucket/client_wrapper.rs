// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::storage::bucket::RecordReader;
use async_stream::stream;
use async_trait::async_trait;

use reduct_base::error::{ErrorCode, IntEnum, ReductError};

use reqwest::header::{HeaderValue, CONTENT_LENGTH, CONTENT_TYPE};
use reqwest::{Body, Client, Method, Response};

use crate::storage::proto::ts_to_us;

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
    async fn write_batch(&self, entry: &str, records: Vec<RecordReader>)
        -> Result<(), ReductError>;

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

/*





       let client = Arc::clone(&self.client);

       let stream = stream! {
            while let Some(record) = self.records.pop_front() {
                let mut stream = record.stream_bytes();
                while let Some(bytes) = stream.next().await {
                    yield bytes;
                }
            }
       };

       let response = client
           .send_request(request.body(Body::wrap_stream(stream)))
           .await?;

       let mut failed_records = FailedRecordMap::new();
       response
           .headers()
           .iter()
           .filter(|(key, _)| key.as_str().starts_with("x-reduct-error"))
           .for_each(|(key, value)| {
               let record_ts = key
                   .as_str()
                   .trim_start_matches("x-reduct-error-")
                   .parse::<u64>()
                   .unwrap();
               let (status, message) = value.to_str().unwrap().split_once(',').unwrap();
               failed_records.insert(
                   record_ts,
                   ReductError::new(
                       ErrorCode::from_int(status.parse().unwrap()).unwrap(),
                       message,
                   ),
               );
           });

       Ok(failed_records)

*/
#[async_trait]
impl ReductBucketApi for BucketWrapper {
    async fn write_batch(
        &self,
        entry: &str,
        mut records: Vec<RecordReader>,
    ) -> Result<(), ReductError> {
        records.sort_by(|a, b| {
            let a = a.record();
            let b = b.record();
            ts_to_us(&a.timestamp.as_ref().unwrap()).cmp(&ts_to_us(&b.timestamp.as_ref().unwrap()))
        });
        let request = self.client.request(
            Method::POST,
            &format!("/b/{}/{}/batch", self.bucket_name, entry),
        );

        let content_length: u64 = records
            .iter()
            .map(|r| {
                let rec = r.record();
                rec.end - rec.begin
            })
            .sum();

        let mut request = request
            .header(
                CONTENT_TYPE,
                HeaderValue::from_str("application/octet-stream").unwrap(),
            )
            .header(
                CONTENT_LENGTH,
                HeaderValue::from_str(&content_length.to_string()).unwrap(),
            );

        for record in &records {
            let rec = record.record();
            let mut header_values = Vec::new();
            header_values.push(rec.content_type.clone());
            header_values.push((rec.end - rec.begin).to_string());
            if !rec.labels.is_empty() {
                for label in &rec.labels {
                    if label.value.contains(',') {
                        header_values.push(format!("{}=\"{}\"", label.name, label.value));
                    } else {
                        header_values.push(format!("{}={}", label.name, label.value));
                    }
                }
            }

            request = request.header(
                &format!(
                    "x-reduct-time-{}",
                    ts_to_us(&rec.timestamp.as_ref().unwrap())
                ),
                HeaderValue::from_str(&header_values.join(",").to_string()).unwrap(),
            );
        }

        let stream = stream! {
             while let Some(mut record) = records.pop() {
                let rx = record.rx();
                 while let Some(chunk) = rx.recv().await {
                     yield chunk;
                 }
             }
        };

        let response = request.body(Body::wrap_stream(stream)).send().await;

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
