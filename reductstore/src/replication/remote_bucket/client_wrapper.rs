// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::bucket::RecordReader;

use async_stream::stream;
use async_trait::async_trait;
use axum::http::HeaderName;
use bytes::Bytes;
use futures_util::Stream;
use std::collections::BTreeMap;

use std::str::FromStr;

use reduct_base::error::{ErrorCode, IntEnum, ReductError};

use crate::replication::remote_bucket::ErrorRecordMap;
use crate::replication::Transaction;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_LENGTH, CONTENT_TYPE};
use reqwest::{Body, Client, Error, Method, Response};

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
    async fn write_batch(
        &self,
        entry: &str,
        records: Vec<(RecordReader, Transaction)>,
    ) -> Result<ErrorRecordMap, ReductError>;

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

    fn build_headers(records: &mut Vec<(RecordReader, Transaction)>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        let content_length: u64 = records.iter().map(|r| r.0.content_length()).sum();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_str("application/octet-stream").unwrap(),
        );
        headers.insert(
            CONTENT_LENGTH,
            HeaderValue::from_str(&content_length.to_string()).unwrap(),
        );

        for (record, _) in records {
            let mut header_values = Vec::new();
            header_values.push(record.content_length().to_string());
            header_values.push(record.content_type().to_string());
            if !record.labels().is_empty() {
                for label in record.labels() {
                    if label.value.contains(',') {
                        header_values.push(format!("{}=\"{}\"", label.name, label.value));
                    } else {
                        header_values.push(format!("{}={}", label.name, label.value));
                    }
                }
            }

            headers.insert(
                HeaderName::from_str(&format!("x-reduct-time-{}", record.timestamp())).unwrap(),
                HeaderValue::from_str(&header_values.join(",").to_string()).unwrap(),
            );
        }

        headers
    }

    fn prepare_batch(
        mut records: Vec<(RecordReader, Transaction)>,
    ) -> (HeaderMap, impl Stream<Item = Result<Bytes, ReductError>>) {
        records.sort_by(|a, b| {
            let a = a.0.record();
            let b = b.0.record();
            ts_to_us(&b.timestamp.as_ref().unwrap()).cmp(&ts_to_us(&a.timestamp.as_ref().unwrap()))
        });

        let headers = Self::build_headers(&mut records);

        let stream = stream! {
             while let Some((mut record, _)) = records.pop() {
                let rx = record.rx();
                 while let Some(chunk) = rx.recv().await {
                     yield chunk;
                 }
             }
        };
        (headers, stream)
    }

    fn parse_record_errors(
        response: Result<Response, Error>,
    ) -> Result<BTreeMap<u64, ReductError>, ReductError> {
        let mut failed_records = BTreeMap::new();
        if response.is_err() {
            check_response(response)?;
        } else {
            response
                .ok()
                .unwrap()
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
        }
        Ok(failed_records)
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
    async fn write_batch(
        &self,
        entry: &str,
        records: Vec<(RecordReader, Transaction)>,
    ) -> Result<ErrorRecordMap, ReductError> {
        let (headers, stream) = Self::prepare_batch(records);
        let request = self.client.request(
            Method::POST,
            &format!(
                "{}{}/b/{}/{}/batch",
                self.server_url, API_PATH, self.bucket_name, entry
            ),
        );
        let response = request
            .headers(headers)
            .body(Body::wrap_stream(stream))
            .send()
            .await;
        let failed_records = Self::parse_record_errors(response)?;

        Ok(failed_records)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::proto::record::Label;
    use crate::storage::proto::{us_to_ts, Record};
    use futures_util::StreamExt;
    use hyper::http;
    use rstest::*;
    use std::pin::pin;

    #[rstest]
    #[tokio::test]
    async fn test_prepare_batch() {
        let (tx1, rx1) = tokio::sync::mpsc::channel(1);
        let rec1 = Record {
            timestamp: Some(us_to_ts(&1)),
            labels: vec![
                Label {
                    name: "label1".to_string(),
                    value: "value1".to_string(),
                },
                Label {
                    name: "with_comma".to_string(),
                    value: "[x,y]".to_string(),
                },
            ],

            content_type: "text/plain".to_string(),
            begin: 0,
            end: 10,
            state: 0,
        };

        let (tx2, rx2) = tokio::sync::mpsc::channel(1);
        let rec2 = Record {
            timestamp: Some(us_to_ts(&2)),
            labels: vec![Label {
                name: "label2".to_string(),
                value: "value2".to_string(),
            }],
            content_type: "image/jpg".to_string(),
            begin: 0,
            end: 10,
            state: 0,
        };

        let records = vec![
            (
                RecordReader::new(rx1, rec1, false),
                Transaction::WriteRecord(1),
            ),
            (
                RecordReader::new(rx2, rec2, false),
                Transaction::WriteRecord(2),
            ),
        ];

        let (headers, stream) = BucketWrapper::prepare_batch(records);

        assert_eq!(headers.len(), 4);
        assert_eq!(
            headers.get(CONTENT_TYPE).unwrap(),
            "application/octet-stream"
        );
        assert_eq!(headers.get(CONTENT_LENGTH).unwrap(), "20");
        assert_eq!(
            headers.get("x-reduct-time-1").unwrap(),
            "10,text/plain,label1=value1,with_comma=\"[x,y]\""
        );
        assert_eq!(
            headers.get("x-reduct-time-2").unwrap(),
            "10,image/jpg,label2=value2"
        );

        let mut stream = pin!(stream);

        tx1.send(Ok(Bytes::from("record1"))).await.unwrap();
        drop(tx1);

        tx2.send(Ok(Bytes::from("record2"))).await.unwrap();
        drop(tx2);

        let chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(chunk, Bytes::from("record1"));
        let chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(chunk, Bytes::from("record2"));
    }

    #[rstest]
    fn test_error_parsing() {
        let response = http::Response::builder()
            .header("x-reduct-error-1", "404,Not Found")
            .body(Bytes::new())
            .unwrap();
        let response = Ok(response).map(|r| r.into());
        let failed_records = BucketWrapper::parse_record_errors(response).unwrap();
        assert_eq!(failed_records.len(), 1);
        assert_eq!(
            failed_records.get(&1).unwrap().status(),
            ErrorCode::NotFound
        );
        assert_eq!(failed_records.get(&1).unwrap().message(), "Not Found");
    }
}
