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

    fn build_headers(records: &Vec<RecordReader>, update_only: bool) -> HeaderMap {
        let mut headers = HeaderMap::new();
        let content_length: u64 = if update_only {
            0
        } else {
            records.iter().map(|r| r.content_length()).sum()
        };

        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_str("application/octet-stream").unwrap(),
        );
        headers.insert(
            CONTENT_LENGTH,
            HeaderValue::from_str(&content_length.to_string()).unwrap(),
        );

        for record in records {
            let mut header_values = Vec::new();
            if update_only {
                header_values.push("0".to_string());
                header_values.push("".to_string());
            } else {
                header_values.push(record.content_length().to_string());
                header_values.push(record.content_type().to_string());
            }

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

    fn prepare_batch_to_write(
        mut records: Vec<RecordReader>,
    ) -> (HeaderMap, impl Stream<Item = Result<Bytes, ReductError>>) {
        Self::sort_by_timestamp(&mut records);
        let headers = Self::build_headers(&mut records, false);

        let stream = stream! {
             while let Some(mut record) = records.pop() {
                let rx = record.rx();
                 while let Some(chunk) = rx.recv().await {
                     yield chunk;
                 }
             }
        };

        (headers, stream)
    }

    fn prepare_batch_to_update(records: &Vec<RecordReader>) -> HeaderMap {
        let headers = Self::build_headers(&records, true);
        headers
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

    fn sort_by_timestamp(records_to_update: &mut Vec<RecordReader>) {
        records_to_update.sort_by(|a, b| {
            let a = a.record();
            let b = b.record();
            ts_to_us(&b.timestamp.as_ref().unwrap()).cmp(&ts_to_us(&a.timestamp.as_ref().unwrap()))
        });
    }
}

fn check_response(response: Result<Response, Error>) -> Result<(), ReductError> {
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
        records: Vec<RecordReader>,
    ) -> Result<ErrorRecordMap, ReductError> {
        let (headers, stream) = Self::prepare_batch_to_write(records);
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

    async fn update_batch(
        &self,
        entry: &str,
        records: &Vec<RecordReader>,
    ) -> Result<ErrorRecordMap, ReductError> {
        let headers = Self::prepare_batch_to_update(records);
        let request = self.client.request(
            Method::PATCH,
            &format!(
                "{}{}/b/{}/{}/batch",
                self.server_url, API_PATH, self.bucket_name, entry
            ),
        );
        let response = request.headers(headers).send().await;
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
    use tokio::sync::mpsc::Sender;

    #[rstest]
    #[tokio::test]
    async fn test_prepare_batch_to_write(records: (Vec<RecordReader>, Vec<Tx>)) {
        let (records, mut txs) = records;
        let (headers, stream) = BucketWrapper::prepare_batch_to_write(records);

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

        txs[0].send(Ok(Bytes::from("record1"))).await.unwrap();
        drop(txs.remove(0));

        txs[0].send(Ok(Bytes::from("record2"))).await.unwrap();
        drop(txs.remove(0));

        let chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(chunk, Bytes::from("record1"));
        let chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(chunk, Bytes::from("record2"));
    }

    #[rstest]
    fn test_prepare_batch_to_update(records: (Vec<RecordReader>, Vec<Tx>)) {
        let (records, _) = records;
        let headers = BucketWrapper::prepare_batch_to_update(&records);

        assert_eq!(headers.len(), 4);
        assert_eq!(
            headers.get(CONTENT_TYPE).unwrap(),
            "application/octet-stream"
        );
        assert_eq!(headers.get(CONTENT_LENGTH).unwrap(), "0");
        assert_eq!(
            headers.get("x-reduct-time-1").unwrap(),
            "0,,label1=value1,with_comma=\"[x,y]\""
        );
        assert_eq!(headers.get("x-reduct-time-2").unwrap(), "0,,label2=value2");
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

    type Tx = Sender<Result<Bytes, ReductError>>;

    #[fixture]
    fn records() -> (Vec<RecordReader>, Vec<Tx>) {
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

        let (tx1, rx1) = tokio::sync::mpsc::channel(1);
        let (tx2, rx2) = tokio::sync::mpsc::channel(1);

        (
            vec![
                RecordReader::new(rx1, rec1, false),
                RecordReader::new(rx2, rec2, false),
            ],
            vec![tx1, tx2],
        )
    }
}
