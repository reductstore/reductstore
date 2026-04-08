// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::core::internal_client::{
    ClientBuildErrorContext, ClientBuildErrorKind, InternalClientApi, InternalClientBuilder,
};
use crate::replication::remote_bucket::{ErrorRecordMap, RemoteBucketConfig};
use async_stream::stream;
use async_trait::async_trait;
use axum::http::HeaderName;
use bytes::Bytes;
use futures_util::Stream;
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::io::BoxedReadRecord;
use reduct_base::unprocessable_entity;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_LENGTH, CONTENT_TYPE};
use reqwest::{Body, Client, Error, Method, Response};
use std::collections::BTreeMap;
use std::str::FromStr;

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

pub(super) type BoxedBucketApi = Box<dyn ReductBucketApi + Sync + Send>;

struct ReductClient {
    client_api: InternalClientApi,
    server_url: String,
}

static API_PATH: &str = "api/v1";

impl ReductClient {
    fn new(
        url: &str,
        api_token: &str,
        verify_ssl: bool,
        ca_path: Option<&std::path::PathBuf>,
    ) -> Result<Self, ReductError> {
        let client = InternalClientBuilder::new(ClientBuildErrorContext {
            ca_read: "Failed to read replication CA certificate",
            ca_parse: "Invalid replication CA certificate",
            client_build: "Failed to build replication HTTP client",
            kind: ClientBuildErrorKind::UnprocessableEntity,
        })
        .api_token(api_token)
        .verify_ssl(verify_ssl)
        .ca_path(ca_path)
        .connect_timeout(std::time::Duration::from_secs(10))
        .build()?;

        let client_api = InternalClientApi::with_single_url(client, url.to_string());
        let server_url = client_api
            .primary_url()
            .expect("normalized URL must exist")
            .to_string();

        Ok(Self {
            client_api,
            server_url,
        })
    }
}

struct BucketWrapper {
    server_url: String,
    bucket_name: String,
    client: Client,
}

impl BucketWrapper {
    fn build_headers(records: &Vec<BoxedReadRecord>, update_only: bool) -> HeaderMap {
        let mut headers = HeaderMap::new();
        let content_length: u64 = if update_only {
            0
        } else {
            records.iter().map(|r| r.meta().content_length()).sum()
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
            let meta = record.meta();
            let mut header_values = Vec::new();
            if update_only {
                header_values.push("0".to_string());
                header_values.push("".to_string());
            } else {
                header_values.push(meta.content_length().to_string());
                header_values.push(meta.content_type().to_string());
            }

            if !meta.labels().is_empty() {
                let mut label_headers = vec![];
                for (name, value) in meta.labels() {
                    if value.contains(',') {
                        label_headers.push(format!("{}=\"{}\"", name, value));
                    } else {
                        label_headers.push(format!("{}={}", name, value));
                    }
                }

                label_headers.sort();
                header_values.push(label_headers.join(","));
            }

            headers.insert(
                HeaderName::from_str(&format!("x-reduct-time-{}", meta.timestamp())).unwrap(),
                HeaderValue::from_str(&header_values.join(",").to_string()).unwrap(),
            );
        }

        headers
    }

    fn prepare_batch_to_write(
        mut records: Vec<BoxedReadRecord>,
    ) -> (HeaderMap, impl Stream<Item = Result<Bytes, ReductError>>) {
        Self::sort_by_timestamp(&mut records);
        let headers = Self::build_headers(&mut records, false);

        let stream = stream! {
            while let Some(mut record) = records.pop() {
                while let Some(chunk) = record.read_chunk() {
                     yield chunk;
                }
            }
        };

        (headers, stream)
    }

    fn prepare_batch_to_update(records: &Vec<BoxedReadRecord>) -> HeaderMap {
        let headers = Self::build_headers(&records, true);
        headers
    }

    fn parse_record_errors(
        response: Result<Response, Error>,
    ) -> Result<BTreeMap<u64, ReductError>, ReductError> {
        let mut failed_records = BTreeMap::new();
        let response = check_response(response)?;
        let headers = response
            .headers()
            .iter()
            .filter(|(key, _)| key.as_str().starts_with("x-reduct-error-"))
            .collect::<Vec<_>>();

        for (key, value) in headers {
            let record_ts = key
                .as_str()
                .trim_start_matches("x-reduct-error-")
                .parse::<u64>()
                .map_err(|err| unprocessable_entity!("Invalid timestamp {}: {}", key, err))?;

            let (status, message) = value.to_str().unwrap().split_once(',').unwrap();
            failed_records.insert(
                record_ts,
                ReductError::new(
                    status
                        .parse::<i16>()
                        .unwrap_or(-1)
                        .try_into()
                        .unwrap_or(ErrorCode::Unknown),
                    message,
                ),
            );
        }

        Ok(failed_records)
    }

    fn sort_by_timestamp(records_to_update: &mut Vec<BoxedReadRecord>) {
        records_to_update.sort_by(|a, b| b.meta().timestamp().cmp(&a.meta().timestamp()));
    }
}

fn check_response(response: Result<Response, Error>) -> Result<Response, ReductError> {
    crate::core::internal_client::check_response(response)
}

#[async_trait]
impl ReductClientApi for ReductClient {
    async fn get_bucket(&self, bucket_name: &str) -> Result<BoxedBucketApi, ReductError> {
        let request = self.client_api.client().request(
            Method::GET,
            &format!("{}{}/b/{}", self.server_url, API_PATH, bucket_name),
        );

        let resp = request.send().await;
        check_response(resp)?;

        Ok(Box::new(BucketWrapper {
            server_url: self.server_url.clone(),
            bucket_name: bucket_name.to_string(),
            client: self.client_api.client().clone(),
        }))
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
        records: Vec<BoxedReadRecord>,
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

        Self::parse_record_errors(response)
    }

    async fn update_batch(
        &self,
        entry: &str,
        records: &Vec<BoxedReadRecord>,
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
        Self::parse_record_errors(response)
    }

    fn server_url(&self) -> &str {
        &self.server_url
    }

    fn name(&self) -> &str {
        &self.bucket_name
    }
}

/// Create a new Reduct client wrapper.
pub(super) fn create_client(config: &RemoteBucketConfig) -> Result<BoxedClientApi, ReductError> {
    Ok(Box::new(ReductClient::new(
        &config.url,
        &config.api_token,
        config.verify_ssl,
        config.ca_path.as_ref(),
    )?))
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::core::sync::RwLock;
    use crate::replication::remote_bucket::RemoteBucketConfig;
    use crate::storage::proto::record::Label;
    use crate::storage::proto::{us_to_ts, Record};
    use crossbeam_channel::{Receiver, Sender};
    use futures_util::StreamExt;
    use hyper::http;
    use reduct_base::io::{ReadChunk, ReadRecord, RecordMeta};
    use rstest::*;
    use std::io::{Read, Seek, SeekFrom};
    use std::path::PathBuf;
    use std::pin::pin;

    #[rstest]
    #[tokio::test]
    async fn test_prepare_batch_to_write(records: (Vec<BoxedReadRecord>, Vec<Tx>)) {
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

        txs[0].send(Ok(Bytes::from("record1"))).unwrap();
        drop(txs.remove(0));

        txs[0].send(Ok(Bytes::from("record2"))).unwrap();
        drop(txs.remove(0));

        let chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(chunk, Bytes::from("record1"));
        let chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(chunk, Bytes::from("record2"));
    }

    #[rstest]
    fn test_prepare_batch_to_update(records: (Vec<BoxedReadRecord>, Vec<Tx>)) {
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
    fn test_add_slash_to_url() {
        let client = ReductClient::new("http://localhost:8080", "", true, None).unwrap();
        assert_eq!(client.server_url, "http://localhost:8080/");
    }

    #[rstest]
    fn test_invalid_ca_path() {
        let err = ReductClient::new(
            "https://localhost:8080",
            "",
            true,
            Some(&PathBuf::from("/definitely/missing/ca.pem")),
        )
        .err()
        .unwrap();

        assert_eq!(err.status(), ErrorCode::UnprocessableEntity);
        assert!(err
            .message()
            .contains("Failed to read replication CA certificate"));
    }

    #[rstest]
    fn test_create_client_with_ca_and_token() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let ca_path = tmp_dir.path().join("ca.crt");
        std::fs::write(
            &ca_path,
            include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/../misc/ca.crt")),
        )
        .unwrap();

        let config = RemoteBucketConfig {
            url: "https://localhost:8080".to_string(),
            bucket_name: "bucket".to_string(),
            api_token: "token".to_string(),
            verify_ssl: true,
            ca_path: Some(ca_path),
        };

        let client = create_client(&config).unwrap();

        assert_eq!(client.url(), "https://localhost:8080/");
    }

    #[rstest]
    fn test_invalid_ca_certificate_content() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let ca_path = tmp_dir.path().join("ca.pem");
        std::fs::write(
            &ca_path,
            b"-----BEGIN CERTIFICATE-----\ninvalid\n-----END CERTIFICATE-----\n",
        )
        .unwrap();

        let err = ReductClient::new("https://localhost:8080", "", true, Some(&ca_path)).err();

        assert!(err.is_some());
        let err = err.unwrap();
        assert_eq!(err.status(), ErrorCode::UnprocessableEntity);
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

    #[rstest]
    fn test_invalid_error_parsing() {
        let response = http::Response::builder()
            .header("x-reduct-error-1", "404,Not Found")
            .header("x-reduct-error-xxx", "404")
            .body(Bytes::new())
            .unwrap();
        let response = Ok(response).map(|r| r.into());
        let failed_records = BucketWrapper::parse_record_errors(response);
        assert_eq!(
            failed_records.unwrap_err().status(),
            ErrorCode::UnprocessableEntity
        );
    }

    type Tx = Sender<Result<Bytes, ReductError>>;

    mod check_response {
        use super::*;
        #[rstest]
        fn response_ok() {
            let response = http::Response::builder()
                .status(200)
                .body(Bytes::new())
                .unwrap();
            let response = Ok(response).map(|r| r.into());
            assert_eq!(check_response(response).unwrap().status(), 200);
        }

        #[rstest]
        fn response_error() {
            let response = http::Response::builder()
                .status(404)
                .header("x-reduct-error", "404,Not Found")
                .body(Bytes::new())
                .unwrap();
            let response = Ok(response).map(|r| r.into());
            assert_eq!(
                check_response(response).unwrap_err().status(),
                ErrorCode::NotFound
            );
        }
    }

    mod mock_record_reader {
        use super::*;
        #[rstest]
        fn test_meta(records: (Vec<BoxedReadRecord>, Vec<Tx>)) {
            let (records, _) = records;
            let mut record = records.into_iter().next().unwrap();
            assert_eq!(record.meta().timestamp(), 1);
            assert_eq!(record.meta().content_type(), "text/plain");
            assert_eq!(record.meta().content_length(), 10);
            assert_eq!(record.meta().labels().len(), 2);

            let _ = record.meta_mut();
        }

        #[rstest]
        fn test_seek_unimplemented(records: (Vec<BoxedReadRecord>, Vec<Tx>)) {
            let (records, _) = records;
            let err = records
                .into_iter()
                .next()
                .unwrap()
                .seek(SeekFrom::Start(0))
                .unwrap_err();

            assert_eq!(err.kind(), std::io::ErrorKind::Unsupported);
        }

        #[rstest]
        fn test_read_unimplemented(records: (Vec<BoxedReadRecord>, Vec<Tx>)) {
            let (records, _) = records;
            let mut record = records.into_iter().next().unwrap();
            let mut buf = [0; 10];
            let err = record.read(&mut buf).unwrap_err();
            assert_eq!(err.kind(), std::io::ErrorKind::Unsupported);
        }
    }

    pub struct MockRecordReader {
        meta: RecordMeta,
        rx: RwLock<Receiver<Result<Bytes, ReductError>>>,
    }

    impl MockRecordReader {
        pub(crate) fn form_record_with_rx(
            rx: Receiver<Result<Bytes, ReductError>>,
            record: Record,
        ) -> BoxedReadRecord {
            Box::new(Self {
                meta: record.into(),
                rx: RwLock::new(rx),
            })
        }
    }

    impl Read for MockRecordReader {
        fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "not implemented",
            ))
        }
    }

    impl Seek for MockRecordReader {
        fn seek(&mut self, _pos: SeekFrom) -> std::io::Result<u64> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "not implemented",
            ))
        }
    }

    impl ReadRecord for MockRecordReader {
        fn read_chunk(&mut self) -> ReadChunk {
            match self.rx.write().unwrap().recv() {
                Ok(chunk) => Some(chunk),
                Err(_) => None,
            }
        }

        fn meta(&self) -> &RecordMeta {
            &self.meta
        }

        fn meta_mut(&mut self) -> &mut RecordMeta {
            &mut self.meta
        }
    }

    #[fixture]
    fn records() -> (Vec<BoxedReadRecord>, Vec<Tx>) {
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

        let (tx1, rx1) = crossbeam_channel::unbounded();
        let (tx2, rx2) = crossbeam_channel::unbounded();

        (
            vec![
                MockRecordReader::form_record_with_rx(rx1, rec1),
                MockRecordReader::form_record_with_rx(rx2, rec2),
            ],
            vec![tx1, tx2],
        )
    }
}
