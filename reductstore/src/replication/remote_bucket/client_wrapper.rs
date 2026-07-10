// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::core::internal_client::{
    ClientBuildErrorContext, ClientBuildErrorKind, InternalClientApi, InternalClientBuilder,
};
use crate::core::payload_codec::compress_payload;
use crate::replication::remote_bucket::{BatchStats, ErrorRecordMap, RemoteBucketConfig};
use async_trait::async_trait;
use axum::http::HeaderName;
use bytes::{Bytes, BytesMut};
use log::warn;
use reduct_base::batch::{is_compressible_mime, make_encoding_header_value, RecordEncoding};
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::io::BoxedReadRecord;
use reduct_base::msg::replication_api::ReplicationCompression;
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
    ) -> Result<(ErrorRecordMap, BatchStats), ReductError>;

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
    compression: ReplicationCompression,
}

static API_PATH: &str = "api/v1";

/// The first server version that understands `x-reduct-encoding-*` headers.
const MIN_COMPRESSION_API_VERSION: (u64, u64) = (1, 21);

/// Records smaller than this are not worth compressing.
const MIN_COMPRESS_SIZE: u64 = 128;

impl ReductClient {
    fn new(
        url: &str,
        api_token: &str,
        verify_ssl: bool,
        ca_path: Option<&std::path::PathBuf>,
        compression: ReplicationCompression,
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
            compression,
        })
    }

    /// Check via the `x-reduct-api` response header that the remote server is
    /// recent enough to understand compressed batch payloads.
    fn supports_compression(headers: &HeaderMap) -> bool {
        headers
            .get("x-reduct-api")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| {
                let (major, minor) = value.split_once('.')?;
                Some((
                    major.trim().parse::<u64>().ok()?,
                    minor.trim().parse::<u64>().ok()?,
                ))
            })
            .map(|version| version >= MIN_COMPRESSION_API_VERSION)
            .unwrap_or(false)
    }
}

struct BucketWrapper {
    server_url: String,
    bucket_name: String,
    client: Client,
    compression: ReplicationCompression,
}

impl BucketWrapper {
    /// Build the `x-reduct-time-*` header value for a record from its on-wire
    /// content length, content type and labels.
    fn format_record_header(
        content_length: &str,
        content_type: &str,
        meta: &reduct_base::io::RecordMeta,
    ) -> String {
        let mut header_values = vec![content_length.to_string(), content_type.to_string()];

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

        header_values.join(",")
    }

    fn base_headers(content_length: u64) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_str("application/octet-stream").unwrap(),
        );
        headers.insert(
            CONTENT_LENGTH,
            HeaderValue::from_str(&content_length.to_string()).unwrap(),
        );
        headers
    }

    /// Read the record bodies, compress them if configured (skipping
    /// already-compressed MIME types and payloads that don't shrink), and
    /// build the batch headers with the on-wire lengths.
    fn prepare_batch_to_write(
        mut records: Vec<BoxedReadRecord>,
        compression: ReplicationCompression,
    ) -> Result<(HeaderMap, Vec<Bytes>, BatchStats), ReductError> {
        records.sort_by(|a, b| a.meta().timestamp().cmp(&b.meta().timestamp()));

        let codec = match compression {
            ReplicationCompression::None => None,
            ReplicationCompression::Zstd => Some(RecordEncoding::Zstd),
            ReplicationCompression::Gzip => Some(RecordEncoding::Gzip),
        };

        let mut bodies = Vec::with_capacity(records.len());
        let mut stats = BatchStats::default();
        let mut headers = HeaderMap::new();
        for record in records.iter_mut() {
            let original_length = record.meta().content_length();
            let mut raw = BytesMut::with_capacity(original_length as usize);
            while let Some(chunk) = record.read_chunk() {
                raw.extend_from_slice(&chunk?);
            }
            let raw = raw.freeze();

            let meta = record.meta();
            let mut wire = raw;
            let mut encoding = None;
            if let Some(codec) = codec {
                if original_length >= MIN_COMPRESS_SIZE && is_compressible_mime(meta.content_type())
                {
                    let compressed = compress_payload(codec, &wire)?;
                    if (compressed.len() as u64) < original_length {
                        wire = Bytes::from(compressed);
                        encoding = Some(codec);
                    }
                }
            }

            let wire_length = if encoding.is_some() {
                wire.len() as u64
            } else {
                original_length
            };
            stats.raw_bytes += original_length;
            stats.wire_bytes += wire_length;

            headers.insert(
                HeaderName::from_str(&format!("x-reduct-time-{}", meta.timestamp())).unwrap(),
                HeaderValue::from_str(&Self::format_record_header(
                    &wire_length.to_string(),
                    meta.content_type(),
                    meta,
                ))
                .unwrap(),
            );
            if let Some(codec) = encoding {
                headers.insert(
                    HeaderName::from_str(&format!("x-reduct-encoding-{}", meta.timestamp()))
                        .unwrap(),
                    HeaderValue::from_str(&make_encoding_header_value(codec, original_length))
                        .unwrap(),
                );
            }

            bodies.push(wire);
        }

        headers.extend(Self::base_headers(stats.wire_bytes));
        Ok((headers, bodies, stats))
    }

    fn prepare_batch_to_update(records: &Vec<BoxedReadRecord>) -> HeaderMap {
        let mut headers = Self::base_headers(0);
        for record in records {
            let meta = record.meta();
            headers.insert(
                HeaderName::from_str(&format!("x-reduct-time-{}", meta.timestamp())).unwrap(),
                HeaderValue::from_str(&Self::format_record_header("0", "", meta)).unwrap(),
            );
        }
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
        let resp = check_response(resp)?;

        let mut compression = self.compression;
        if compression != ReplicationCompression::None
            && !Self::supports_compression(resp.headers())
        {
            warn!(
                "Remote server {} does not support replication payload compression, sending uncompressed",
                self.server_url
            );
            compression = ReplicationCompression::None;
        }

        Ok(Box::new(BucketWrapper {
            server_url: self.server_url.clone(),
            bucket_name: bucket_name.to_string(),
            client: self.client_api.client().clone(),
            compression,
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
    ) -> Result<(ErrorRecordMap, BatchStats), ReductError> {
        let (headers, bodies, stats) = Self::prepare_batch_to_write(records, self.compression)?;
        let request = self.client.request(
            Method::POST,
            &format!(
                "{}{}/b/{}/{}/batch",
                self.server_url, API_PATH, self.bucket_name, entry
            ),
        );

        let stream = futures_util::stream::iter(bodies.into_iter().map(Ok::<Bytes, ReductError>));
        let response = request
            .headers(headers)
            .body(Body::wrap_stream(stream))
            .send()
            .await;

        Ok((Self::parse_record_errors(response)?, stats))
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
        config.compression,
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
    use hyper::http;
    use reduct_base::io::{ReadChunk, ReadRecord, RecordMeta};
    use rstest::*;
    use std::io::{Read, Seek, SeekFrom};
    use std::path::PathBuf;

    #[rstest]
    #[tokio::test]
    async fn test_prepare_batch_to_write(records: (Vec<BoxedReadRecord>, Vec<Tx>)) {
        let (records, mut txs) = records;

        txs[0].send(Ok(Bytes::from("record1"))).unwrap();
        drop(txs.remove(0));
        txs[0].send(Ok(Bytes::from("record2"))).unwrap();
        drop(txs.remove(0));

        let (headers, bodies, stats) =
            BucketWrapper::prepare_batch_to_write(records, ReplicationCompression::None).unwrap();

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

        assert_eq!(bodies, vec![Bytes::from("record1"), Bytes::from("record2")]);
        assert_eq!(
            stats,
            BatchStats {
                raw_bytes: 20,
                wire_bytes: 20
            }
        );
    }

    mod compression {
        use super::*;
        use reduct_base::batch::parse_encoding_header;

        fn compressible_record(timestamp: u64, content_type: &str, body: Bytes) -> BoxedReadRecord {
            let (tx, rx) = crossbeam_channel::unbounded();
            let record = Record {
                timestamp: Some(us_to_ts(&timestamp)),
                labels: vec![],
                content_type: content_type.to_string(),
                begin: 0,
                end: body.len() as u64,
                state: 0,
            };
            tx.send(Ok(body)).unwrap();
            drop(tx);
            MockRecordReader::form_record_with_rx(rx, record)
        }

        #[rstest]
        #[case(ReplicationCompression::Zstd, RecordEncoding::Zstd)]
        #[case(ReplicationCompression::Gzip, RecordEncoding::Gzip)]
        fn test_compresses_record(
            #[case] compression: ReplicationCompression,
            #[case] expected_codec: RecordEncoding,
        ) {
            let body = Bytes::from("na ".repeat(1000));
            let records = vec![compressible_record(1, "text/plain", body.clone())];

            let (headers, bodies, stats) =
                BucketWrapper::prepare_batch_to_write(records, compression).unwrap();

            let wire_len = bodies[0].len() as u64;
            assert!(wire_len < 3000);
            assert_eq!(
                headers.get("x-reduct-time-1").unwrap().to_str().unwrap(),
                format!("{},text/plain", wire_len)
            );
            let (codec, original_length) = parse_encoding_header(
                headers
                    .get("x-reduct-encoding-1")
                    .unwrap()
                    .to_str()
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(codec, expected_codec);
            assert_eq!(original_length, 3000);
            assert_eq!(
                headers.get(CONTENT_LENGTH).unwrap().to_str().unwrap(),
                wire_len.to_string()
            );
            assert_eq!(
                stats,
                BatchStats {
                    raw_bytes: 3000,
                    wire_bytes: wire_len
                }
            );
        }

        #[rstest]
        fn test_skips_compressed_mime() {
            let body = Bytes::from(vec![0u8; 1000]);
            let records = vec![compressible_record(1, "image/jpeg", body.clone())];

            let (headers, bodies, stats) =
                BucketWrapper::prepare_batch_to_write(records, ReplicationCompression::Zstd)
                    .unwrap();

            assert!(headers.get("x-reduct-encoding-1").is_none());
            assert_eq!(headers.get("x-reduct-time-1").unwrap(), "1000,image/jpeg");
            assert_eq!(bodies[0], body);
            assert_eq!(
                stats,
                BatchStats {
                    raw_bytes: 1000,
                    wire_bytes: 1000
                }
            );
        }

        #[rstest]
        fn test_skips_tiny_records() {
            let records = vec![compressible_record(1, "text/plain", Bytes::from("tiny"))];

            let (headers, bodies, _) =
                BucketWrapper::prepare_batch_to_write(records, ReplicationCompression::Zstd)
                    .unwrap();

            assert!(headers.get("x-reduct-encoding-1").is_none());
            assert_eq!(bodies[0], Bytes::from("tiny"));
        }

        #[rstest]
        fn test_falls_back_when_payload_grows() {
            // random bytes don't compress, so the raw payload must be sent
            let body: Bytes = (0..1000u32)
                .flat_map(|i| i.wrapping_mul(2654435761).to_le_bytes())
                .collect::<Vec<u8>>()
                .into();
            let records = vec![compressible_record(
                1,
                "application/octet-stream",
                body.clone(),
            )];

            let (headers, bodies, stats) =
                BucketWrapper::prepare_batch_to_write(records, ReplicationCompression::Zstd)
                    .unwrap();

            assert!(headers.get("x-reduct-encoding-1").is_none());
            assert_eq!(bodies[0], body);
            assert_eq!(stats.wire_bytes, stats.raw_bytes);
        }

        #[rstest]
        #[case("1.21", true)]
        #[case("1.22", true)]
        #[case("2.0", true)]
        #[case("1.20", false)]
        #[case("1.9", false)]
        #[case("garbage", false)]
        fn test_supports_compression(#[case] version: &str, #[case] expected: bool) {
            let mut headers = HeaderMap::new();
            headers.insert("x-reduct-api", HeaderValue::from_str(version).unwrap());
            assert_eq!(ReductClient::supports_compression(&headers), expected);
        }

        #[rstest]
        fn test_supports_compression_no_header() {
            assert!(!ReductClient::supports_compression(&HeaderMap::new()));
        }
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
        let client = ReductClient::new(
            "http://localhost:8080",
            "",
            true,
            None,
            ReplicationCompression::None,
        )
        .unwrap();
        assert_eq!(client.server_url, "http://localhost:8080/");
    }

    #[rstest]
    fn test_invalid_ca_path() {
        let err = ReductClient::new(
            "https://localhost:8080",
            "",
            true,
            Some(&PathBuf::from("/definitely/missing/ca.pem")),
            ReplicationCompression::None,
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
            compression: ReplicationCompression::None,
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

        let err = ReductClient::new(
            "https://localhost:8080",
            "",
            true,
            Some(&ca_path),
            ReplicationCompression::None,
        )
        .err();

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
