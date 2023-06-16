// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use axum::async_trait;
use axum::body::StreamBody;
use axum::extract::{BodyStream, FromRequest, Path, Query, State};
use axum::http::header::HeaderMap;
use axum::http::{HeaderName, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures_util::stream::StreamExt;
use futures_util::Stream;

use std::collections::HashMap;

use crate::auth::policy::{ReadAccessPolicy, WriteAccessPolicy};
use axum::headers;
use axum::headers::{Expect, Header, HeaderMapExt, HeaderValue};

use log::{debug, error};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;

use crate::core::status::{HttpError, HttpStatus};
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::HttpServerComponents;
use crate::storage::bucket::Bucket;
use crate::storage::entry::Labels;
use crate::storage::proto::QueryInfo;
use crate::storage::query::base::QueryOptions;
use crate::storage::reader::RecordReader;
use crate::storage::writer::{Chunk, RecordWriter};

pub struct EntryApi {}

pub struct MethodExtractor {
    name: String,
}

impl MethodExtractor {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }
}

#[async_trait]
impl<S, B> FromRequest<S, B> for MethodExtractor
where
    S: Send + Sync + 'static,
    B: Send + Sync + 'static,
{
    type Rejection = HttpError;

    async fn from_request(req: Request<B>, _: &S) -> Result<Self, Self::Rejection> {
        let method = req.method().to_string();
        Ok(MethodExtractor { name: method })
    }
}

fn check_and_extract_ts_or_query_id(
    components: &Arc<RwLock<HttpServerComponents>>,
    params: HashMap<String, String>,
    bucket_name: &String,
    entry_name: &String,
) -> Result<(Option<u64>, Option<u64>), HttpError> {
    let ts = match params.get("ts") {
        Some(ts) => Some(ts.parse::<u64>().map_err(|_| {
            HttpError::unprocessable_entity("'ts' must be an unix timestamp in microseconds")
        })?),
        None => None,
    };

    let query_id = match params.get("q") {
        Some(query) => Some(
            query
                .parse::<u64>()
                .map_err(|_| HttpError::unprocessable_entity("'query' must be a number"))?,
        ),
        None => None,
    };

    let ts = if ts.is_none() && query_id.is_none() {
        let mut components = components.write().unwrap();
        Some(
            components
                .storage
                .get_bucket(bucket_name)?
                .get_entry(entry_name)?
                .info()?
                .latest_record,
        )
    } else {
        ts
    };
    Ok((query_id, ts))
}

fn fetch_and_response_batched_records(
    bucket: &mut Bucket,
    entry_name: &str,
    query_id: u64,
    empty_body: bool,
) -> Result<impl IntoResponse, HttpError> {
    const MAX_HEADER_SIZE: u64 = 6_000; // many http servers have a default limit of 8kb
    const MAX_BODY_SIZE: u64 = 16_000_000; // 16mb just not to be too big
    const MAX_RECORDS: usize = 85; // some clients have a limit of 100 headers

    let make_header = |reader: &RecordReader| {
        let name = HeaderName::from_str(&format!("x-reduct-time-{}", reader.timestamp())).unwrap();

        let mut meta_data = vec![
            format!("content-length={}", reader.content_length()),
            format!("content-type={}", reader.content_type()),
        ];
        meta_data.append(
            &mut reader
                .labels()
                .iter()
                .map(|(k, v)| {
                    if v.contains(",") {
                        format!("label-{}=\"{}\"", k, v)
                    } else {
                        format!("label-{}={}", k, v)
                    }
                })
                .collect(),
        );
        meta_data.sort();

        let value: HeaderValue = meta_data.join(",").parse().unwrap();

        (name, value)
    };

    let mut header_size = 0u64;
    let mut body_size = 0u64;
    let mut headers = HeaderMap::new();
    let mut readers = Vec::new();
    let mut last = false;
    loop {
        let _reader = match bucket.next(entry_name, query_id) {
            Ok((reader, _)) => {
                {
                    let reader_lock = reader.read().unwrap();

                    let (name, value) = make_header(&reader_lock);
                    header_size += (name.as_str().len() + value.to_str().unwrap().len() + 2) as u64;
                    body_size += reader_lock.content_length();
                    headers.insert(name, value);
                }
                readers.push(reader);

                if header_size > MAX_HEADER_SIZE
                    || body_size > MAX_BODY_SIZE
                    || readers.len() > MAX_RECORDS
                {
                    // This is not correct, because we should check sizes before adding the record
                    // but we can't know the size in advance and after next() we can't go back
                    break;
                }
            }
            Err(err) => {
                if readers.is_empty() {
                    return Err(err);
                } else {
                    if err.status() == HttpStatus::NoContent as i32 {
                        last = true;
                        break;
                    } else {
                        return Err(err);
                    }
                }
            }
        };
    }

    headers.insert("content-length", body_size.to_string().parse().unwrap());
    headers.insert("content-type", "application/octet-stream".parse().unwrap());
    headers.insert("x-reduct-last", last.to_string().parse().unwrap());

    struct ReadersWrapper {
        readers: Vec<Arc<RwLock<RecordReader>>>,
        empty_body: bool,
    }

    impl Stream for ReadersWrapper {
        type Item = Result<Bytes, HttpError>;

        fn poll_next(
            mut self: Pin<&mut ReadersWrapper>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            if self.empty_body {
                return Poll::Ready(None);
            }

            if self.readers.is_empty() {
                return Poll::Ready(None);
            }

            if self.readers[0].read().unwrap().is_done() {
                self.readers.remove(0);
            }

            match self.readers[0].write().unwrap().read() {
                Ok(chunk) => Poll::Ready(Some(Ok(chunk.unwrap()))),
                Err(e) => Poll::Ready(Some(Err(e))),
            }
        }
        fn size_hint(&self) -> (usize, Option<usize>) {
            (0, None)
        }
    }

    Ok((
        headers,
        StreamBody::new(ReadersWrapper {
            readers,
            empty_body,
        }),
    ))
}

fn fetch_and_response_single_record(
    bucket: &mut Bucket,
    entry_name: &str,
    ts: Option<u64>,
    query_id: Option<u64>,
    empty_body: bool,
) -> Result<impl IntoResponse, HttpError> {
    let make_headers = |reader: &Arc<RwLock<RecordReader>>, last| {
        let mut headers = HeaderMap::new();

        let reader = reader.read().unwrap();
        for (k, v) in reader.labels() {
            headers.insert(
                format!("x-reduct-label-{}", k)
                    .parse::<HeaderName>()
                    .unwrap(),
                v.parse().unwrap(),
            );
        }

        headers.insert(
            "content-type",
            reader.content_type().to_string().parse().unwrap(),
        );
        headers.insert(
            "content-length",
            reader.content_length().to_string().parse().unwrap(),
        );
        headers.insert(
            "x-reduct-time",
            reader.timestamp().to_string().parse().unwrap(),
        );
        headers.insert("x-reduct-last", u8::from(last).to_string().parse().unwrap());
        headers
    };

    let (reader, last) = if let Some(ts) = ts {
        let reader = bucket.begin_read(entry_name, ts)?;
        (reader, true)
    } else {
        bucket.next(entry_name, query_id.unwrap())?
    };

    let headers = make_headers(&reader, last);

    struct ReaderWrapper {
        reader: Arc<RwLock<RecordReader>>,
        empty_body: bool,
    }

    /// A wrapper around a `RecordReader` that implements `Stream` with RwLock
    impl Stream for ReaderWrapper {
        type Item = Result<Bytes, HttpError>;

        fn poll_next(
            self: Pin<&mut ReaderWrapper>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            if self.empty_body {
                return Poll::Ready(None);
            }

            if self.reader.read().unwrap().is_done() {
                return Poll::Ready(None);
            }
            match self.reader.write().unwrap().read() {
                Ok(chunk) => Poll::Ready(Some(Ok(chunk.unwrap()))),
                Err(e) => Poll::Ready(Some(Err(e))),
            }
        }
        fn size_hint(&self) -> (usize, Option<usize>) {
            (0, None)
        }
    }

    Ok((
        headers,
        StreamBody::new(ReaderWrapper { reader, empty_body }),
    ))
}

impl IntoResponse for QueryInfo {
    fn into_response(self) -> Response {
        let mut headers = HeaderMap::new();
        headers.typed_insert(headers::ContentType::json());

        (
            StatusCode::OK,
            headers,
            serde_json::to_string(&self).unwrap(),
        )
            .into_response()
    }
}

impl EntryApi {
    // POST /:bucket/:entry?ts=<number>
    pub async fn write_record(
        State(components): State<Arc<RwLock<HttpServerComponents>>>,
        headers: HeaderMap,
        Path(path): Path<HashMap<String, String>>,
        Query(params): Query<HashMap<String, String>>,
        mut stream: BodyStream,
    ) -> Result<(), HttpError> {
        let bucket = path.get("bucket_name").unwrap();
        check_permissions(
            Arc::clone(&components),
            headers.clone(),
            WriteAccessPolicy {
                bucket: bucket.clone(),
            },
        )?;

        let check_request_and_get_writer = || -> Result<Arc<RwLock<RecordWriter>>, HttpError> {
            if !params.contains_key("ts") {
                return Err(HttpError::unprocessable_entity(
                    "'ts' parameter is required",
                ));
            }

            let ts = match params.get("ts").unwrap().parse::<u64>() {
                Ok(ts) => ts,
                Err(_) => {
                    return Err(HttpError::unprocessable_entity(
                        "'ts' must be an unix timestamp in microseconds",
                    ));
                }
            };
            let content_size = headers
                .get("content-length")
                .ok_or(HttpError::unprocessable_entity(
                    "content-length header is required",
                ))?
                .to_str()
                .unwrap()
                .parse::<u64>()
                .map_err(|_| {
                    HttpError::unprocessable_entity("content-length header must be a number")
                })?;

            let content_type = headers
                .get("content-type")
                .map_or("application/octet-stream", |v| v.to_str().unwrap())
                .to_string();

            let mut labels = Labels::new();
            for (k, v) in headers.iter() {
                if k.as_str().starts_with("x-reduct-label-") {
                    let key = k.as_str()[15..].to_string();
                    let value = match v.to_str() {
                        Ok(value) => value.to_string(),
                        Err(_) => {
                            return Err(HttpError::unprocessable_entity(&format!(
                                "Label values for {} must be valid UTF-8 strings",
                                k
                            )));
                        }
                    };
                    labels.insert(key, value);
                }
            }

            let writer = {
                let mut components = components.write().unwrap();
                let bucket = components.storage.get_bucket(bucket)?;
                bucket.begin_write(
                    path.get("entry_name").unwrap(),
                    ts,
                    content_size,
                    content_type,
                    labels,
                )?
            };
            Ok(writer)
        };

        match check_request_and_get_writer() {
            Ok(writer) => {
                while let Some(chunk) = stream.next().await {
                    let mut writer = writer.write().unwrap();
                    let chunk = match chunk {
                        Ok(chunk) => chunk,
                        Err(e) => {
                            writer.write(Chunk::Error)?;
                            error!("Error while receiving data: {}", e);
                            return Err(HttpError::from(e));
                        }
                    };
                    writer.write(Chunk::Data(chunk))?;
                }

                writer.write().unwrap().write(Chunk::Last(Bytes::new()))?;
                Ok(())
            }
            Err(e) => {
                // drain the stream in the case if a client doesn't support Expect: 100-continue
                if !headers.contains_key(Expect::name()) {
                    debug!("draining the stream");
                    while let Some(_) = stream.next().await {}
                }

                Err(e)
            }
        }
    }

    // GET /:bucket/:entry?ts=<number>|q=<number>|
    pub async fn read_single_record(
        State(components): State<Arc<RwLock<HttpServerComponents>>>,
        Path(path): Path<HashMap<String, String>>,
        Query(params): Query<HashMap<String, String>>,
        headers: HeaderMap,
        method: MethodExtractor,
    ) -> Result<impl IntoResponse, HttpError> {
        let bucket_name = path.get("bucket_name").unwrap();
        let entry_name = path.get("entry_name").unwrap();
        check_permissions(
            Arc::clone(&components),
            headers,
            ReadAccessPolicy {
                bucket: bucket_name.clone(),
            },
        )?;

        let (query_id, ts) =
            check_and_extract_ts_or_query_id(&components, params, bucket_name, entry_name)?;

        fetch_and_response_single_record(
            components
                .write()
                .unwrap()
                .storage
                .get_bucket(bucket_name)?,
            entry_name,
            ts,
            query_id,
            method.name() == "HEAD",
        )
    }

    // GET /:bucket/:entry/batch?q=<number>
    pub async fn read_batched_records(
        State(components): State<Arc<RwLock<HttpServerComponents>>>,
        Path(path): Path<HashMap<String, String>>,
        Query(params): Query<HashMap<String, String>>,
        headers: HeaderMap,
        method: MethodExtractor,
    ) -> Result<impl IntoResponse, HttpError> {
        let bucket_name = path.get("bucket_name").unwrap();
        let entry_name = path.get("entry_name").unwrap();
        check_permissions(
            Arc::clone(&components),
            headers,
            ReadAccessPolicy {
                bucket: bucket_name.clone(),
            },
        )?;

        let query_id = match params.get("q") {
            Some(query) => query
                .parse::<u64>()
                .map_err(|_| HttpError::unprocessable_entity("'query' must be a number"))?,

            None => {
                return Err(HttpError::unprocessable_entity(
                    "'q' parameter is required for batched reads",
                ));
            }
        };

        fetch_and_response_batched_records(
            components
                .write()
                .unwrap()
                .storage
                .get_bucket(bucket_name)?,
            entry_name,
            query_id,
            method.name == "HEAD",
        )
    }

    // GET /:bucket/:entry/q?start=<number>&stop=<number>&continue=<number>&exclude-<label>=<value>&include-<label>=<value>&ttl=<number>
    pub async fn query(
        State(components): State<Arc<RwLock<HttpServerComponents>>>,
        Path(path): Path<HashMap<String, String>>,
        Query(params): Query<HashMap<String, String>>,
        headers: HeaderMap,
    ) -> Result<QueryInfo, HttpError> {
        let bucket_name = path.get("bucket_name").unwrap();
        let entry_name = path.get("entry_name").unwrap();

        check_permissions(
            Arc::clone(&components),
            headers,
            ReadAccessPolicy {
                bucket: bucket_name.clone(),
            },
        )?;

        let entry_info = {
            let mut components = components.write().unwrap();
            let bucket = components.storage.get_bucket(bucket_name)?;
            bucket.get_entry(entry_name)?.info()?
        };

        let start = match params.get("start") {
            Some(start) => start.parse::<u64>().map_err(|_| {
                HttpError::unprocessable_entity("'start' must be an unix timestamp in microseconds")
            })?,
            None => entry_info.oldest_record,
        };

        let stop = match params.get("stop") {
            Some(stop) => stop.parse::<u64>().map_err(|_| {
                HttpError::unprocessable_entity("'stop' must be an unix timestamp in microseconds")
            })?,
            None => entry_info.latest_record + 1,
        };

        let continuous = match params.get("continuous") {
            Some(continue_) => continue_.parse::<bool>().map_err(|_| {
                HttpError::unprocessable_entity(
                    "'continue' must be an unix timestamp in microseconds",
                )
            })?,
            None => false,
        };

        let ttl = match params.get("ttl") {
            Some(ttl) => ttl.parse::<u64>().map_err(|_| {
                HttpError::unprocessable_entity("'ttl' must be an unix timestamp in microseconds")
            })?,
            None => 5,
        };

        let mut include = HashMap::new();
        let mut exclude = HashMap::new();

        for (k, v) in params.iter() {
            if k.starts_with("include-") {
                include.insert(k[8..].to_string(), v.to_string());
            } else if k.starts_with("exclude-") {
                exclude.insert(k[8..].to_string(), v.to_string());
            }
        }

        let mut components = components.write().unwrap();
        let bucket = components.storage.get_bucket(bucket_name)?;
        let entry = bucket.get_or_create_entry(entry_name)?;
        let id = entry.query(
            start,
            stop,
            QueryOptions {
                continuous,
                include,
                exclude,
                ttl: Duration::from_secs(ttl),
            },
        )?;

        Ok(QueryInfo { id })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::asset_manager::ZipAssetManager;
    use crate::auth::token_auth::TokenAuthorization;
    use crate::auth::token_repository::create_token_repository;
    use crate::storage::proto::BucketSettings;
    use crate::storage::storage::Storage;
    use axum::body::{Empty, HttpBody};
    use axum::extract::FromRequest;
    use axum::http::Request;

    use rstest::{fixture, rstest};
    use std::path::PathBuf;

    #[fixture]
    fn components() -> Arc<RwLock<HttpServerComponents>> {
        let data_path = tempfile::tempdir().unwrap().into_path();

        let mut components = HttpServerComponents {
            storage: Storage::new(PathBuf::from(data_path.clone())),
            auth: TokenAuthorization::new(""),
            token_repo: create_token_repository(data_path.clone(), ""),
            console: ZipAssetManager::new(&[]),
            base_path: "/".to_string(),
        };

        let labels = HashMap::from_iter(vec![
            ("x".to_string(), "y".to_string()),
            ("b".to_string(), "[a,b]".to_string()),
        ]);
        components
            .storage
            .create_bucket("bucket-1", BucketSettings::default())
            .unwrap()
            .begin_write("entry-1", 0, 6, "text/plain".to_string(), labels)
            .unwrap()
            .write()
            .unwrap()
            .write(Chunk::Last(Bytes::from("Hey!!!")))
            .unwrap();
        Arc::new(RwLock::new(components))
    }

    #[fixture]
    fn headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("content-length", "0".parse().unwrap());
        headers.insert("x-reduct-label-x", "y".parse().unwrap());
        headers
    }

    #[fixture]
    fn path() -> Path<HashMap<String, String>> {
        let path = Path(HashMap::from_iter(vec![
            ("bucket_name".to_string(), "bucket-1".to_string()),
            ("entry_name".to_string(), "entry-1".to_string()),
        ]));
        path
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_with_label_ok(
        components: Arc<RwLock<HttpServerComponents>>,
        headers: HeaderMap,
        path: Path<HashMap<String, String>>,
    ) {
        let emtpy_stream: Empty<Bytes> = Empty::new();
        let request = Request::builder().body(emtpy_stream).unwrap();
        let body = BodyStream::from_request(request, &()).await.unwrap();

        EntryApi::write_record(
            State(Arc::clone(&components)),
            headers,
            path,
            Query(HashMap::from_iter(vec![(
                "ts".to_string(),
                "1".to_string(),
            )])),
            body,
        )
        .await
        .unwrap();

        let record = components
            .write()
            .unwrap()
            .storage
            .get_bucket("bucket-1")
            .unwrap()
            .begin_read("entry-1", 1)
            .unwrap();

        assert_eq!(
            record.read().unwrap().labels().get("x"),
            Some(&"y".to_string())
        );
    }

    #[rstest]
    #[case("GET", "Hey!!!")]
    #[case("HEAD", "")]
    #[tokio::test]
    async fn test_single_read_ts(
        components: Arc<RwLock<HttpServerComponents>>,
        path: Path<HashMap<String, String>>,
        #[case] method: String,
        #[case] body: String,
    ) {
        let mut response = EntryApi::read_single_record(
            State(Arc::clone(&components)),
            path,
            Query(HashMap::from_iter(vec![(
                "ts".to_string(),
                "0".to_string(),
            )])),
            HeaderMap::new(),
            MethodExtractor::new(&method),
        )
        .await
        .unwrap()
        .into_response();

        let headers = response.headers();
        assert_eq!(headers["x-reduct-time"], "0");
        assert_eq!(headers["content-type"], "text/plain");
        assert_eq!(headers["content-length"], "6");

        assert_eq!(
            response.data().await.unwrap_or(Ok(Bytes::new())).unwrap(),
            Bytes::from(body)
        );
    }

    #[rstest]
    #[case("GET", "Hey!!!")]
    #[case("HEAD", "")]
    #[tokio::test]
    async fn test_single_read_query(
        components: Arc<RwLock<HttpServerComponents>>,
        path: Path<HashMap<String, String>>,
        #[case] method: String,
        #[case] body: String,
    ) {
        let query_id = query_records(&components);

        let mut response = EntryApi::read_single_record(
            State(Arc::clone(&components)),
            path,
            Query(HashMap::from_iter(vec![(
                "q".to_string(),
                query_id.to_string(),
            )])),
            HeaderMap::new(),
            MethodExtractor::new(&method),
        )
        .await
        .unwrap()
        .into_response();

        let headers = response.headers();
        assert_eq!(headers["x-reduct-time"], "0");
        assert_eq!(headers["content-type"], "text/plain");
        assert_eq!(headers["content-length"], "6");

        assert_eq!(
            response.data().await.unwrap_or(Ok(Bytes::new())).unwrap(),
            Bytes::from(body)
        );
    }

    fn query_records(components: &Arc<RwLock<HttpServerComponents>>) -> u64 {
        let query_id = components
            .write()
            .unwrap()
            .storage
            .get_bucket("bucket-1")
            .unwrap()
            .get_entry("entry-1")
            .unwrap()
            .query(
                0,
                1,
                QueryOptions {
                    continuous: false,
                    include: HashMap::new(),
                    exclude: HashMap::new(),
                    ttl: Duration::from_secs(1),
                },
            )
            .unwrap();
        query_id
    }

    #[rstest]
    #[case("GET", "Hey!!!")]
    #[case("HEAD", "")]
    #[tokio::test]
    async fn test_batched_read(
        components: Arc<RwLock<HttpServerComponents>>,
        path: Path<HashMap<String, String>>,
        #[case] method: String,
        #[case] body: String,
    ) {
        let query_id = query_records(&components);

        let mut response = EntryApi::read_batched_records(
            State(Arc::clone(&components)),
            path,
            Query(HashMap::from_iter(vec![(
                "q".to_string(),
                query_id.to_string(),
            )])),
            HeaderMap::new(),
            MethodExtractor::new(method.as_str()),
        )
        .await
        .unwrap()
        .into_response();

        let headers = response.headers();
        assert_eq!(
            headers["x-reduct-time-0"],
            "content-length=6,content-type=text/plain,label-b=\"[a,b]\",label-x=y"
        );
        assert_eq!(headers["content-type"], "application/octet-stream");
        assert_eq!(headers["content-length"], "6");
        assert_eq!(headers["x-reduct-last"], "true");

        assert_eq!(
            response.data().await.unwrap_or(Ok(Bytes::new())).unwrap(),
            Bytes::from(body)
        );
    }
}
