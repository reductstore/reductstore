// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use axum::extract::{BodyStream, Path, Query, State};
use axum::http::header::HeaderMap;

use axum::body::StreamBody;
use axum::http::{HeaderName, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures_util::stream::StreamExt;
use futures_util::Stream;

use std::collections::HashMap;

use crate::auth::policy::{ReadAccessPolicy, WriteAccessPolicy};
use axum::headers;
use axum::headers::{Expect, Header, HeaderMapExt};
use log::{debug, error};
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;

use crate::core::status::HttpError;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::HttpServerComponents;
use crate::storage::entry::Labels;
use crate::storage::proto::QueryInfo;
use crate::storage::query::base::QueryOptions;
use crate::storage::reader::RecordReader;
use crate::storage::writer::{Chunk, RecordWriter};

pub struct EntryApi {}

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

    // GET /:bucket/:entry?ts=<number>|id=<number>|
    pub async fn read_record(
        State(components): State<Arc<RwLock<HttpServerComponents>>>,
        Path(path): Path<HashMap<String, String>>,
        Query(params): Query<HashMap<String, String>>,
        headers: HeaderMap,
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

        let ts = match params.get("ts") {
            Some(ts) => Some(ts.parse::<u64>().map_err(|_| {
                HttpError::unprocessable_entity("'ts' must be an unix timestamp in microseconds")
            })?),
            None => None,
        };

        let query = match params.get("q") {
            Some(query) => Some(query.parse::<u64>().map_err(|_| {
                HttpError::unprocessable_entity("'query' must be an unix timestamp in microseconds")
            })?),
            None => None,
        };

        let ts = if ts.is_none() && query.is_none() {
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

        let (reader, last) = if let Some(ts) = ts {
            let mut components = components.write().unwrap();
            let bucket = components.storage.get_bucket(bucket_name)?;
            (bucket.begin_read(entry_name, ts)?, true)
        } else {
            let mut components = components.write().unwrap();
            let bucket = components.storage.get_bucket(bucket_name)?;
            let entry = bucket.get_entry(entry_name)?;
            entry.next(query.unwrap())?
        };

        let headers = {
            let reader = reader.read().unwrap();
            let mut headers = HeaderMap::new();
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

        struct ReaderWrapper {
            reader: Arc<RwLock<RecordReader>>,
        }

        /// A wrapper around a `RecordReader` that implements `Stream` with RwLock
        impl Stream for ReaderWrapper {
            type Item = Result<Bytes, HttpError>;

            fn poll_next(
                self: Pin<&mut ReaderWrapper>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
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

        Ok((headers, StreamBody::new(ReaderWrapper { reader })))
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
    use axum::body::Empty;
    use axum::extract::FromRequest;
    use axum::http::Request;

    use std::path::PathBuf;

    #[tokio::test]
    async fn test_write_with_label_ok() {
        let (components, headers, body, path) = setup().await;

        let mut headers = headers;
        headers.insert("x-reduct-label-x", "y".parse().unwrap());
        EntryApi::write_record(
            State(Arc::clone(&components)),
            headers,
            path,
            Query(HashMap::from_iter(vec![(
                "ts".to_string(),
                "0".to_string(),
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
            .begin_read("entry-1", 0)
            .unwrap();

        assert_eq!(
            record.read().unwrap().labels().get("x"),
            Some(&"y".to_string())
        );
    }

    async fn setup() -> (
        Arc<RwLock<HttpServerComponents>>,
        HeaderMap,
        BodyStream,
        Path<HashMap<String, String>>,
    ) {
        let data_path = tempfile::tempdir().unwrap().into_path();

        let mut components = HttpServerComponents {
            storage: Storage::new(PathBuf::from(data_path.clone())),
            auth: TokenAuthorization::new(""),
            token_repo: create_token_repository(data_path.clone(), ""),
            console: ZipAssetManager::new(&[]),
            base_path: "/".to_string(),
        };

        components
            .storage
            .create_bucket("bucket-1", BucketSettings::default())
            .unwrap();

        let emtpy_stream: Empty<Bytes> = Empty::new();
        let request = Request::builder().body(emtpy_stream).unwrap();
        let body = BodyStream::from_request(request, &()).await.unwrap();

        let mut headers = HeaderMap::new();
        headers.insert("content-length", "0".parse().unwrap());

        let path = Path(HashMap::from_iter(vec![
            ("bucket_name".to_string(), "bucket-1".to_string()),
            ("entry_name".to_string(), "entry-1".to_string()),
        ]));
        (Arc::new(RwLock::new(components)), headers, body, path)
    }
}
