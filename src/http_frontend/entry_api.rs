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
use log::{debug, info};
use std::collections::HashMap;

use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;

use crate::core::status::HttpError;
use crate::http_frontend::HttpServerComponents;
use crate::storage::entry::Labels;
use crate::storage::proto::QueryInfo;
use crate::storage::query::base::QueryOptions;
use crate::storage::reader::RecordReader;

pub struct EntryApi {}

impl IntoResponse for QueryInfo {
    fn into_response(self) -> Response {
        (StatusCode::OK, serde_json::to_string(&self).unwrap()).into_response()
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
        debug!("headers: {:?}", headers);
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

        let labels = headers
            .iter()
            .filter(|(k, _)| k.as_str().starts_with("x-reduct-label-"))
            .map(|(k, v)| {
                (
                    k.as_str()[15..].to_string(),
                    v.to_str().unwrap().to_string(),
                )
            })
            .collect::<Labels>();

        let writer = {
            let mut components = components.write().unwrap();
            let bucket = components
                .storage
                .get_bucket(path.get("bucket_name").unwrap())?;
            bucket.begin_write(
                path.get("entry_name").unwrap(),
                ts,
                content_size,
                content_type,
                labels,
            )?
        };

        while let Some(chunk) = stream.next().await {
            let mut writer = writer.write().unwrap();
            let chunk = chunk.unwrap();
            writer.write(chunk.as_ref(), false).unwrap();
        }

        writer.write().unwrap().write(vec![].as_ref(), true)?;
        Ok(())
    }

    // GET /:bucket/:entry?ts=<number>|id=<number>|
    pub async fn read_record(
        State(components): State<Arc<RwLock<HttpServerComponents>>>,
        Path(path): Path<HashMap<String, String>>,
        Query(params): Query<HashMap<String, String>>,
    ) -> Result<impl IntoResponse, HttpError> {
        let bucket_name = path.get("bucket_name").unwrap();
        let entry_name = path.get("entry_name").unwrap();

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
                    .info()?
                    .info
                    .unwrap()
                    .latest_record,
            )
        } else {
            ts
        };

        let reader = if let Some(ts) = ts {
            let mut components = components.write().unwrap();
            let bucket = components.storage.get_bucket(bucket_name)?;
            bucket.begin_read(entry_name, ts)?
        } else {
            let mut components = components.write().unwrap();
            let bucket = components.storage.get_bucket(bucket_name)?;
            let entry = bucket.get_or_create_entry(entry_name)?;
            entry.next(query.unwrap())?.0
        };

        let headers = {
            let reader = reader.read().unwrap();
            reader
                .labels()
                .iter()
                .fold(HeaderMap::new(), |mut headers, (k, v)| {
                    headers.insert(
                        format!("x-reduct-label-{}", k)
                            .parse::<HeaderName>()
                            .unwrap(),
                        v.parse().unwrap(),
                    );
                    headers
                })
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
                    Ok(chunk) => Poll::Ready(Some(Ok(Bytes::from(chunk.data)))),
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
    ) -> Result<QueryInfo, HttpError> {
        let bucket_name = path.get("bucket_name").unwrap();
        let entry_name = path.get("entry_name").unwrap();

        let entry_info = {
            let mut components = components.write().unwrap();
            let bucket = components.storage.get_bucket(bucket_name)?;
            bucket.get_or_create_entry(entry_name)?.info()?
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
            None => entry_info.latest_record,
        };

        let continuous = match params.get("continue") {
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
