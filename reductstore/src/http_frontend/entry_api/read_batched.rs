// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::auth::policy::ReadAccessPolicy;
use crate::core::status::{HttpError, HttpStatus};
use crate::http_frontend::entry_api::MethodExtractor;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::HttpServerState;
use crate::storage::bucket::Bucket;

use crate::storage::reader::RecordReader;

use axum::body::StreamBody;
use axum::extract::{Path, Query, State};
use axum::headers::{HeaderMap, HeaderName, HeaderValue};
use axum::response::IntoResponse;
use bytes::{Buf, Bytes};
use futures_util::Stream;

use std::collections::HashMap;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};

// GET /:bucket/:entry/batch?q=<number>
pub async fn read_batched_records(
    State(components): State<Arc<HttpServerState>>,
    Path(path): Path<HashMap<String, String>>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    method: MethodExtractor,
) -> Result<impl IntoResponse, HttpError> {
    let bucket_name = path.get("bucket_name").unwrap();
    let entry_name = path.get("entry_name").unwrap();
    check_permissions(
        &components,
        headers,
        ReadAccessPolicy {
            bucket: bucket_name.clone(),
        },
    )
    .await?;

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
            .storage
            .write()
            .await
            .get_mut_bucket(bucket_name)?,
        entry_name,
        query_id,
        method.name == "HEAD",
    )
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
            reader.content_length().to_string(),
            reader.content_type().clone(),
        ];

        let mut labels: Vec<String> = reader
            .labels()
            .iter()
            .map(|(k, v)| {
                if v.contains(",") {
                    format!("{}=\"{}\"", k, v)
                } else {
                    format!("{}={}", k, v)
                }
            })
            .collect();
        labels.sort();

        meta_data.append(&mut labels);
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

#[cfg(test)]
mod tests {
    use super::*;

    use axum::body::HttpBody;

    use crate::http_frontend::tests::{components, headers, path_to_entry_1};
    use crate::storage::query::base::QueryOptions;
    use rstest::*;

    #[rstest]
    #[case("GET", "Hey!!!")]
    #[case("HEAD", "")]
    #[tokio::test]
    async fn test_batched_read(
        components: Arc<RwLock<HttpServerState>>,
        path_to_entry_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
        #[case] method: String,
        #[case] body: String,
    ) {
        let query_id = {
            components
                .write()
                .unwrap()
                .storage
                .get_bucket(path_to_entry_1.get("bucket_name").unwrap())
                .unwrap()
                .get_entry(path_to_entry_1.get("entry_name").unwrap())
                .unwrap()
                .query(0, u64::MAX, QueryOptions::default())
                .unwrap()
        };

        let mut response = read_batched_records(
            State(Arc::clone(&components)),
            path_to_entry_1,
            Query(HashMap::from_iter(vec![(
                "q".to_string(),
                query_id.to_string(),
            )])),
            headers,
            MethodExtractor::new(method.as_str()),
        )
        .await
        .unwrap()
        .into_response();

        let headers = response.headers();
        assert_eq!(headers["x-reduct-time-0"], "6,text/plain,b=\"[a,b]\",x=y");
        assert_eq!(headers["content-type"], "application/octet-stream");
        assert_eq!(headers["content-length"], "6");
        assert_eq!(headers["x-reduct-last"], "true");

        assert_eq!(
            response.data().await.unwrap_or(Ok(Bytes::new())).unwrap(),
            Bytes::from(body)
        );
    }
}
