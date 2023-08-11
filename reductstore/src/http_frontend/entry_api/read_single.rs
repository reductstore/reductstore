// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::auth::policy::ReadAccessPolicy;
use crate::http_frontend::entry_api::{check_and_extract_ts_or_query_id, MethodExtractor};
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::HttpError;
use crate::http_frontend::HttpServerState;
use crate::storage::bucket::Bucket;

use crate::storage::reader::RecordReader;

use axum::body::StreamBody;
use axum::extract::{Path, Query, State};
use axum::headers::{HeaderMap, HeaderName};
use axum::response::IntoResponse;
use bytes::Bytes;
use futures_util::Stream;

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};

// GET /:bucket/:entry?ts=<number>|q=<number>|
pub async fn read_single_record(
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

    let mut storage = components.storage.write().await;

    let (query_id, ts) =
        check_and_extract_ts_or_query_id(&storage, params, bucket_name, entry_name)?;

    let bucket = storage.get_mut_bucket(bucket_name)?;
    fetch_and_response_single_record(bucket, entry_name, ts, query_id, method.name() == "HEAD")
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
                Err(e) => Poll::Ready(Some(Err(HttpError::from(e)))),
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
    async fn test_single_read_ts(
        components: Arc<HttpServerState>,
        path_to_entry_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
        #[case] method: String,
        #[case] body: String,
    ) {
        let mut response = read_single_record(
            State(Arc::clone(&components)),
            path_to_entry_1,
            Query(HashMap::from_iter(vec![(
                "ts".to_string(),
                "0".to_string(),
            )])),
            headers,
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
        components: Arc<HttpServerState>,
        path_to_entry_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
        #[case] method: String,
        #[case] body: String,
    ) {
        let query_id = {
            components
                .storage
                .write()
                .await
                .get_mut_bucket(path_to_entry_1.get("bucket_name").unwrap())
                .unwrap()
                .get_mut_entry(path_to_entry_1.get("entry_name").unwrap())
                .unwrap()
                .query(0, u64::MAX, QueryOptions::default())
                .unwrap()
        };

        let mut response = read_single_record(
            State(Arc::clone(&components)),
            path_to_entry_1,
            Query(HashMap::from_iter(vec![(
                "q".to_string(),
                query_id.to_string(),
            )])),
            headers,
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
}
