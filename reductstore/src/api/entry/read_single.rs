// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::entry::MethodExtractor;
use crate::api::middleware::check_permissions;
use crate::api::Components;
use crate::api::HttpError;
use crate::auth::policy::ReadAccessPolicy;
use reduct_base::error::ReductError;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use axum_extra::headers::{HeaderMap, HeaderName};
use bytes::Bytes;
use futures_util::Stream;

use crate::api::entry::common::check_and_extract_ts_or_query_id;
use crate::core::weak::Weak;
use crate::storage::entry::{Entry, RecordReader};
use crate::storage::query::QueryRx;
use futures_util::Future;
use hyper::http::HeaderValue;
use reduct_base::bad_request;
use reduct_base::io::ReadRecord;
use std::collections::HashMap;
use std::pin::{pin, Pin};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::RwLock as AsyncRwLock;

// GET /:bucket/:entry?ts=<number>|q=<number>|
pub(crate) async fn read_record(
    State(components): State<Arc<Components>>,
    Path(path): Path<HashMap<String, String>>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    method: MethodExtractor,
) -> Result<impl IntoResponse, HttpError> {
    let bucket_name = path.get("bucket_name").unwrap();
    let entry_name = path.get("entry_name").unwrap();
    check_permissions(
        &components,
        &headers,
        ReadAccessPolicy {
            bucket: bucket_name.clone(),
        },
    )
    .await?;

    let entry = components
        .storage
        .get_bucket(bucket_name)?
        .upgrade()?
        .get_entry(entry_name)?;
    let last_record = entry.upgrade()?.info()?.latest_record;

    let (query_id, ts) = check_and_extract_ts_or_query_id(params, last_record)?;

    fetch_and_response_single_record(entry, ts, query_id, method.name() == "HEAD").await
}

async fn fetch_and_response_single_record(
    entry: Weak<Entry>,
    ts: Option<u64>,
    query_id: Option<u64>,
    empty_body: bool,
) -> Result<impl IntoResponse, HttpError> {
    let make_headers = |record_reader: &RecordReader| {
        let mut headers = HeaderMap::new();

        let meta = record_reader.meta();
        for (k, v) in meta.labels() {
            headers.insert(
                format!("x-reduct-label-{}", k)
                    .parse::<HeaderName>()
                    .unwrap(),
                v.parse().unwrap(),
            );
        }

        headers.insert(
            "content-type",
            HeaderValue::from_str(meta.content_type()).unwrap(),
        );
        headers.insert("content-length", HeaderValue::from(meta.content_length()));
        headers.insert("x-reduct-time", HeaderValue::from(meta.timestamp()));
        headers
    };

    let entry = entry.upgrade()?;
    let reader = if let Some(ts) = ts {
        entry.begin_read(ts).await?
    } else {
        let query_id = query_id.unwrap();
        let rx = entry.get_query_receiver(query_id)?;
        let query_path = format!("{}/{}/{}", entry.bucket_name(), entry.name(), query_id);
        next_record_reader(rx, &query_path).await?
    };

    let headers = make_headers(&reader);

    Ok((
        headers,
        Body::from_stream(ReaderWrapper { reader, empty_body }),
    ))
}

async fn next_record_reader(
    rx: Weak<AsyncRwLock<QueryRx>>,
    query_path: &str,
) -> Result<RecordReader, HttpError> {
    let rc = rx
        .upgrade()
        .map_err(|_| bad_request!("Query '{}' was closed", query_path))?;
    let mut rx = rc.write().await;
    if let Some(reader) = rx.recv().await {
        reader.map_err(|e| HttpError::from(e))
    } else {
        Err(bad_request!("Query'{}' was closed: broken channel", query_path).into())
    }
}

struct ReaderWrapper {
    reader: RecordReader,
    empty_body: bool,
}

/// A wrapper around a `RecordReader` that implements `Stream` with RwLock
impl Stream for ReaderWrapper {
    type Item = Result<Bytes, HttpError>;

    fn poll_next(
        mut self: Pin<&mut ReaderWrapper>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.empty_body {
            return Poll::Ready(None);
        }

        let pinned_future = pin!(self.reader.read());
        if let Poll::Ready(data) = pinned_future.poll(cx) {
            match data {
                Some(Ok(chunk)) => Poll::Ready(Some(Ok(chunk))),
                Some(Err(e)) => Poll::Ready(Some(Err(HttpError::from(e)))),
                None => Poll::Ready(None),
            }
        } else {
            Poll::Pending
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::body::to_bytes;

    use crate::api::entry::tests::query;
    use crate::api::tests::{components, headers, path_to_entry_1};

    use reduct_base::error::ErrorCode;
    use reduct_base::error::ErrorCode::NotFound;
    use rstest::*;

    #[rstest]
    #[case("GET", "Hey!!!")]
    #[case("HEAD", "")]
    #[tokio::test]
    async fn test_single_read_ts(
        #[future] components: Arc<Components>,
        path_to_entry_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
        #[case] method: String,
        #[case] body: String,
    ) {
        let components = components.await;
        let response = read_record(
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
            to_bytes(response.into_body(), usize::MAX).await.unwrap(),
            Bytes::from(body)
        );
    }

    #[rstest]
    #[case("GET", "Hey!!!")]
    #[case("HEAD", "")]
    #[tokio::test]
    async fn test_single_read_query(
        #[future] components: Arc<Components>,
        path_to_entry_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
        #[case] method: String,
        #[case] body: String,
    ) {
        let components = components.await;
        let query_id = query(&path_to_entry_1, Arc::clone(&components)).await;
        let response = read_record(
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
            to_bytes(response.into_body(), usize::MAX).await.unwrap(),
            Bytes::from(body)
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_single_read_bucket_not_found(
        #[future] components: Arc<Components>,
        headers: HeaderMap,
    ) {
        let components = components.await;
        let path = Path(HashMap::from_iter(vec![
            ("bucket_name".to_string(), "XXX".to_string()),
            ("entry_name".to_string(), "entru-1".to_string()),
        ]));

        let err = read_record(
            State(Arc::clone(&components)),
            path,
            Query(HashMap::from_iter(vec![(
                "ts".to_string(),
                "0".to_string(),
            )])),
            headers,
            MethodExtractor::new("GET"),
        )
        .await
        .err()
        .unwrap();

        assert_eq!(err, HttpError::new(NotFound, "Bucket 'XXX' is not found"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_single_read_ts_not_found(
        #[future] components: Arc<Components>,
        path_to_entry_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let components = components.await;
        let err = read_record(
            State(Arc::clone(&components)),
            path_to_entry_1,
            Query(HashMap::from_iter(vec![(
                "ts".to_string(),
                "1".to_string(),
            )])),
            headers,
            MethodExtractor::new("GET"),
        )
        .await
        .err()
        .unwrap();

        assert_eq!(err, HttpError::new(NotFound, "No record with timestamp 1"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_single_read_bad_ts(
        #[future] components: Arc<Components>,
        path_to_entry_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let components = components.await;
        let err = read_record(
            State(Arc::clone(&components)),
            path_to_entry_1,
            Query(HashMap::from_iter(vec![(
                "ts".to_string(),
                "bad".to_string(),
            )])),
            headers,
            MethodExtractor::new("GET"),
        )
        .await
        .err()
        .unwrap();

        assert_eq!(
            err,
            HttpError::new(
                ErrorCode::UnprocessableEntity,
                "'ts' must be an unix timestamp in microseconds",
            )
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_single_read_query_not_found(
        #[future] components: Arc<Components>,
        path_to_entry_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let components = components.await;
        let err = read_record(
            State(Arc::clone(&components)),
            path_to_entry_1,
            Query(HashMap::from_iter(vec![("q".to_string(), "1".to_string())])),
            headers,
            MethodExtractor::new("GET"),
        )
        .await
        .err()
        .unwrap();

        assert_eq!(err, HttpError::new(NotFound, "Query 1 not found and it might have expired. Check TTL in your query request. Default value 60 sec."));
    }

    mod next_record_reader {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_next_record_reader_err() {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let rx = Arc::new(AsyncRwLock::new(rx));
            drop(tx);
            assert_eq!(
                next_record_reader(rx.into(), "test").await.err().unwrap(),
                HttpError::new(ErrorCode::BadRequest, "Query 'test' was closed")
            );
        }
    }

    mod steam_wrapper {
        use super::*;
        use crate::storage::proto::Record;
        use prost_wkt_types::Timestamp;

        #[rstest]
        fn test_size_hint() {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);

            let wrapper = ReaderWrapper {
                reader: RecordReader::form_record_with_rx(
                    rx,
                    Record {
                        timestamp: Some(Timestamp::default()),
                        begin: 0,
                        end: 0,
                        state: 0,
                        labels: vec![],
                        content_type: "".to_string(),
                    },
                ),
                empty_body: false,
            };
            assert_eq!(wrapper.size_hint(), (0, None));
        }
    }
}
