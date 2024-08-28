// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::entry::{check_and_extract_ts_or_query_id, MethodExtractor};
use crate::api::middleware::check_permissions;
use crate::api::Components;
use crate::api::HttpError;
use crate::auth::policy::ReadAccessPolicy;
use crate::storage::bucket::{Bucket, RecordReader};

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use axum_extra::headers::{HeaderMap, HeaderName};
use bytes::Bytes;
use futures_util::Stream;

use crate::storage::query::QueryRx;
use reduct_base::error::ErrorCode;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

// GET /:bucket/:entry?ts=<number>|q=<number>|
pub(crate) async fn read_single_record(
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
        headers,
        ReadAccessPolicy {
            bucket: bucket_name.clone(),
        },
    )
    .await?;

    let storage = components.storage.read().await;
    let (query_id, ts) =
        check_and_extract_ts_or_query_id(&storage, params, bucket_name, entry_name).await?;
    drop(storage); // Release the lock

    let mut storage = components.storage.write().await;
    let bucket = storage.get_bucket_mut(bucket_name)?;
    fetch_and_response_single_record(bucket, entry_name, ts, query_id, method.name() == "HEAD")
        .await
}

async fn fetch_and_response_single_record(
    bucket: &mut Bucket,
    entry_name: &str,
    ts: Option<u64>,
    query_id: Option<u64>,
    empty_body: bool,
) -> Result<impl IntoResponse, HttpError> {
    let make_headers = |record_reader: &RecordReader| {
        let mut headers = HeaderMap::new();

        for label in record_reader.labels() {
            let (k, v) = (&label.name, &label.value);
            headers.insert(
                format!("x-reduct-label-{}", k)
                    .parse::<HeaderName>()
                    .unwrap(),
                v.parse().unwrap(),
            );
        }

        headers.insert(
            "content-type",
            record_reader.content_type().to_string().parse().unwrap(),
        );
        headers.insert(
            "content-length",
            record_reader.content_length().to_string().parse().unwrap(),
        );
        headers.insert(
            "x-reduct-time",
            record_reader.timestamp().to_string().parse().unwrap(),
        );
        headers.insert(
            "x-reduct-last",
            u8::from(record_reader.last()).to_string().parse().unwrap(),
        );
        headers
    };

    let reader = if let Some(ts) = ts {
        bucket.begin_read(entry_name, ts).await?
    } else {
        let query_id = query_id.unwrap();
        let bucket_name = bucket.name().to_string();
        let rx = bucket
            .get_entry_mut(entry_name)?
            .get_query_receiver(query_id)
            .await?;
        let query_path = format!("{}/{}/{}", bucket_name, entry_name, query_id);
        next_record_reader(rx, &query_path).await?
    };

    let headers = make_headers(&reader);

    Ok((
        headers,
        Body::from_stream(ReaderWrapper { reader, empty_body }),
    ))
}

async fn next_record_reader(rx: &mut QueryRx, query_path: &str) -> Result<RecordReader, HttpError> {
    if let Some(reader) = rx.recv().await {
        reader.map_err(|e| HttpError::from(e))
    } else {
        Err(HttpError::new(
            ErrorCode::BadRequest,
            &format!("Query {} closed before the response was sent", query_path),
        ))
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

        if let Poll::Ready(data) = self.reader.rx().poll_recv(cx) {
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

    use crate::api::tests::{components, headers, path_to_entry_1};
    use crate::storage::query::base::QueryOptions;
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
        let response = read_single_record(
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
        let query_id = {
            components
                .storage
                .write()
                .await
                .get_bucket_mut(path_to_entry_1.get("bucket_name").unwrap())
                .unwrap()
                .get_entry_mut(path_to_entry_1.get("entry_name").unwrap())
                .unwrap()
                .query(0, u64::MAX, QueryOptions::default())
                .unwrap()
        };

        let response = read_single_record(
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

        let err = read_single_record(
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
        let err = read_single_record(
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
        let err = read_single_record(
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
        let err = read_single_record(
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
            let (tx, mut rx) = tokio::sync::mpsc::channel(1);
            drop(tx);
            assert_eq!(
                next_record_reader(&mut rx, "test").await.err().unwrap(),
                HttpError::new(
                    ErrorCode::BadRequest,
                    "Query test closed before the response was sent"
                )
            );
        }
    }

    mod stram_wrapper {
        use super::*;
        use crate::storage::proto::Record;

        #[rstest]
        fn test_size_hint() {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);

            let wrapper = ReaderWrapper {
                reader: RecordReader::new(
                    rx,
                    Record {
                        timestamp: None,
                        begin: 0,
                        end: 0,
                        state: 0,
                        labels: vec![],
                        content_type: "".to_string(),
                    },
                    false,
                ),
                empty_body: false,
            };
            assert_eq!(wrapper.size_hint(), (0, None));
        }
    }
}
