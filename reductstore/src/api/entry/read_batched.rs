// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::entry::MethodExtractor;
use crate::api::middleware::check_permissions;
use crate::api::{Components, ErrorCode, HttpError};
use crate::auth::policy::ReadAccessPolicy;
use crate::storage::bucket::Bucket;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use axum_extra::headers::{HeaderMap, HeaderName, HeaderValue};
use bytes::Bytes;
use futures_util::Stream;

use crate::storage::entry::RecordReader;
use crate::storage::query::QueryRx;
use log::{debug, info, warn};
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;
use std::collections::HashMap;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::RwLock as AsyncRwLock;
use tokio::time::timeout;

// GET /:bucket/:entry/batch?q=<number>
pub(crate) async fn read_batched_records(
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

    let query_id = match params.get("q") {
        Some(query) => query.parse::<u64>().map_err(|_| {
            HttpError::new(ErrorCode::UnprocessableEntity, "'query' must be a number")
        })?,

        None => {
            return Err(
                unprocessable_entity!("'q' parameter is required for batched reads").into(),
            );
        }
    };

    fetch_and_response_batched_records(
        components.storage.get_bucket(bucket_name)?.upgrade()?,
        entry_name,
        query_id,
        method.name == "HEAD",
    )
    .await
}

async fn fetch_and_response_batched_records(
    bucket: Arc<Bucket>,
    entry_name: &str,
    query_id: u64,
    empty_body: bool,
) -> Result<impl IntoResponse, HttpError> {
    const MAX_HEADER_SIZE: u64 = 6_000; // many http servers have a default limit of 8kb
    const MAX_BODY_SIZE: u64 = 16_000_000; // 16mb just not to be too big
    const MAX_RECORDS: usize = 85; // some clients have a limit of 100 headers

    const BATCH_TIMEOUT: Duration = Duration::from_secs(5); // 5 seconds for the batch
    const RECORD_TIMEOUT: Duration = Duration::from_secs(1); // 1 second for each record

    let make_header = |reader: &RecordReader| {
        let name = HeaderName::from_str(&format!("x-reduct-time-{}", reader.timestamp())).unwrap();
        let mut meta_data = vec![
            reader.content_length().to_string(),
            reader.content_type().to_string(),
        ];

        let mut labels: Vec<String> = reader
            .labels()
            .iter()
            .map(|label| {
                let (k, v) = (&label.name, &label.value);
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
    headers.reserve(MAX_RECORDS + 3);
    let mut readers = Vec::new();
    readers.reserve(MAX_RECORDS);

    let mut last = false;
    let bucket_name = bucket.name().to_string();
    let rx = bucket
        .get_entry(entry_name)?
        .upgrade()?
        .get_query_receiver(query_id)?;

    let start_time = std::time::Instant::now();
    loop {
        let reader = match next_record_reader(
            rx.upgrade()?,
            &format!("{}/{}/{}", bucket_name, entry_name, query_id),
            RECORD_TIMEOUT,
        )
        .await
        {
            Some(value) => value,
            None => break,
        };

        match reader {
            Ok(reader) => {
                {
                    let (name, value) = make_header(&reader);
                    header_size += (name.as_str().len() + value.to_str().unwrap().len() + 2) as u64;
                    body_size += reader.content_length();
                    headers.insert(name, value);
                }
                readers.push(reader);

                if header_size > MAX_HEADER_SIZE
                    || body_size > MAX_BODY_SIZE
                    || readers.len() > MAX_RECORDS
                    || start_time.elapsed() > BATCH_TIMEOUT
                {
                    // This is not correct, because we should check sizes before adding the record
                    // but we can't know the size in advance and after next() we can't go back
                    break;
                }
            }
            Err(err) => {
                if readers.is_empty() {
                    return Err(HttpError::from(err));
                } else {
                    if err.status() == ErrorCode::NoContent {
                        last = true;
                        break;
                    } else {
                        return Err(HttpError::from(err));
                    }
                }
            }
        };
    }

    // TODO: it's workaround
    // check if the query is still alive
    // unfortunately, we can start using a finished query so we need to check if it's still alive again
    if readers.is_empty() {
        tokio::time::sleep(Duration::from_millis(5)).await;
        let _ = bucket
            .get_entry(entry_name)?
            .upgrade()?
            .get_query_receiver(query_id)?
            .upgrade()?;
    }

    headers.insert("content-length", body_size.to_string().parse().unwrap());
    headers.insert("content-type", "application/octet-stream".parse().unwrap());
    headers.insert("x-reduct-last", last.to_string().parse().unwrap());

    Ok((
        headers,
        Body::from_stream(ReadersWrapper {
            readers,
            empty_body,
        }),
    ))
}

// This function is used to get the next record from the query receiver
// created for better testing
async fn next_record_reader(
    rx: Arc<AsyncRwLock<QueryRx>>,
    query_path: &str,
    recv_timeout: Duration,
) -> Option<Result<RecordReader, ReductError>> {
    // we need to wait for the first record
    let result = if let Ok(result) = timeout(recv_timeout, rx.write().await.recv()).await {
        result
    } else {
        debug!("Timeout while waiting for record from query {}", query_path);
        return None;
    };

    let reader = match result {
        Some(reader) => reader,
        None => {
            warn!("Query {} is closed", query_path);
            return None;
        }
    };
    Some(reader)
}

struct ReadersWrapper {
    readers: Vec<RecordReader>,
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

        let mut index = 0;
        while !self.readers.is_empty() {
            if let Poll::Ready(data) = self.readers[index].rx().poll_recv(_cx) {
                match data {
                    Some(Ok(chunk)) => {
                        return Poll::Ready(Some(Ok(chunk)));
                    }
                    Some(Err(err)) => {
                        return Poll::Ready(Some(Err(HttpError::from(err))));
                    }
                    None => index += 1,
                };
            } else {
                return Poll::Pending;
            }
        }
        Poll::Ready(None)
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

    use rstest::*;
    use tokio::time::sleep;

    #[rstest]
    #[case("GET", "Hey!!!")]
    #[case("HEAD", "")]
    #[tokio::test]
    async fn test_batched_read(
        #[future] components: Arc<Components>,
        path_to_entry_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
        #[case] method: String,
        #[case] _body: String,
    ) {
        let components = components.await;
        let entry = components
            .storage
            .get_bucket("bucket-1")
            .unwrap()
            .upgrade_and_unwrap()
            .get_entry("entry-1")
            .unwrap()
            .upgrade_and_unwrap();
        for time in 10..100 {
            let writer = entry
                .begin_write(time, 6, "text/plain".to_string(), HashMap::new())
                .await
                .unwrap();
            writer
                .tx()
                .send(Ok(Some(Bytes::from("Hey!!!"))))
                .await
                .unwrap();
            writer.tx().send(Ok(None)).await.unwrap();
        }

        // let threads finish writing
        sleep(Duration::from_millis(100)).await;

        let query_id = query(&path_to_entry_1, components.clone()).await;
        let query = Query(HashMap::from_iter(vec![(
            "q".to_string(),
            query_id.to_string(),
        )]));

        macro_rules! read_batched_records {
            () => {
                read_batched_records(
                    State(components.clone()),
                    Path(path_to_entry_1.clone()),
                    query.clone(),
                    headers.clone(),
                    MethodExtractor::new(method.as_str()),
                )
                .await
                .into_response()
            };
        }

        let response = read_batched_records!();
        let resp_headers = response.headers();
        assert_eq!(
            resp_headers["x-reduct-time-0"],
            "6,text/plain,b=\"[a,b]\",x=y"
        );
        assert_eq!(resp_headers["content-type"], "application/octet-stream");
        assert_eq!(resp_headers["content-length"], "516");
        assert_eq!(resp_headers["x-reduct-last"], "false");

        if method == "GET" {
            assert_eq!(
                to_bytes(response.into_body(), usize::MAX).await.unwrap(),
                Bytes::from("Hey!!!".repeat(86))
            );
        } else {
            assert_eq!(
                to_bytes(response.into_body(), usize::MAX)
                    .await
                    .unwrap()
                    .len(),
                0
            );
        }

        let response = read_batched_records!();
        let resp_headers = response.headers();
        assert_eq!(resp_headers["content-length"], "30", "{:?}", resp_headers);
        assert_eq!(resp_headers["content-type"], "application/octet-stream");
        assert_eq!(resp_headers["x-reduct-time-98"], "6,text/plain");
        assert_eq!(resp_headers["x-reduct-last"], "true");

        if method == "GET" {
            assert_eq!(
                to_bytes(response.into_body(), usize::MAX).await.unwrap(),
                Bytes::from("Hey!!!".repeat(5))
            );
        } else {
            assert_eq!(
                to_bytes(response.into_body(), usize::MAX)
                    .await
                    .unwrap()
                    .len(),
                0
            );
        }

        let response = read_batched_records!();
        let resp_headers = response.headers();
        println!("{:?}", resp_headers);

        assert_eq!(
            resp_headers["x-reduct-error"],
            format!("Query {} not found and it might have expired. Check TTL in your query request. Default value 60 sec.", query_id)
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_batched_no_entry(
        #[future] components: Arc<Components>,
        path_to_entry_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let components = components.await;
        let query_id = query(&path_to_entry_1, components.clone()).await;

        components
            .storage
            .get_bucket(path_to_entry_1.get("bucket_name").unwrap())
            .unwrap()
            .upgrade()
            .unwrap()
            .remove_entry(path_to_entry_1.get("entry_name").unwrap())
            .await
            .unwrap();

        let err = read_batched_records(
            State(components.clone()),
            path_to_entry_1,
            Query(HashMap::from_iter(vec![(
                "q".to_string(),
                query_id.to_string(),
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
                ErrorCode::NotFound,
                "Entry 'entry-1' not found in bucket 'bucket-1'"
            )
        );
    }

    mod next_record_reader {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_next_record_reader_timeout() {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            let rx = Arc::new(AsyncRwLock::new(rx));
            assert!(
                timeout(
                    Duration::from_secs(1),
                    next_record_reader(rx.clone(), "", Duration::from_millis(10))
                )
                .await
                .unwrap()
                .is_none(),
                "should return None if no record is received after timeout"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_next_record_reader_closed_tx() {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let rx = Arc::new(AsyncRwLock::new(rx));
            drop(tx);
            assert!(
                timeout(
                    Duration::from_secs(1),
                    next_record_reader(rx.clone(), "", Duration::from_millis(0))
                )
                .await
                .unwrap()
                .is_none(),
                "should return None if the query is closed"
            );
        }
    }

    mod stream_wrapper {
        use super::*;

        #[rstest]
        fn test_size_hint() {
            let wrapper = ReadersWrapper {
                readers: vec![],
                empty_body: false,
            };
            assert_eq!(wrapper.size_hint(), (0, None));
        }
    }
}
