// Copyright 2025-2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::entry::MethodExtractor;
use crate::api::utils::ReadersWrapper;
use crate::api::{ErrorCode, HttpError, StateKeeper};
use crate::auth::policy::ReadAccessPolicy;
use crate::ext::ext_repository::BoxedManageExtensions;
use crate::storage::bucket::Bucket;
use crate::storage::query::QueryRx;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http;
use axum::response::IntoResponse;
use axum_extra::headers::HeaderMap;
use log::debug;
use reduct_base::batch::v2::{
    make_batched_header_name, make_entries_header, make_record_header_value, LabelIndex,
    ENTRIES_HEADER, LABELS_HEADER, QUERY_ID_HEADER, START_TS_HEADER,
};
use reduct_base::error::ReductError;
use reduct_base::io::BoxedReadRecord;
use reduct_base::{no_content, unprocessable_entity};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::time::timeout;

// GET /io/:bucket/read (query id provided in header)
pub(super) async fn read_batched_records(
    State(keeper): State<Arc<StateKeeper>>,
    headers: HeaderMap,
    Path(path): Path<HashMap<String, String>>,
    method: MethodExtractor,
) -> Result<impl IntoResponse, HttpError> {
    let bucket_name = path.get("bucket_name").unwrap();
    let components = keeper
        .get_with_permissions(
            &headers,
            ReadAccessPolicy {
                bucket: bucket_name,
            },
        )
        .await?;

    let query_id = parse_query_id(&headers)?;

    fetch_and_response_batched_records(
        components
            .storage
            .get_bucket(bucket_name)
            .await?
            .upgrade()?,
        query_id,
        method.name() == "HEAD",
        &components.ext_repo,
    )
    .await
}

struct BatchedRecord {
    entry_index: usize,
    timestamp: u64,
    header_value: http::HeaderValue,
    reader: BoxedReadRecord,
}

#[derive(Clone)]
struct PrevMeta {
    content_type: String,
    labels: reduct_base::Labels,
}

fn calculate_header_size(records: &[BatchedRecord], start_ts: u64) -> usize {
    records
        .iter()
        .map(|record| {
            let name = make_batched_header_name(record.entry_index, record.timestamp - start_ts);
            name.as_str().len() + record.header_value.to_str().unwrap().len() + 2
        })
        .sum()
}

fn parse_query_id(headers: &HeaderMap) -> Result<u64, HttpError> {
    let value = headers.get(QUERY_ID_HEADER).ok_or_else(|| {
        HttpError::from(unprocessable_entity!(
            "{} header is required for batched reads",
            QUERY_ID_HEADER
        ))
    })?;

    let value = value.to_str().map_err(|_| {
        HttpError::new(
            ErrorCode::UnprocessableEntity,
            "Query id header must be valid UTF-8",
        )
    })?;

    value
        .parse::<u64>()
        .map_err(|_| HttpError::new(ErrorCode::UnprocessableEntity, "Invalid query id"))
}

async fn fetch_and_response_batched_records(
    bucket: Arc<Bucket>,
    query_id: u64,
    empty_body: bool,
    ext_repository: &BoxedManageExtensions,
) -> Result<impl IntoResponse, HttpError> {
    // Acquire the query receiver + IO limits for this bucket.
    let (rx, io_settings) = bucket.get_query_receiver(query_id).await?;

    // Batch accumulation state: metadata, per-entry ordering, and size accounting.
    let mut entries: Vec<String> = Vec::new();
    let mut entry_indices: HashMap<String, usize> = HashMap::new();
    let mut records = Vec::new();
    let mut prev_meta: HashMap<String, PrevMeta> = HashMap::new();
    let mut label_index = LabelIndex::default();

    let mut header_size = 0usize;
    let mut body_size = 0u64;
    let mut start_ts: Option<u64> = None;
    let mut last = false;

    let bucket_name = bucket.name().to_string();
    let start_time = Instant::now();
    loop {
        // Fetch the next chunk of readers (with timeout) from the query stream.
        let batch_of_readers = match next_record_readers(
            query_id,
            rx.upgrade()?,
            &format!("{}/{}", bucket_name, query_id),
            io_settings.batch_records_timeout,
            ext_repository,
        )
        .await
        {
            Some(value) => value,
            None => {
                // Check timeout even when no records received to avoid infinite loop on continuous queries
                if start_time.elapsed() > io_settings.batch_timeout || !records.is_empty() {
                    break;
                }
                continue;
            }
        };

        for reader in batch_of_readers {
            match reader {
                Ok(reader) => {
                    // Update header metadata, entry index mapping, and size counters.
                    let meta = reader.meta().clone();
                    start_ts =
                        Some(start_ts.map_or(meta.timestamp(), |curr| curr.min(meta.timestamp())));
                    let entry_index = *entry_indices
                        .entry(meta.entry_name().to_string())
                        .or_insert_with(|| {
                            entries.push(meta.entry_name().to_string());
                            entries.len() - 1
                        });

                    let prev = prev_meta.get(meta.entry_name());
                    let header_value = make_record_header_value(
                        &meta,
                        prev.map(|p| p.content_type.as_str()),
                        prev.map(|p| &p.labels),
                        &mut label_index,
                    );
                    body_size += meta.content_length();

                    records.push(BatchedRecord {
                        entry_index,
                        timestamp: meta.timestamp(),
                        header_value,
                        reader,
                    });
                    header_size = calculate_header_size(&records, start_ts.unwrap());

                    prev_meta.insert(
                        meta.entry_name().to_string(),
                        PrevMeta {
                            content_type: meta.content_type().to_string(),
                            labels: meta.labels().clone(),
                        },
                    );
                }
                Err(err) => {
                    // Propagate errors unless this is a terminal NoContent after at least one record.
                    if records.is_empty() {
                        return Err(HttpError::from(err));
                    } else if err.status() == ErrorCode::NoContent {
                        last = true;
                        break;
                    } else {
                        return Err(HttpError::from(err));
                    }
                }
            }
        }

        if last {
            break;
        }

        // Stop if we hit any batching limits (metadata size, payload size, record count, or timeout).
        if header_size > io_settings.batch_max_metadata_size
            || (!empty_body && body_size > io_settings.batch_max_size)
            || records.len() >= io_settings.batch_max_records
            || start_time.elapsed() > io_settings.batch_timeout
        {
            break;
        }
    }

    if records.is_empty() {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        match bucket.get_query_receiver(query_id).await {
            Err(err) if err.status() == ErrorCode::NotFound => {
                return Err(no_content!("No more records").into())
            }
            _ => { /* query is still alive */ }
        }
    }

    // Align entry indices with the chronological order of the first record per entry
    let mut first_ts_by_entry: HashMap<usize, u64> = HashMap::new();
    for record in &records {
        first_ts_by_entry
            .entry(record.entry_index)
            .and_modify(|ts| *ts = (*ts).min(record.timestamp))
            .or_insert(record.timestamp);
    }

    let mut ordered_entries: Vec<(usize, String)> = entries.into_iter().enumerate().collect();
    ordered_entries.sort_by_key(|(idx, _)| {
        (
            first_ts_by_entry.get(idx).copied().unwrap_or(u64::MAX),
            *idx,
        )
    });

    let mut remapped_indices = HashMap::new();
    let mut reordered_entries = Vec::with_capacity(ordered_entries.len());
    for (new_idx, (old_idx, entry)) in ordered_entries.into_iter().enumerate() {
        remapped_indices.insert(old_idx, new_idx);
        reordered_entries.push(entry);
    }
    entries = reordered_entries;

    for record in &mut records {
        record.entry_index = *remapped_indices.get(&record.entry_index).unwrap();
    }

    // Keep global chronological ordering across all entries.
    records.sort_by_key(|record| (record.timestamp, record.entry_index));

    // Build response headers (entries list, start timestamp, and label index).
    let mut resp_headers = http::HeaderMap::new();
    let start_ts = start_ts.unwrap_or_default();
    resp_headers.insert(ENTRIES_HEADER, make_entries_header(&entries));
    resp_headers.insert(START_TS_HEADER, start_ts.to_string().parse().unwrap());
    if let Some(label_header) = label_index.as_header() {
        resp_headers.insert(LABELS_HEADER, label_header);
    }

    // Add per-record metadata headers and collect readers for the response body.
    let mut readers_only = Vec::with_capacity(records.len());
    for record in records.into_iter() {
        let header_name = make_batched_header_name(record.entry_index, record.timestamp - start_ts);
        resp_headers.insert(header_name, record.header_value);
        readers_only.push(record.reader);
    }

    resp_headers.insert("content-length", body_size.to_string().parse().unwrap());
    resp_headers.insert("content-type", "application/octet-stream".parse().unwrap());
    resp_headers.insert("x-reduct-last", last.to_string().parse().unwrap());

    Ok((
        resp_headers,
        Body::from_stream(ReadersWrapper::new(readers_only, empty_body)),
    ))
}

// This function is used to get the next record from the query receiver
async fn next_record_readers(
    query_id: u64,
    rx: Arc<crate::core::sync::AsyncRwLock<QueryRx>>,
    query_path: &str,
    recv_timeout: std::time::Duration,
    ext_repository: &BoxedManageExtensions,
) -> Option<Vec<Result<BoxedReadRecord, ReductError>>> {
    // Wait for the next batch of records (or timeout), then let extensions process them.
    if let Ok(result) = timeout(
        recv_timeout,
        ext_repository.fetch_and_process_record(query_id, rx),
    )
    .await
    {
        result
    } else {
        debug!("Timeout while waiting for record from query {}", query_path);
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::entry::QueryEntryAxum;
    use crate::api::io::query::query;
    use crate::api::tests::{headers, keeper, path_to_bucket_1};
    use axum::body::to_bytes;
    use axum::extract::Path;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use bytes::Bytes;
    use reduct_base::msg::entry_api::{QueryEntry, QueryInfo, QueryType};
    use rstest::rstest;
    use serde_json::from_slice;

    #[rstest]
    #[tokio::test]
    async fn reads_records_with_query_header(
        #[future] keeper: Arc<StateKeeper>,
        path_to_bucket_1: Path<HashMap<String, String>>,
        mut headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        let bucket = components
            .storage
            .get_bucket("bucket-1")
            .await
            .unwrap()
            .upgrade_and_unwrap();

        for (entry, ts, data) in [("entry-1", 1000u64, "aa"), ("entry-2", 1010u64, "bbb")] {
            let mut writer = bucket
                .begin_write(
                    entry,
                    ts,
                    data.len() as u64,
                    "text/plain".to_string(),
                    Default::default(),
                )
                .await
                .unwrap();
            writer
                .send(Ok(Some(Bytes::from(data.to_string()))))
                .await
                .unwrap();
            writer.send(Ok(None)).await.unwrap();
        }

        let request = QueryEntry {
            query_type: QueryType::Query,
            entries: Some(vec!["entry-1".into(), "entry-2".into()]),
            start: Some(1000),
            ..Default::default()
        };
        let path = Path(path_to_bucket_1.0.clone());
        let response = query(
            State(keeper.clone()),
            headers.clone(),
            path,
            QueryEntryAxum(request),
        )
        .await
        .unwrap()
        .into_response();
        let QueryInfo { id } =
            from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();

        headers.insert(
            QUERY_ID_HEADER,
            http::HeaderValue::from_str(&id.to_string()).unwrap(),
        );

        let response = read_batched_records(
            State(keeper.clone()),
            headers,
            path_to_bucket_1,
            MethodExtractor::new("GET"),
        )
        .await
        .unwrap()
        .into_response();

        let resp_headers = response.headers();
        assert_eq!(resp_headers["x-reduct-entries"], "entry-1,entry-2");
        assert_eq!(resp_headers["x-reduct-start-ts"], "1000");
        assert!(resp_headers.contains_key("x-reduct-0-0"));
        assert!(resp_headers.contains_key("x-reduct-1-10"));
        assert_eq!(resp_headers["content-length"], "5");
        assert_eq!(resp_headers["x-reduct-last"], "true");
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn sorts_entries_by_first_timestamp(
        #[future] keeper: Arc<StateKeeper>,
        path_to_bucket_1: Path<HashMap<String, String>>,
        mut headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        let bucket = components
            .storage
            .get_bucket("bucket-1")
            .await
            .unwrap()
            .upgrade_and_unwrap();

        for (entry, ts, data) in [
            ("late-entry", 1010u64, "aa"),
            ("early-entry", 1000u64, "bbb"),
        ] {
            let mut writer = bucket
                .begin_write(
                    entry,
                    ts,
                    data.len() as u64,
                    "text/plain".to_string(),
                    Default::default(),
                )
                .await
                .unwrap();
            writer
                .send(Ok(Some(Bytes::from(data.to_string()))))
                .await
                .unwrap();
            writer.send(Ok(None)).await.unwrap();
        }

        let request = QueryEntry {
            query_type: QueryType::Query,
            entries: Some(vec!["late-entry".into(), "early-entry".into()]),
            start: Some(1000),
            ..Default::default()
        };
        let path = Path(path_to_bucket_1.0.clone());
        let response = query(
            State(keeper.clone()),
            headers.clone(),
            path,
            QueryEntryAxum(request),
        )
        .await
        .unwrap()
        .into_response();
        let QueryInfo { id } =
            from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();

        headers.insert(
            QUERY_ID_HEADER,
            http::HeaderValue::from_str(&id.to_string()).unwrap(),
        );

        let response = read_batched_records(
            State(keeper.clone()),
            headers,
            path_to_bucket_1,
            MethodExtractor::new("GET"),
        )
        .await
        .unwrap()
        .into_response();

        let resp_headers = response.headers().clone();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();

        assert_eq!(resp_headers["x-reduct-entries"], "early-entry,late-entry");
        assert!(resp_headers["x-reduct-0-0"]
            .to_str()
            .unwrap()
            .starts_with("3,"));
        assert!(resp_headers["x-reduct-1-10"]
            .to_str()
            .unwrap()
            .starts_with("2,"));
        assert_eq!(resp_headers["content-length"], "5");
        assert_eq!(resp_headers["x-reduct-start-ts"], "1000");
        assert_eq!(resp_headers["x-reduct-last"], "true");
        assert_eq!(body, Bytes::from("bbbaa"));
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn sorts_records_globally_by_timestamp(
        #[future] keeper: Arc<StateKeeper>,
        path_to_bucket_1: Path<HashMap<String, String>>,
        mut headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        let bucket = components
            .storage
            .get_bucket("bucket-1")
            .await
            .unwrap()
            .upgrade_and_unwrap();

        for (entry, ts, data) in [
            ("entry-a", 1000u64, "a1"),
            ("entry-a", 1010u64, "a2"),
            ("entry-b", 1005u64, "b1"),
        ] {
            let mut writer = bucket
                .begin_write(
                    entry,
                    ts,
                    data.len() as u64,
                    "text/plain".to_string(),
                    Default::default(),
                )
                .await
                .unwrap();
            writer
                .send(Ok(Some(Bytes::from(data.to_string()))))
                .await
                .unwrap();
            writer.send(Ok(None)).await.unwrap();
        }

        let request = QueryEntry {
            query_type: QueryType::Query,
            entries: Some(vec!["entry-a".into(), "entry-b".into()]),
            start: Some(1000),
            ..Default::default()
        };
        let path = Path(path_to_bucket_1.0.clone());
        let response = query(
            State(keeper.clone()),
            headers.clone(),
            path,
            QueryEntryAxum(request),
        )
        .await
        .unwrap()
        .into_response();
        let QueryInfo { id } =
            from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();

        headers.insert(
            QUERY_ID_HEADER,
            http::HeaderValue::from_str(&id.to_string()).unwrap(),
        );

        let response = read_batched_records(
            State(keeper.clone()),
            headers,
            path_to_bucket_1,
            MethodExtractor::new("GET"),
        )
        .await
        .unwrap()
        .into_response();

        let resp_headers = response.headers().clone();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();

        assert_eq!(resp_headers["x-reduct-entries"], "entry-a,entry-b");
        assert!(resp_headers.contains_key("x-reduct-0-0"));
        assert!(resp_headers.contains_key("x-reduct-1-5"));
        assert!(resp_headers.contains_key("x-reduct-0-10"));
        assert_eq!(resp_headers["x-reduct-start-ts"], "1000");
        assert_eq!(resp_headers["x-reduct-last"], "true");
        assert_eq!(body, Bytes::from("a1b1a2"));
    }

    #[rstest]
    #[tokio::test]
    async fn writes_label_index_header(
        #[future] keeper: Arc<StateKeeper>,
        path_to_bucket_1: Path<HashMap<String, String>>,
        mut headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        let bucket = components
            .storage
            .get_bucket("bucket-1")
            .await
            .unwrap()
            .upgrade_and_unwrap();

        for (entry, ts, data, label) in [
            ("batch-label-entry-1", 1000u64, "aa", "foo"),
            ("batch-label-entry-2", 1010u64, "bbb", "bar"),
        ] {
            let mut writer = bucket
                .begin_write(
                    entry,
                    ts,
                    data.len() as u64,
                    "text/plain".to_string(),
                    [("label".to_string(), label.to_string())]
                        .into_iter()
                        .collect(),
                )
                .await
                .unwrap();
            writer
                .send(Ok(Some(Bytes::from(data.to_string()))))
                .await
                .unwrap();
            writer.send(Ok(None)).await.unwrap();
        }

        let request = QueryEntry {
            query_type: QueryType::Query,
            entries: Some(vec![
                "batch-label-entry-1".into(),
                "batch-label-entry-2".into(),
            ]),
            ..Default::default()
        };
        let path = Path(path_to_bucket_1.0.clone());
        let response = query(
            State(keeper.clone()),
            headers.clone(),
            path,
            QueryEntryAxum(request),
        )
        .await
        .unwrap()
        .into_response();
        let QueryInfo { id } =
            from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();

        headers.insert(
            QUERY_ID_HEADER,
            http::HeaderValue::from_str(&id.to_string()).unwrap(),
        );

        let response = read_batched_records(
            State(keeper.clone()),
            headers,
            path_to_bucket_1,
            MethodExtractor::new("GET"),
        )
        .await
        .unwrap()
        .into_response();

        let resp_headers = response.headers();
        assert_eq!(
            resp_headers["x-reduct-entries"],
            "batch-label-entry-1,batch-label-entry-2"
        );
        let label_header = resp_headers[LABELS_HEADER].to_str().unwrap();
        let label_index = label_header
            .split(',')
            .position(|label| label == "label")
            .expect("label index should be present");

        let expected_first = format!("{}=foo", label_index);
        let expected_second = format!("{}=bar", label_index);

        let entries: Vec<&str> = resp_headers["x-reduct-entries"]
            .to_str()
            .unwrap()
            .split(',')
            .collect();
        let entry1_idx = entries
            .iter()
            .position(|entry| *entry == "batch-label-entry-1")
            .unwrap();
        let entry2_idx = entries
            .iter()
            .position(|entry| *entry == "batch-label-entry-2")
            .unwrap();

        let first_header = resp_headers
            .iter()
            .find(|(name, _)| {
                name.as_str()
                    .starts_with(&format!("x-reduct-{}-", entry1_idx))
            })
            .map(|(_, value)| value.to_str().unwrap().to_string())
            .expect("header for entry-1");
        let second_header = resp_headers
            .iter()
            .find(|(name, _)| {
                name.as_str()
                    .starts_with(&format!("x-reduct-{}-", entry2_idx))
            })
            .map(|(_, value)| value.to_str().unwrap().to_string())
            .expect("header for entry-2");

        assert!(
            first_header.contains(&expected_first),
            "header: {}",
            first_header
        );
        assert!(
            second_header.contains(&expected_second),
            "header: {}",
            second_header
        );
    }

    #[rstest]
    #[tokio::test]
    async fn requires_header(
        #[future] keeper: Arc<StateKeeper>,
        path_to_bucket_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let err = read_batched_records(
            State(keeper.await),
            headers,
            path_to_bucket_1,
            MethodExtractor::new("GET"),
        )
        .await
        .err()
        .unwrap();

        assert_eq!(err.status(), ErrorCode::UnprocessableEntity);
    }

    #[test]
    fn rejects_non_utf_query_id_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            QUERY_ID_HEADER,
            http::HeaderValue::from_bytes(&[0xff, 0xfe]).unwrap(),
        );

        let err = parse_query_id(&headers).err().unwrap();

        assert_eq!(err.status(), ErrorCode::UnprocessableEntity);
        assert_eq!(err.message(), "Query id header must be valid UTF-8");
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn continuous_query_returns_after_timeout(
        #[future] keeper: Arc<StateKeeper>,
        path_to_bucket_1: Path<HashMap<String, String>>,
        mut headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        let bucket = components
            .storage
            .get_bucket("bucket-1")
            .await
            .unwrap()
            .upgrade_and_unwrap();

        // Write one record to entry
        let mut writer = bucket
            .begin_write(
                "continuous-entry",
                1000,
                2,
                "text/plain".to_string(),
                Default::default(),
            )
            .await
            .unwrap();
        writer.send(Ok(Some(Bytes::from("ok")))).await.unwrap();
        writer.send(Ok(None)).await.unwrap();

        // Create a continuous query starting after the record
        let request = QueryEntry {
            query_type: QueryType::Query,
            entries: Some(vec!["continuous-entry".into()]),
            start: Some(1001), // Start after the only record
            continuous: Some(true),
            ..Default::default()
        };
        let path = Path(path_to_bucket_1.0.clone());
        let response = query(
            State(keeper.clone()),
            headers.clone(),
            path,
            QueryEntryAxum(request),
        )
        .await
        .unwrap()
        .into_response();
        let QueryInfo { id } =
            from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();

        headers.insert(
            QUERY_ID_HEADER,
            http::HeaderValue::from_str(&id.to_string()).unwrap(),
        );

        // This should not block indefinitely - it should return after batch_timeout
        let start = std::time::Instant::now();
        let response = read_batched_records(
            State(keeper.clone()),
            headers,
            path_to_bucket_1,
            MethodExtractor::new("GET"),
        )
        .await
        .into_response();

        // Verify it returned in reasonable time (batch_timeout is typically a few seconds)
        assert!(
            start.elapsed() < std::time::Duration::from_secs(30),
            "continuous query should not block indefinitely"
        );

        // Empty response since no records match
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[rstest]
    #[tokio::test]
    async fn returns_no_content_when_no_records(
        #[future] keeper: Arc<StateKeeper>,
        path_to_bucket_1: Path<HashMap<String, String>>,
        mut headers: HeaderMap,
    ) {
        let keeper = keeper.await;

        let request = QueryEntry {
            query_type: QueryType::Query,
            entries: Some(vec!["entry-1".into()]),
            start: Some(1),
            stop: Some(2),
            ..Default::default()
        };
        let path = Path(path_to_bucket_1.0.clone());
        let response = query(
            State(keeper.clone()),
            headers.clone(),
            path,
            QueryEntryAxum(request),
        )
        .await
        .unwrap()
        .into_response();
        let QueryInfo { id } =
            from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();

        headers.insert(
            QUERY_ID_HEADER,
            http::HeaderValue::from_str(&id.to_string()).unwrap(),
        );

        let response = read_batched_records(
            State(keeper.clone()),
            headers,
            path_to_bucket_1,
            MethodExtractor::new("GET"),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }
}
