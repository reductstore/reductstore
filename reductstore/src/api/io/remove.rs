// Copyright 2025-2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::{HttpError, StateKeeper};
use crate::auth::policy::WriteAccessPolicy;
use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;
use reduct_base::batch::v2::{
    make_entries_header, make_error_batched_header, make_start_timestamp_header, parse_entries,
    parse_start_timestamp, sort_headers_by_entry_and_time, ENTRIES_HEADER, START_TS_HEADER,
};
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
struct IndexedTimestamp {
    entry_index: usize,
    delta: u64,
    timestamp: u64,
}

// DELETE /io/:bucket_name/remove
pub(super) async fn remove_batched_records(
    State(keeper): State<Arc<StateKeeper>>,
    headers: HeaderMap,
    Path(path): Path<HashMap<String, String>>,
) -> Result<HeaderMap, HttpError> {
    let bucket_name = path.get("bucket_name").unwrap();
    let components = keeper
        .get_with_permissions(
            &headers,
            WriteAccessPolicy {
                bucket: bucket_name,
            },
        )
        .await?;

    let start_ts = parse_start_timestamp(&headers).map_err(HttpError::from)?;
    let entries = parse_entries(&headers).map_err(HttpError::from)?;
    let parsed_headers = sort_headers_by_entry_and_time(&headers)?;

    let mut records_by_entry: HashMap<String, Vec<IndexedTimestamp>> = HashMap::new();
    let mut records_by_timestamp: HashMap<u64, Vec<IndexedTimestamp>> = HashMap::new();
    for (entry_index, delta, _) in parsed_headers {
        let entry_name = entries.get(entry_index).ok_or_else(|| {
            HttpError::from(unprocessable_entity!(
                "Invalid header 'x-reduct-{}-{}': entry index out of range",
                entry_index,
                delta
            ))
        })?;

        let record = IndexedTimestamp {
            entry_index,
            delta,
            timestamp: start_ts + delta,
        };

        records_by_timestamp
            .entry(record.timestamp)
            .or_default()
            .push(record.clone());

        records_by_entry
            .entry(entry_name.to_string())
            .or_default()
            .push(record);
    }

    let bucket = components
        .storage
        .get_bucket(bucket_name)
        .await?
        .upgrade()?;

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(START_TS_HEADER, make_start_timestamp_header(start_ts));
    resp_headers.insert(ENTRIES_HEADER, make_entries_header(&entries));

    if !records_by_entry.is_empty() {
        let mut record_ids: HashMap<String, Vec<u64>> = HashMap::new();
        for (entry_name, records) in &records_by_entry {
            record_ids.insert(
                entry_name.clone(),
                records.iter().map(|record| record.timestamp).collect(),
            );
        }

        let errors = bucket.clone().remove_records(record_ids).await?;
        for (timestamp, err) in errors {
            if let Some(records) = records_by_timestamp.get(&timestamp) {
                for record in records {
                    let (name, value) =
                        make_error_batched_header(record.entry_index, record.delta, &err);
                    resp_headers.insert(name, value);
                }
            }
        }
    }

    Ok(resp_headers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tests::{headers, keeper, path_to_bucket_1};
    use axum::extract::Path;
    use axum_extra::headers::HeaderValue;
    use bytes::Bytes;
    use reduct_base::batch::v2::encode_entry_name;
    use reduct_base::error::ErrorCode;
    use rstest::rstest;

    async fn write_record(bucket: &Arc<crate::storage::bucket::Bucket>, entry: &str, ts: u64) {
        let mut writer = bucket
            .begin_write(entry, ts, 1, "text/plain".to_string(), Default::default())
            .await
            .unwrap();
        writer.send(Ok(Some(Bytes::from("a")))).await.unwrap();
        writer.send(Ok(None)).await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn removes_records_across_entries(
        #[future] keeper: Arc<StateKeeper>,
        mut headers: HeaderMap,
        path_to_bucket_1: Path<HashMap<String, String>>,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        let bucket = components
            .storage
            .get_bucket("bucket-1")
            .await
            .unwrap()
            .upgrade_and_unwrap();

        write_record(&bucket, "entry-1", 1000).await;
        write_record(&bucket, "entry-2", 1010).await;

        headers.insert(
            "x-reduct-entries",
            HeaderValue::from_str(
                format!(
                    "{},{}",
                    encode_entry_name("entry-1"),
                    encode_entry_name("entry-2")
                )
                .as_str(),
            )
            .unwrap(),
        );
        headers.insert("x-reduct-start-ts", HeaderValue::from_static("1000"));
        headers.insert(
            axum::http::HeaderName::from_static("x-reduct-0-0"),
            HeaderValue::from_static(""),
        );
        headers.insert(
            axum::http::HeaderName::from_static("x-reduct-1-10"),
            HeaderValue::from_static(""),
        );

        let resp_headers = remove_batched_records(State(keeper.clone()), headers, path_to_bucket_1)
            .await
            .unwrap();

        assert_eq!(resp_headers.get(ENTRIES_HEADER).unwrap(), "entry-1,entry-2");
        assert_eq!(resp_headers.get(START_TS_HEADER).unwrap(), "1000");
        assert!(resp_headers.get("x-reduct-error-0-0").is_none());
        assert!(resp_headers.get("x-reduct-error-1-10").is_none());
        assert!(bucket.begin_read("entry-1", 1000).await.is_err());
        assert!(bucket.begin_read("entry-2", 1010).await.is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn rejects_entry_index_out_of_range(
        #[future] keeper: Arc<StateKeeper>,
        mut headers: HeaderMap,
        path_to_bucket_1: Path<HashMap<String, String>>,
    ) {
        let keeper = keeper.await;
        let entries = encode_entry_name("entry-1");
        headers.insert(
            "x-reduct-entries",
            HeaderValue::from_str(entries.as_str()).unwrap(),
        );
        headers.insert("x-reduct-start-ts", HeaderValue::from_static("1000"));
        headers.insert(
            axum::http::HeaderName::from_static("x-reduct-1-0"),
            HeaderValue::from_static(""),
        );

        let err = remove_batched_records(State(keeper.clone()), headers, path_to_bucket_1)
            .await
            .err()
            .unwrap();

        assert_eq!(err.status(), ErrorCode::UnprocessableEntity);
        assert_eq!(
            err.message(),
            "Invalid header 'x-reduct-1-0': entry index out of range"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn reports_missing_entries_in_response_headers(
        #[future] keeper: Arc<StateKeeper>,
        mut headers: HeaderMap,
        path_to_bucket_1: Path<HashMap<String, String>>,
    ) {
        let keeper = keeper.await;
        let entries = encode_entry_name("missing-entry");
        headers.insert(
            "x-reduct-entries",
            HeaderValue::from_str(entries.as_str()).unwrap(),
        );
        headers.insert("x-reduct-start-ts", HeaderValue::from_static("1000"));
        headers.insert(
            axum::http::HeaderName::from_static("x-reduct-0-0"),
            HeaderValue::from_static(""),
        );

        let resp_headers = remove_batched_records(State(keeper.clone()), headers, path_to_bucket_1)
            .await
            .unwrap();

        assert_eq!(
            resp_headers.get("x-reduct-error-0-0").unwrap(),
            "404,Entry 'missing-entry' not found in bucket 'bucket-1'"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn reports_missing_records_in_response_headers(
        #[future] keeper: Arc<StateKeeper>,
        mut headers: HeaderMap,
        path_to_bucket_1: Path<HashMap<String, String>>,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        let bucket = components
            .storage
            .get_bucket("bucket-1")
            .await
            .unwrap()
            .upgrade_and_unwrap();

        write_record(&bucket, "entry-1", 1000).await;

        let entries = encode_entry_name("entry-1");
        headers.insert(
            "x-reduct-entries",
            HeaderValue::from_str(entries.as_str()).unwrap(),
        );
        headers.insert("x-reduct-start-ts", HeaderValue::from_static("1000"));
        headers.insert(
            axum::http::HeaderName::from_static("x-reduct-0-0"),
            HeaderValue::from_static(""),
        );
        headers.insert(
            axum::http::HeaderName::from_static("x-reduct-0-1"),
            HeaderValue::from_static(""),
        );

        let resp_headers = remove_batched_records(State(keeper.clone()), headers, path_to_bucket_1)
            .await
            .unwrap();

        assert_eq!(resp_headers.len(), 3);
        assert_eq!(resp_headers.get(ENTRIES_HEADER).unwrap(), "entry-1");
        assert_eq!(resp_headers.get(START_TS_HEADER).unwrap(), "1000");
        assert_eq!(
            resp_headers.get("x-reduct-error-0-1").unwrap(),
            "404,Record 1001 not found in entry bucket-1/entry-1"
        );
        assert!(bucket.begin_read("entry-1", 1000).await.is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn requires_headers(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
        path_to_bucket_1: Path<HashMap<String, String>>,
    ) {
        let err = remove_batched_records(State(keeper.await), headers, path_to_bucket_1)
            .await
            .err()
            .unwrap();

        assert_eq!(err.status(), ErrorCode::UnprocessableEntity);
    }
}
