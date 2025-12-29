// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::{HttpError, StateKeeper};
use crate::auth::policy::WriteAccessPolicy;
use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;
use reduct_base::batch::v2::{parse_entries_header, sort_headers_by_entry_and_time};
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;
use std::collections::HashMap;
use std::str::FromStr;
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

    let start_ts = parse_start_timestamp(&headers)?;
    let entries = parse_entries(&headers)?;
    let parsed_headers = sort_headers_by_entry_and_time(&headers)?;

    let mut records_by_entry: HashMap<String, Vec<IndexedTimestamp>> = HashMap::new();
    for (entry_index, delta, _) in parsed_headers {
        let entry_name = entries.get(entry_index).ok_or_else(|| {
            HttpError::from(unprocessable_entity!(
                "Invalid header 'x-reduct-{}-{}': entry index out of range",
                entry_index,
                delta
            ))
        })?;

        records_by_entry
            .entry(entry_name.to_string())
            .or_default()
            .push(IndexedTimestamp {
                entry_index,
                delta,
                timestamp: start_ts + delta,
            });
    }

    let bucket = components.storage.get_bucket(bucket_name)?.upgrade()?;
    let mut resp_headers = HeaderMap::new();

    for (entry_name, records) in records_by_entry {
        match bucket.get_entry(&entry_name) {
            Ok(entry) => {
                let entry = entry.upgrade()?;
                let timestamps = records.iter().map(|record| record.timestamp).collect();
                let errors = entry.remove_records(timestamps).await?;

                for (timestamp, err) in errors {
                    if let Some(record) =
                        records.iter().find(|record| record.timestamp == timestamp)
                    {
                        err_to_batched_header(
                            &mut resp_headers,
                            record.entry_index,
                            record.delta,
                            &err,
                        );
                    }
                }
            }
            Err(err) => {
                for record in records {
                    err_to_batched_header(
                        &mut resp_headers,
                        record.entry_index,
                        record.delta,
                        &err,
                    );
                }
            }
        }
    }

    Ok(resp_headers)
}

fn err_to_batched_header(
    headers: &mut HeaderMap,
    entry_index: usize,
    delta: u64,
    err: &ReductError,
) {
    let name = format!("x-reduct-error-{}-{}", entry_index, delta);
    let value = format!("{},{}", err.status(), err.message());

    headers.insert(
        axum::http::HeaderName::from_str(&name).unwrap(),
        axum::http::HeaderValue::from_str(&value).unwrap(),
    );
}

fn parse_start_timestamp(headers: &HeaderMap) -> Result<u64, HttpError> {
    headers
        .get("x-reduct-start-ts")
        .ok_or(unprocessable_entity!(
            "x-reduct-start-ts header is required"
        ))?
        .to_str()
        .map_err(|_| HttpError::from(unprocessable_entity!("Invalid x-reduct-start-ts header")))?
        .parse::<u64>()
        .map_err(|_| HttpError::from(unprocessable_entity!("Invalid x-reduct-start-ts header")))
}

fn parse_entries(headers: &HeaderMap) -> Result<Vec<String>, HttpError> {
    headers
        .get("x-reduct-entries")
        .ok_or(unprocessable_entity!("x-reduct-entries header is required"))
        .and_then(parse_entries_header)
        .map_err(HttpError::from)
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
    use reduct_base::io::ReadRecord;
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

        assert!(resp_headers.is_empty());
        assert!(bucket.begin_read("entry-1", 1000).await.is_err());
        assert!(bucket.begin_read("entry-2", 1010).await.is_err());
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
