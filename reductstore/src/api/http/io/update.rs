// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::http::utils::ensure_public_bucket;
use crate::api::http::{HttpError, StateKeeper};
use crate::auth::policy::WriteAccessPolicy;
use crate::replication::{Transaction, TransactionNotification};
use crate::storage::bucket::update_records::UpdateLabelsMulti;
use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;
use reduct_base::batch::v2::{
    make_entries_header, make_error_batched_header, make_start_timestamp_header,
    parse_batched_update_headers, parse_entries, parse_start_timestamp, ENTRIES_HEADER,
    START_TS_HEADER,
};
use reduct_base::error::ReductError;
use reduct_base::io::RecordMeta;
use reduct_base::unprocessable_entity;
use std::collections::HashMap;
use std::sync::Arc;

// PATCH /io/:bucket_name/update
pub(super) async fn update_batched_records(
    State(keeper): State<Arc<StateKeeper>>,
    headers: HeaderMap,
    Path(path): Path<HashMap<String, String>>,
) -> Result<HeaderMap, HttpError> {
    let bucket_name = path.get("bucket_name").unwrap();
    ensure_public_bucket(bucket_name)?;
    let components = keeper
        .get_with_permissions(
            &headers,
            WriteAccessPolicy {
                bucket: bucket_name,
            },
        )
        .await?;

    let updates = parse_batched_update_headers(&headers)?
        .into_iter()
        .map(|header_record| UpdateLabelsMulti {
            entry_name: header_record.entry,
            time: header_record.timestamp,
            update: header_record.update,
            remove: header_record.remove,
        })
        .collect::<Vec<_>>();

    let bucket = components
        .storage
        .get_bucket(bucket_name)
        .await?
        .upgrade()?;
    let result = bucket.update_labels(updates.clone()).await?;

    let mut resp_headers = HeaderMap::new();
    let start_ts = parse_start_timestamp(&headers)?;
    let entries = parse_entries(&headers)?;
    resp_headers.insert(START_TS_HEADER, make_start_timestamp_header(start_ts));
    resp_headers.insert(ENTRIES_HEADER, make_entries_header(&entries));

    for update in updates {
        if let Some(entry_results) = result.get(&update.entry_name) {
            if let Some(res) = entry_results.get(&update.time) {
                match res {
                    Ok(labels) => {
                        let mut replication_repo = components.replication_repo.write().await?;
                        replication_repo
                            .notify(TransactionNotification {
                                bucket: bucket_name.clone(),
                                entry: update.entry_name.clone(),
                                meta: RecordMeta::builder()
                                    .timestamp(update.time)
                                    .labels(labels.clone())
                                    .build(),
                                event: Transaction::UpdateRecord(update.time),
                            })
                            .await?;
                    }
                    Err(err) => {
                        let entry_index = entries
                            .iter()
                            .position(|entry| entry == &update.entry_name)
                            .ok_or_else(|| {
                                HttpError::from(unprocessable_entity!(
                                    "Entry '{}' is missing in x-reduct-entries",
                                    update.entry_name
                                ))
                            })?;
                        let (name, value) =
                            make_error_batched_header(entry_index, update.time - start_ts, err);
                        resp_headers.insert(name, value);
                    }
                }
            }
        }
    }

    Ok(resp_headers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::tests::{headers, keeper, path_to_bucket_1};
    use axum::extract::Path;
    use axum_extra::headers::HeaderValue;
    use bytes::Bytes;
    use reduct_base::batch::v2::encode_entry_name;
    use reduct_base::error::ErrorCode;
    use reduct_base::io::ReadRecord;
    use reduct_base::Labels;
    use rstest::rstest;

    async fn write_record_with_labels(
        bucket: &Arc<crate::storage::bucket::Bucket>,
        entry: &str,
        ts: u64,
        labels: Labels,
    ) {
        let mut writer = bucket
            .begin_write(entry, ts, 1, "text/plain".to_string(), labels)
            .await
            .unwrap();
        writer.send(Ok(Some(Bytes::from("a")))).await.unwrap();
        writer.send(Ok(None)).await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn updates_labels_across_entries(
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

        write_record_with_labels(
            &bucket,
            "entry-1",
            1000,
            Labels::from_iter(vec![("a".into(), "1".into()), ("b".into(), "2".into())]),
        )
        .await;
        write_record_with_labels(
            &bucket,
            "entry-2",
            1010,
            Labels::from_iter(vec![("a".into(), "old".into())]),
        )
        .await;

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
        headers.insert("x-reduct-labels", HeaderValue::from_static("a,b"));
        headers.insert(
            axum::http::HeaderName::from_static("x-reduct-0-0"),
            HeaderValue::from_static("0,,0=one,1=,c=3"),
        );
        headers.insert(
            axum::http::HeaderName::from_static("x-reduct-1-10"),
            HeaderValue::from_static("0,,0=two"),
        );

        let resp_headers = update_batched_records(State(keeper.clone()), headers, path_to_bucket_1)
            .await
            .unwrap();
        assert_eq!(resp_headers.len(), 2);
        assert_eq!(
            resp_headers[ENTRIES_HEADER].to_str().unwrap(),
            "entry-1,entry-2"
        );
        assert_eq!(resp_headers[START_TS_HEADER].to_str().unwrap(), "1000");

        let record1 = bucket.begin_read("entry-1", 1000).await.unwrap();
        assert_eq!(
            record1.meta().labels(),
            &Labels::from_iter(vec![("a".into(), "one".into()), ("c".into(), "3".into())])
        );

        let record2 = bucket.begin_read("entry-2", 1010).await.unwrap();
        assert_eq!(
            record2.meta().labels(),
            &Labels::from_iter(vec![("a".into(), "two".into())])
        );
    }

    #[rstest]
    #[tokio::test]
    async fn returns_error_for_missing_entry(
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
        write_record_with_labels(&bucket, "entry-1", 1000, Labels::new()).await;

        headers.insert(
            "x-reduct-entries",
            HeaderValue::from_str(
                format!(
                    "{},{}",
                    encode_entry_name("entry-1"),
                    encode_entry_name("missing")
                )
                .as_str(),
            )
            .unwrap(),
        );
        headers.insert("x-reduct-start-ts", HeaderValue::from_static("1000"));
        headers.insert(
            axum::http::HeaderName::from_static("x-reduct-0-0"),
            HeaderValue::from_static("0,,a=1"),
        );
        headers.insert(
            axum::http::HeaderName::from_static("x-reduct-1-0"),
            HeaderValue::from_static("0,,a=1"),
        );

        let resp_headers = update_batched_records(State(keeper.clone()), headers, path_to_bucket_1)
            .await
            .unwrap();

        assert_eq!(resp_headers.len(), 3);
        assert_eq!(
            resp_headers[ENTRIES_HEADER].to_str().unwrap(),
            "entry-1,missing"
        );
        assert_eq!(resp_headers[START_TS_HEADER].to_str().unwrap(), "1000");
        let err_value = resp_headers
            .get("x-reduct-error-1-0")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(err_value.contains("404"));
    }

    #[rstest]
    #[tokio::test]
    async fn requires_headers(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
        path_to_bucket_1: Path<HashMap<String, String>>,
    ) {
        let err = update_batched_records(State(keeper.await), headers, path_to_bucket_1)
            .await
            .err()
            .unwrap();

        assert_eq!(err.status(), ErrorCode::UnprocessableEntity);
    }

    #[rstest]
    #[tokio::test]
    async fn updates_labels_with_names_and_quotes(
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

        write_record_with_labels(
            &bucket,
            "entry-1",
            1000,
            Labels::from_iter(vec![
                ("a".into(), "old".into()),
                ("b".into(), "keep".into()),
            ]),
        )
        .await;

        headers.insert(
            "x-reduct-entries",
            HeaderValue::from_str(encode_entry_name("entry-1").as_str()).unwrap(),
        );
        headers.insert("x-reduct-start-ts", HeaderValue::from_static("1000"));
        headers.insert(
            axum::http::HeaderName::from_static("x-reduct-0-0"),
            HeaderValue::from_static("0,,a=\"hello,world\",b="),
        );

        let resp_headers = update_batched_records(State(keeper.clone()), headers, path_to_bucket_1)
            .await
            .unwrap();
        assert_eq!(resp_headers.len(), 2);
        assert_eq!(resp_headers[ENTRIES_HEADER].to_str().unwrap(), "entry-1");
        assert_eq!(resp_headers[START_TS_HEADER].to_str().unwrap(), "1000");

        let record = bucket.begin_read("entry-1", 1000).await.unwrap();
        assert_eq!(
            record.meta().labels(),
            &Labels::from_iter(vec![("a".into(), "hello,world".into())])
        );
    }

    #[rstest]
    #[tokio::test]
    async fn reuses_labels_for_subsequent_records(
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

        write_record_with_labels(&bucket, "entry-1", 1000, Labels::new()).await;
        write_record_with_labels(&bucket, "entry-1", 1001, Labels::new()).await;

        headers.insert(
            "x-reduct-entries",
            HeaderValue::from_str(encode_entry_name("entry-1").as_str()).unwrap(),
        );
        headers.insert("x-reduct-start-ts", HeaderValue::from_static("1000"));
        headers.insert("x-reduct-labels", HeaderValue::from_static("key,remove"));
        headers.insert(
            axum::http::HeaderName::from_static("x-reduct-0-0"),
            HeaderValue::from_static("0,,0=meta-1,1=true"),
        );
        headers.insert(
            axum::http::HeaderName::from_static("x-reduct-0-1"),
            HeaderValue::from_static("0,,0=meta-2"),
        );

        let resp_headers = update_batched_records(State(keeper.clone()), headers, path_to_bucket_1)
            .await
            .unwrap();
        assert_eq!(resp_headers.len(), 2);
        assert_eq!(resp_headers[ENTRIES_HEADER].to_str().unwrap(), "entry-1");
        assert_eq!(resp_headers[START_TS_HEADER].to_str().unwrap(), "1000");

        let record_1 = bucket.begin_read("entry-1", 1000).await.unwrap();
        assert_eq!(
            record_1.meta().labels(),
            &Labels::from_iter(vec![
                ("key".into(), "meta-1".into()),
                ("remove".into(), "true".into()),
            ])
        );

        let record_2 = bucket.begin_read("entry-1", 1001).await.unwrap();
        assert_eq!(
            record_2.meta().labels(),
            &Labels::from_iter(vec![
                ("key".into(), "meta-2".into()),
                ("remove".into(), "true".into()),
            ])
        );
    }
}
