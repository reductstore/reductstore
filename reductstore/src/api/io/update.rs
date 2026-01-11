// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::{HttpError, StateKeeper};
use crate::auth::policy::WriteAccessPolicy;
use crate::replication::{Transaction, TransactionNotification};
use crate::storage::bucket::update_records::UpdateLabelsMulti;
use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;
use reduct_base::batch::v2::{
    make_error_batched_header, parse_batched_headers, parse_entries, parse_label_delta,
    parse_labels, sort_headers_by_entry_and_time,
};
use reduct_base::error::ReductError;
use reduct_base::io::RecordMeta;
use reduct_base::{unprocessable_entity, Labels};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

struct IndexedUpdate {
    entry_index: usize,
    delta: u64,
    labels: UpdateLabelsMulti,
}

// PATCH /io/:bucket_name/update
pub(super) async fn update_batched_records(
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

    let entries = parse_entries(&headers).map_err(HttpError::from)?;
    let label_names = parse_labels(&headers).map_err(HttpError::from)?;

    // Validate headers and resolve final labels so we mirror protocol v2 behaviour.
    let parsed_headers = parse_batched_headers(&headers).map_err(HttpError::from)?;
    let raw_headers = sort_headers_by_entry_and_time(&headers).map_err(HttpError::from)?;

    if parsed_headers.len() != raw_headers.len() {
        return Err(HttpError::from(unprocessable_entity!(
            "Invalid batched headers"
        )));
    }

    let mut updates = Vec::new();
    for ((entry_index, delta, value), parsed) in
        raw_headers.into_iter().zip(parsed_headers.into_iter())
    {
        let entry_name = entries.get(entry_index).ok_or_else(|| {
            HttpError::from(unprocessable_entity!(
                "Invalid header 'x-reduct-{}-{}': entry index out of range",
                entry_index,
                delta
            ))
        })?;

        let raw_value = value
            .to_str()
            .map_err(|_| HttpError::from(unprocessable_entity!("Invalid batched header")))?;
        let (update, remove) = parse_label_updates(raw_value, label_names.as_ref())?;

        updates.push(IndexedUpdate {
            entry_index,
            delta,
            labels: UpdateLabelsMulti {
                entry_name: entry_name.clone(),
                time: parsed.timestamp,
                update,
                remove,
            },
        });
    }

    let bucket = components
        .storage
        .get_bucket(bucket_name)
        .await?
        .upgrade()?;
    let result = bucket
        .update_labels(
            updates
                .iter()
                .map(|update| UpdateLabelsMulti {
                    entry_name: update.labels.entry_name.clone(),
                    time: update.labels.time,
                    update: update.labels.update.clone(),
                    remove: update.labels.remove.clone(),
                })
                .collect(),
        )
        .await?;

    let mut resp_headers = HeaderMap::new();
    for update in updates {
        if let Some(entry_results) = result.get(&update.labels.entry_name) {
            if let Some(res) = entry_results.get(&update.labels.time) {
                match res {
                    Ok(labels) => {
                        let mut replication_repo = components.replication_repo.write().await?;
                        replication_repo
                            .notify(TransactionNotification {
                                bucket: bucket_name.clone(),
                                entry: update.labels.entry_name.clone(),
                                meta: RecordMeta::builder()
                                    .timestamp(update.labels.time)
                                    .labels(labels.clone())
                                    .build(),
                                event: Transaction::UpdateRecord(update.labels.time),
                            })
                            .await?;
                    }
                    Err(err) => {
                        let (name, value) =
                            make_error_batched_header(update.entry_index, update.delta, err);
                        resp_headers.insert(name, value);
                    }
                }
            }
        }
    }

    Ok(resp_headers)
}

fn parse_label_updates(
    raw_header: &str,
    label_names: Option<&Vec<String>>,
) -> Result<(Labels, HashSet<String>), HttpError> {
    let (content_length_str, rest) = raw_header
        .split_once(',')
        .map(|(len, rest)| (len.trim(), Some(rest)))
        .unwrap_or((raw_header.trim(), None));

    content_length_str
        .parse::<u64>()
        .map_err(|_| HttpError::from(unprocessable_entity!("Invalid batched header")))?;

    let labels_raw = match rest {
        None => None,
        Some(rest) => rest.split_once(',').map(|(_, labels)| labels),
    };

    match labels_raw {
        None => Ok((Labels::new(), HashSet::new())),
        Some(labels_raw) => parse_label_delta(labels_raw, label_names).map_err(HttpError::from),
    }
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
        assert!(resp_headers.is_empty());

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

        assert_eq!(resp_headers.len(), 1);
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
        assert!(resp_headers.is_empty());

        let record = bucket.begin_read("entry-1", 1000).await.unwrap();
        assert_eq!(
            record.meta().labels(),
            &Labels::from_iter(vec![("a".into(), "hello,world".into())])
        );
    }
}
