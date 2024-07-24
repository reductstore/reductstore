// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use std::collections::{BTreeMap, HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::HeaderName;
use axum_extra::headers::{HeaderMap, HeaderValue};

use reduct_base::batch::{parse_batched_header, sort_headers_by_time};
use reduct_base::Labels;

use crate::api::entry::common::parse_content_length_from_header;
use crate::api::middleware::check_permissions;
use crate::api::{Components, ErrorCode, HttpError};
use crate::auth::policy::WriteAccessPolicy;
use crate::replication::{Transaction, TransactionNotification};
use crate::storage::proto::record::Label;

// PATCH /:bucket/:entry/batch
pub(crate) async fn update_batched_records(
    State(components): State<Arc<Components>>,
    headers: HeaderMap,
    Path(path): Path<HashMap<String, String>>,
    _: Body,
) -> Result<HeaderMap, HttpError> {
    let bucket_name = path.get("bucket_name").unwrap();
    check_permissions(
        &components,
        headers.clone(),
        WriteAccessPolicy {
            bucket: bucket_name.clone(),
        },
    )
    .await?;

    let content_size = parse_content_length_from_header(&headers)?;

    // we update only labels, so content-length must be 0
    if content_size > 0 {
        return Err(HttpError::new(
            ErrorCode::UnprocessableEntity,
            "content-length header must be 0",
        ));
    }

    let entry_name = path.get("entry_name").unwrap();
    let record_headers: Vec<_> = sort_headers_by_time(&headers)?;
    let mut error_map = BTreeMap::new();

    for (time, v) in record_headers {
        let header = parse_batched_header(v.to_str().unwrap())?;
        let mut labels_to_update = Labels::new();
        let mut labels_to_remove = HashSet::new();

        for (k, v) in header.labels.iter() {
            if v.is_empty() {
                labels_to_remove.insert(k.clone());
            } else {
                labels_to_update.insert(k.clone(), v.clone());
            }
        }

        let mut storage = components.storage.write().await;
        let entry = storage
            .get_mut_bucket(bucket_name)?
            .get_mut_entry(entry_name)?;
        if let Err(e) = entry
            .update_labels(time, labels_to_update.clone(), labels_to_remove)
            .await
        {
            error_map.insert(time, e);
        } else {
            drop(storage); // drop the lock because we may need to wait for the replication
            let mut replication_repo = components.replication_repo.write().await;
            replication_repo
                .notify(TransactionNotification {
                    bucket: bucket_name.clone(),
                    entry: entry_name.clone(),
                    labels: labels_to_update
                        .iter()
                        .map(|(k, v)| Label {
                            name: k.clone(),
                            value: v.clone(),
                        })
                        .collect(),
                    event: Transaction::UpdateRecord(time),
                })
                .await
                .unwrap();
        }
    }

    let mut headers = HeaderMap::new();
    error_map.iter().for_each(|(time, err)| {
        headers.insert(
            HeaderName::from_str(&format!("x-reduct-error-{}", time)).unwrap(),
            HeaderValue::from_str(&format!("{},{}", err.status(), err.message())).unwrap(),
        );
    });

    Ok(headers.into())
}

#[cfg(test)]
mod tests {
    use crate::api::tests::{components, empty_body, headers, path_to_entry_1};
    use crate::storage::proto::record::Label;
    use axum::response::IntoResponse;
    use bytes::Bytes;
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[tokio::test]
    async fn test_update_record_bad_timestamp(
        #[future] components: Arc<Components>,
        mut headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] empty_body: Body,
    ) {
        headers.insert("content-length", "0".parse().unwrap());
        headers.insert("x-reduct-time-xxx", "10".parse().unwrap());

        let err = update_batched_records(
            State(components.await),
            headers,
            path_to_entry_1,
            empty_body.await,
        )
        .await
        .err()
        .unwrap();

        assert_eq!(
            err,
            HttpError::new(
                ErrorCode::UnprocessableEntity,
                "Invalid header'x-reduct-time-xxx': must be an unix timestamp in microseconds",
            )
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_batched_invalid_header(
        #[future] components: Arc<Components>,
        mut headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] empty_body: Body,
    ) {
        headers.insert("content-length", "0".parse().unwrap());
        headers.insert("x-reduct-time-1", "".parse().unwrap());

        let err = update_batched_records(
            State(components.await),
            headers,
            path_to_entry_1,
            empty_body.await,
        )
        .await
        .err()
        .unwrap();

        assert_eq!(
            err,
            HttpError::new(ErrorCode::UnprocessableEntity, "Invalid batched header")
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_batched_records(
        #[future] components: Arc<Components>,
        mut headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] empty_body: Body,
    ) {
        let components = components.await;
        headers.insert("content-length", "0".parse().unwrap());
        headers.insert("x-reduct-time-0", "0,,x=z,b=,1=2".parse().unwrap());

        let err_map = update_batched_records(
            State(Arc::clone(&components)),
            headers,
            path_to_entry_1,
            empty_body.await,
        )
        .await
        .unwrap();

        let storage = components.storage.read().await;
        let bucket = storage.get_bucket("bucket-1").unwrap();

        {
            let reader = bucket.begin_read("entry-1", 0).await.unwrap();
            assert_eq!(reader.labels().len(), 2);
            assert_eq!(
                reader.labels()[0],
                Label {
                    name: "x".to_string(),
                    value: "z".to_string(),
                }
            );
            assert_eq!(
                reader.labels()[1],
                Label {
                    name: "1".to_string(),
                    value: "2".to_string(),
                }
            );
        }

        assert_eq!(err_map.len(), 0);
        let info = components
            .replication_repo
            .read()
            .await
            .get_info("api-test")
            .await
            .unwrap();
        assert_eq!(info.info.pending_records, 1);
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_batched_records_error(
        #[future] components: Arc<Components>,
        mut headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] empty_body: Body,
    ) {
        let components = components.await;
        {
            let mut storage = components.storage.write().await;
            let tx = storage
                .get_mut_bucket("bucket-1")
                .unwrap()
                .write_record("entry-1", 2, 20, "text/plain".to_string(), HashMap::new())
                .await
                .unwrap();
            tx.send(Ok(Some(Bytes::from(vec![0; 20])))).await.unwrap();
            tx.closed().await;
        }

        headers.insert("content-length", "0".parse().unwrap());
        headers.insert("x-reduct-time-0", "0,,".parse().unwrap());
        headers.insert("x-reduct-time-1", "0,,".parse().unwrap());

        let resp = update_batched_records(
            State(Arc::clone(&components)),
            headers,
            path_to_entry_1,
            empty_body.await,
        )
        .await
        .unwrap()
        .into_response();

        let headers = resp.headers();
        assert_eq!(headers.len(), 1);
        assert_eq!(
            headers.get("x-reduct-error-1").unwrap(),
            &HeaderValue::from_static("404,No record with timestamp 1")
        );
    }
}
