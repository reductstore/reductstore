// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;

use reduct_base::batch::{parse_batched_header, sort_headers_by_time};
use reduct_base::Labels;

use crate::api::entry::common::{err_to_batched_header, parse_content_length_from_header};
use crate::api::middleware::check_permissions;
use crate::api::{Components, ErrorCode, HttpError};
use crate::auth::policy::WriteAccessPolicy;
use crate::replication::{Transaction, TransactionNotification};
use crate::storage::entry::update_labels::UpdateLabels;

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
    let mut records_to_update = Vec::new();

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

        records_to_update.push(UpdateLabels {
            time,
            update: labels_to_update,
            remove: labels_to_remove,
        });
    }

    let result = {
        let entry = components
            .storage
            .get_bucket(bucket_name)?
            .upgrade()?
            .get_entry(entry_name)?
            .upgrade()?;
        entry.update_labels(records_to_update).await?
    };

    let mut headers = HeaderMap::new();
    for (time, result) in result {
        match result {
            Err(err) => {
                err_to_batched_header(&mut headers, time, &err);
            }
            Ok(new_labels) => {
                let mut replication_repo = components.replication_repo.write().await;
                replication_repo.notify(TransactionNotification {
                    bucket: bucket_name.clone(),
                    entry: entry_name.clone(),
                    labels: new_labels,
                    event: Transaction::UpdateRecord(time),
                })?;
            }
        };
    }

    Ok(headers.into())
}

#[cfg(test)]
mod tests {
    use crate::api::tests::{components, empty_body, headers, path_to_entry_1};
    use crate::storage::proto::record::Label;
    use axum::response::IntoResponse;
    use axum_extra::headers::HeaderValue;
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
        headers.insert("x-reduct-time-yyy", "10".parse().unwrap());

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
                "Invalid header 'x-reduct-time-yyy': must be an unix timestamp in microseconds",
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

        let bucket = components
            .storage
            .get_bucket("bucket-1")
            .unwrap()
            .upgrade_and_unwrap();

        {
            let reader = bucket
                .get_entry("entry-1")
                .unwrap()
                .upgrade_and_unwrap()
                .begin_read(0)
                .await
                .unwrap();
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
            let writer = components
                .storage
                .get_bucket("bucket-1")
                .unwrap()
                .upgrade_and_unwrap()
                .begin_write("entry-1", 2, 20, "text/plain".to_string(), HashMap::new())
                .await
                .unwrap();
            writer
                .tx()
                .send(Ok(Some(Bytes::from(vec![0; 20]))))
                .await
                .unwrap();
            writer.tx().send(Ok(None)).await.unwrap();
            writer.tx().closed().await;
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
