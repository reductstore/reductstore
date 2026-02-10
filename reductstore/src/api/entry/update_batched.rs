// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;

use reduct_base::batch::{parse_batched_header, sort_headers_by_time};
use reduct_base::io::RecordMeta;
use reduct_base::Labels;

use crate::api::entry::common::err_to_batched_header;
use crate::api::HttpError;
use crate::api::StateKeeper;
use crate::auth::policy::WriteAccessPolicy;
use crate::replication::{Transaction, TransactionNotification};
use crate::storage::entry::update_labels::UpdateLabels;

// PATCH /:bucket/:entry/batch

pub(super) async fn update_batched_records(
    State(keeper): State<Arc<StateKeeper>>,
    headers: HeaderMap,
    Path(path): Path<HashMap<String, String>>,
    _: Body,
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
            .get_bucket(bucket_name)
            .await?
            .upgrade()?
            .get_entry(entry_name)
            .await?
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
                let mut replication_repo = components.replication_repo.write().await?;
                replication_repo
                    .notify(TransactionNotification {
                        bucket: bucket_name.clone(),
                        entry: entry_name.clone(),
                        meta: RecordMeta::builder()
                            .timestamp(time)
                            .labels(new_labels)
                            .build(),
                        event: Transaction::UpdateRecord(time),
                    })
                    .await?;
            }
        };
    }

    Ok(headers.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tests::{empty_body, headers, keeper, path_to_entry_1};
    use crate::api::ErrorCode;

    use axum::response::IntoResponse;
    use axum_extra::headers::HeaderValue;
    use bytes::Bytes;
    use reduct_base::io::ReadRecord;
    use rstest::rstest;
    use tokio::time::{sleep, Duration};

    #[rstest]
    #[tokio::test]
    async fn test_update_record_bad_timestamp(
        #[future] keeper: Arc<StateKeeper>,
        mut headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] empty_body: Body,
    ) {
        headers.insert("x-reduct-time-yyy", "10".parse().unwrap());

        let err = update_batched_records(
            State(keeper.await),
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
        #[future] keeper: Arc<StateKeeper>,
        mut headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] empty_body: Body,
    ) {
        headers.insert("x-reduct-time-1", "".parse().unwrap());

        let err = update_batched_records(
            State(keeper.await),
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
        #[future] keeper: Arc<StateKeeper>,
        mut headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] empty_body: Body,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        headers.insert("x-reduct-time-0", "0,,x=z,b=,1=2".parse().unwrap());

        let err_map = update_batched_records(
            State(Arc::clone(&keeper)),
            headers,
            path_to_entry_1,
            empty_body.await,
        )
        .await
        .unwrap();

        let bucket = components
            .storage
            .get_bucket("bucket-1")
            .await
            .unwrap()
            .upgrade_and_unwrap();

        {
            let reader = bucket
                .get_entry("entry-1")
                .await
                .unwrap()
                .upgrade_and_unwrap()
                .begin_read(0)
                .await
                .unwrap();
            assert_eq!(reader.meta().labels().len(), 2);
            assert_eq!(&reader.meta().labels()["x"], "z");
            assert_eq!(&reader.meta().labels()["1"], "2");
        }

        assert_eq!(err_map.len(), 0);
        let info = components
            .replication_repo
            .read()
            .await
            .unwrap()
            .get_info("api-test")
            .await
            .unwrap();
        if info.info.pending_records == 0 {
            sleep(Duration::from_millis(50)).await;
        }
        let info = components
            .replication_repo
            .read()
            .await
            .unwrap()
            .get_info("api-test")
            .await
            .unwrap();
        assert!(info.info.pending_records >= 1);
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_batched_records_error(
        #[future] keeper: Arc<StateKeeper>,
        mut headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] empty_body: Body,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        {
            let mut writer = components
                .storage
                .get_bucket("bucket-1")
                .await
                .unwrap()
                .upgrade_and_unwrap()
                .begin_write("entry-1", 2, 20, "text/plain".to_string(), HashMap::new())
                .await
                .unwrap();
            writer
                .send(Ok(Some(Bytes::from(vec![0; 20]))))
                .await
                .unwrap();
            writer.send(Ok(None)).await.unwrap();
        }

        headers.insert("x-reduct-time-0", "0,,".parse().unwrap());
        headers.insert("x-reduct-time-1", "0,,".parse().unwrap());

        let resp = update_batched_records(
            State(Arc::clone(&keeper)),
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
            &HeaderValue::from_static("404,Record 1 not found in entry bucket-1/entry-1")
        );
    }
}
