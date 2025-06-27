// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum_extra::headers::HeaderMap;
use reduct_base::io::RecordMeta;
use reduct_base::Labels;

use crate::api::entry::common::parse_timestamp_from_query;
use crate::api::middleware::check_permissions;
use crate::api::{Components, ErrorCode, HttpError};
use crate::auth::policy::WriteAccessPolicy;
use crate::replication::{Transaction, TransactionNotification};
use crate::storage::entry::update_labels::UpdateLabels;

// PATCH /:bucket/:entry?ts=<number>
pub(crate) async fn update_record(
    State(components): State<Arc<Components>>,
    headers: HeaderMap,
    Path(path): Path<HashMap<String, String>>,
    Query(params): Query<HashMap<String, String>>,
    _: Body,
) -> Result<(), HttpError> {
    let bucket = path.get("bucket_name").unwrap();
    check_permissions(
        &components,
        &headers,
        WriteAccessPolicy {
            bucket: bucket.clone(),
        },
    )
    .await?;

    let ts = parse_timestamp_from_query(&params)?;

    let mut labels_to_update = Labels::new();
    let mut labels_to_remove = HashSet::new();
    for (k, v) in headers.iter() {
        if k.as_str().starts_with("x-reduct-label-") {
            let key = k.as_str()[15..].to_string();
            let value = match v.to_str() {
                Ok(value) => value.to_string(),
                Err(_) => {
                    return Err(HttpError::new(
                        ErrorCode::UnprocessableEntity,
                        &format!("Label values for {} must be valid UTF-8 strings", k),
                    ));
                }
            };

            if value.is_empty() {
                labels_to_remove.insert(key);
            } else {
                labels_to_update.insert(key, value);
            }
        }
    }

    let entry_name = path.get("entry_name").unwrap();
    let batched_result = components
        .storage
        .get_bucket(bucket)?
        .upgrade()?
        .get_entry(entry_name)?
        .upgrade()?
        .update_labels(vec![UpdateLabels {
            time: ts,
            update: labels_to_update,
            remove: labels_to_remove,
        }])
        .await?;

    components
        .replication_repo
        .write()
        .await
        .notify(TransactionNotification {
            bucket: bucket.clone(),
            entry: entry_name.clone(),
            meta: RecordMeta::builder()
                .timestamp(ts)
                .labels(batched_result.get(&ts).unwrap().clone()?.clone())
                .build(),
            event: Transaction::UpdateRecord(ts),
        })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::api::tests::{components, empty_body, path_to_entry_1};

    use axum_extra::headers::{Authorization, HeaderMapExt};
    use reduct_base::io::ReadRecord;
    use rstest::*;

    use super::*;

    #[rstest]
    #[tokio::test]
    async fn test_update_with_label_ok(
        #[future] components: Arc<Components>,
        headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] empty_body: Body,
    ) {
        let components = components.await;

        update_record(
            State(Arc::clone(&components)),
            headers,
            path_to_entry_1,
            Query(HashMap::from_iter(vec![(
                "ts".to_string(),
                "0".to_string(),
            )])),
            empty_body.await,
        )
        .await
        .unwrap();

        let record = components
            .storage
            .get_bucket("bucket-1")
            .unwrap()
            .upgrade_and_unwrap()
            .get_entry("entry-1")
            .unwrap()
            .upgrade_and_unwrap()
            .begin_read(0)
            .await
            .unwrap();

        assert_eq!(record.meta().labels().len(), 2);
        assert_eq!(&record.meta().labels()["x"], "z",);
        assert_eq!(&record.meta().labels()["1"], "2",);

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
    async fn test_update_bucket_not_found(
        #[future] components: Arc<Components>,
        headers: HeaderMap,
        #[future] empty_body: Body,
    ) {
        let components = components.await;
        let path = Path(HashMap::from_iter(vec![
            ("bucket_name".to_string(), "XXX".to_string()),
            ("entry_name".to_string(), "entry-1".to_string()),
        ]));
        let err = update_record(
            State(Arc::clone(&components)),
            headers,
            path,
            Query(HashMap::from_iter(vec![(
                "ts".to_string(),
                "1".to_string(),
            )])),
            empty_body.await,
        )
        .await
        .err()
        .unwrap();

        assert_eq!(
            err,
            HttpError::new(ErrorCode::NotFound, "Bucket 'XXX' is not found")
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_no_label_to_delete(
        #[future] components: Arc<Components>,
        mut headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] empty_body: Body,
    ) {
        let components = components.await;

        headers.insert("x-reduct-label-not-exist", "".parse().unwrap());

        let result = update_record(
            State(Arc::clone(&components)),
            headers,
            path_to_entry_1,
            Query(HashMap::from_iter(vec![(
                "ts".to_string(),
                "0".to_string(),
            )])),
            empty_body.await,
        )
        .await;

        assert!(result.is_ok(), "we ignore labels that do not exist");
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_bad_ts(
        #[future] components: Arc<Components>,
        headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] empty_body: Body,
    ) {
        let components = components.await;
        let err = update_record(
            State(Arc::clone(&components)),
            headers,
            path_to_entry_1,
            Query(HashMap::from_iter(vec![(
                "ts".to_string(),
                "bad".to_string(),
            )])),
            empty_body.await,
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

    #[fixture]
    pub fn headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-reduct-label-x", "z".parse().unwrap()); // update
        headers.insert("x-reduct-label-b", "".parse().unwrap()); // remove
        headers.insert("x-reduct-label-1", "2".parse().unwrap()); // add

        headers.typed_insert(Authorization::bearer("init-token").unwrap());

        headers
    }
}
