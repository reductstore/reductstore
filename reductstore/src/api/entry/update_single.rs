// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum_extra::headers::{Header, HeaderMap};
use futures_util::StreamExt;

use reduct_base::Labels;

use crate::api::entry::common::{parse_content_length_from_header, parse_timestamp_from_query};
use crate::api::middleware::check_permissions;
use crate::api::{Components, ErrorCode, HttpError};
use crate::auth::policy::WriteAccessPolicy;
use crate::storage::proto::record::Label;

// PATCH /:bucket/:entry?ts=<number>
pub(crate) async fn update_record(
    State(components): State<Arc<Components>>,
    headers: HeaderMap,
    Path(path): Path<HashMap<String, String>>,
    Query(params): Query<HashMap<String, String>>,
    body: Body,
) -> Result<(), HttpError> {
    let bucket = path.get("bucket_name").unwrap();
    check_permissions(
        &components,
        headers.clone(),
        WriteAccessPolicy {
            bucket: bucket.clone(),
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
    components
        .storage
        .write()
        .await
        .get_mut_bucket(bucket)?
        .get_mut_entry(entry_name)?
        .update_labels(ts, labels_to_update, labels_to_remove)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use axum_extra::headers::{Authorization, HeaderMapExt};
    use rstest::*;

    use crate::api::tests::{components, empty_body, path_to_entry_1};
    use crate::storage::proto::record::Label;

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
            .read()
            .await
            .get_bucket("bucket-1")
            .unwrap()
            .begin_read("entry-1", 0)
            .await
            .unwrap();

        assert_eq!(record.labels().len(), 2);
        assert_eq!(
            record.labels()[0],
            Label {
                name: "x".to_string(),
                value: "z".to_string(),
            }
        );
        assert_eq!(
            record.labels()[1],
            Label {
                name: "1".to_string(),
                value: "2".to_string(),
            }
        );
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
        headers.insert("content-length", "0".parse().unwrap());
        headers.insert("x-reduct-label-x", "z".parse().unwrap()); // update
        headers.insert("x-reduct-label-b", "".parse().unwrap()); // remove
        headers.insert("x-reduct-label-1", "2".parse().unwrap()); // add

        headers.typed_insert(Authorization::bearer("init-token").unwrap());

        headers
    }
}
