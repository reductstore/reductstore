// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum_extra::headers::HeaderMap;

use crate::api::entry::common::parse_timestamp_from_query;
use crate::api::middleware::check_permissions;
use crate::api::{Components, HttpError};
use crate::auth::policy::WriteAccessPolicy;

// DELETE /:bucket/:entry?ts=<number>
pub(crate) async fn remove_record(
    State(components): State<Arc<Components>>,
    headers: HeaderMap,
    Path(path): Path<HashMap<String, String>>,
    Query(params): Query<HashMap<String, String>>,
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

    let ts = parse_timestamp_from_query(&params)?;
    let entry_name = path.get("entry_name").unwrap();
    let err_map = components
        .storage
        .get_bucket(bucket)?
        .upgrade()?
        .get_entry(entry_name)?
        .upgrade()?
        .remove_records(vec![ts])?;

    // We don't replicate the deletion of records

    if err_map.is_empty() {
        Ok(())
    } else {
        Err(err_map.into_iter().next().unwrap().1.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::api::tests::{components, path_to_entry_1};
    use axum_extra::headers::{Authorization, HeaderMapExt};
    use reduct_base::error::ReductError;
    use reduct_base::{not_found, unprocessable_entity};
    use rstest::*;

    #[rstest]
    #[tokio::test]
    async fn test_remove_single_ok(
        #[future] components: Arc<Components>,
        headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
    ) {
        let components = components.await;

        remove_record(
            State(Arc::clone(&components)),
            headers,
            path_to_entry_1,
            Query(HashMap::from_iter(vec![(
                "ts".to_string(),
                "0".to_string(),
            )])),
        )
        .await
        .unwrap();

        let err = components
            .storage
            .read()
            .await
            .get_bucket("bucket-1")
            .unwrap()
            .begin_read("entry-1", 0)
            .await
            .err()
            .unwrap();
        assert_eq!(err, not_found!("No record with timestamp 0"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_bucket_not_found(
        #[future] components: Arc<Components>,
        headers: HeaderMap,
    ) {
        let components = components.await;
        let path = Path(HashMap::from_iter(vec![
            ("bucket_name".to_string(), "XXX".to_string()),
            ("entry_name".to_string(), "entry-1".to_string()),
        ]));
        let err = remove_record(
            State(Arc::clone(&components)),
            headers,
            path,
            Query(HashMap::from_iter(vec![(
                "ts".to_string(),
                "1".to_string(),
            )])),
        )
        .await
        .err()
        .unwrap();

        assert_eq!(
            err,
            HttpError::from(not_found!("Bucket 'XXX' is not found"))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_bad_ts(
        #[future] components: Arc<Components>,
        headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
    ) {
        let components = components.await;
        let err = remove_record(
            State(Arc::clone(&components)),
            headers,
            path_to_entry_1,
            Query(HashMap::from_iter(vec![(
                "ts".to_string(),
                "bad".to_string(),
            )])),
        )
        .await
        .err()
        .unwrap();

        assert_eq!(
            err,
            HttpError::from(unprocessable_entity!(
                "'ts' must be an unix timestamp in microseconds"
            ))
        );
    }

    #[fixture]
    pub fn headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.typed_insert(Authorization::bearer("init-token").unwrap());

        headers
    }
}
