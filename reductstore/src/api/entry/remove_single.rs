// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum_extra::headers::HeaderMap;

use crate::api::entry::common::parse_timestamp_from_query;
use crate::api::HttpError;
use crate::api::StateKeeper;
use crate::auth::policy::WriteAccessPolicy;

// DELETE /:bucket/:entry?ts=<number>
pub(super) async fn remove_record(
    State(keeper): State<Arc<StateKeeper>>,
    headers: HeaderMap,
    Path(path): Path<HashMap<String, String>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<(), HttpError> {
    let bucket = path.get("bucket_name").unwrap();
    let components = keeper
        .get_with_permissions(&headers, WriteAccessPolicy { bucket })
        .await?;

    let ts = parse_timestamp_from_query(&params)?;
    let entry_name = path.get("entry_name").unwrap();
    let err_map = components
        .storage
        .get_bucket(bucket)
        .await?
        .upgrade()?
        .get_entry(entry_name)
        .await?
        .upgrade()?
        .remove_records(vec![ts])
        .await?;

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

    use crate::api::tests::{keeper, path_to_entry_1};
    use axum_extra::headers::{Authorization, HeaderMapExt};
    use reduct_base::error::ReductError;
    use reduct_base::{not_found, unprocessable_entity};
    use rstest::*;

    #[rstest]
    #[tokio::test]
    async fn test_remove_single_ok(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();

        remove_record(
            State(Arc::clone(&keeper)),
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
            .get_bucket("bucket-1")
            .await
            .unwrap()
            .upgrade_and_unwrap()
            .get_entry("entry-1")
            .await
            .unwrap()
            .upgrade_and_unwrap()
            .begin_read(0)
            .await
            .err()
            .unwrap();
        assert_eq!(err, not_found!("No record with timestamp 0"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_bucket_not_found(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let keeper = keeper.await;
        let path = Path(HashMap::from_iter(vec![
            ("bucket_name".to_string(), "XXX".to_string()),
            ("entry_name".to_string(), "entry-1".to_string()),
        ]));
        let err = remove_record(
            State(Arc::clone(&keeper)),
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
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
    ) {
        let keeper = keeper.await;
        let err = remove_record(
            State(keeper.clone()),
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
