// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::http::entry::RemoveQueryInfoAxum;
use crate::api::http::HttpError;
use crate::api::http::StateKeeper;
use crate::auth::policy::WriteAccessPolicy;

use crate::api::http::entry::common::parse_query_params;
use crate::storage::entry::validate_remove_records;
use axum::extract::{Path, Query, State};
use axum_extra::headers::HeaderMap;
use reduct_base::error::ReductError;
use reduct_base::msg::entry_api::RemoveQueryInfo;
use reduct_base::unprocessable_entity;
use std::collections::HashMap;
use std::sync::Arc;

// DELETE /:bucket/:entry/q?start=<number>&stop=<number>&continue=<number>&exclude-<label>=<value>&include-<label>=<value>&ttl=<number>
pub(super) async fn remove_query(
    State(keeper): State<Arc<StateKeeper>>,
    Path(path): Path<HashMap<String, String>>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<RemoveQueryInfoAxum, HttpError> {
    let bucket_name = path.get("bucket_name").unwrap();
    let entry_name = path.get("entry_name").unwrap();
    validate_remove_records(entry_name)?;

    let components = keeper
        .get_with_permissions(
            &headers,
            WriteAccessPolicy {
                bucket: bucket_name,
            },
        )
        .await?;

    let bucket = components
        .storage
        .get_bucket(bucket_name)
        .await?
        .upgrade()?;
    let entry = bucket.get_or_create_entry(entry_name).await?.upgrade()?;
    if params.is_empty() {
        return Err(
            unprocessable_entity!("Define at least one query parameter to delete records").into(),
        );
    }

    let removed_records = entry
        .query_remove_records(parse_query_params(params, true)?)
        .await?;

    Ok(RemoveQueryInfoAxum::from(RemoveQueryInfo {
        removed_records,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::tests::{headers, keeper, path_to_entry_1};
    use reduct_base::error::ReductError;
    use rstest::*;
    #[rstest]
    #[tokio::test]
    async fn test_remove_query(
        #[future] keeper: Arc<StateKeeper>,
        path_to_entry_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let mut params = HashMap::new();
        params.insert("start".to_string(), "0".to_string());
        params.insert("stop".to_string(), "2".to_string());

        let result = remove_query(
            State(keeper.clone()),
            path_to_entry_1,
            Query(params),
            headers,
        )
        .await
        .unwrap()
        .0;
        assert_eq!(result.removed_records, 1);
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_query_wrong_param(
        #[future] keeper: Arc<StateKeeper>,
        path_to_entry_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let mut params = HashMap::new();
        params.insert("start".to_string(), "0".to_string());
        params.insert("stop".to_string(), "xxx".to_string());

        let err = remove_query(
            State(keeper.clone()),
            path_to_entry_1,
            Query(params),
            headers,
        )
        .await
        .err()
        .unwrap();
        assert_eq!(
            err,
            unprocessable_entity!("'stop' must be an unix timestamp in microseconds").into()
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_query_at_least_on(
        #[future] keeper: Arc<StateKeeper>,
        path_to_entry_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let params = HashMap::new();
        let err = remove_query(
            State(keeper.clone()),
            path_to_entry_1,
            Query(params),
            headers,
        )
        .await
        .err()
        .unwrap();
        assert_eq!(
            err,
            unprocessable_entity!("Define at least one query parameter to delete records").into()
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_query_meta_forbidden(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let path = Path(HashMap::from_iter(vec![
            ("bucket_name".to_string(), "bucket-1".to_string()),
            ("entry_name".to_string(), "entry-1/$meta".to_string()),
        ]));

        let mut params = HashMap::new();
        params.insert("start".to_string(), "0".to_string());

        let err = remove_query(State(keeper), path, Query(params), headers)
            .await
            .err()
            .unwrap();

        assert_eq!(
            err,
            unprocessable_entity!(
                "Can't delete records from system entry 'entry-1/$meta'; use label update with remove=true"
            )
            .into()
        );
    }
}
