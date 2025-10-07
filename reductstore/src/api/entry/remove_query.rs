// Copyright 2023-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::entry::RemoveQueryInfoAxum;
use crate::api::StateKeeper;
use crate::api::{Components, HttpError};
use crate::auth::policy::WriteAccessPolicy;

use crate::api::entry::common::parse_query_params;
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
    let components = keeper
        .get_with_permissions(
            &headers,
            WriteAccessPolicy {
                bucket: bucket_name,
            },
        )
        .await?;

    let bucket = components.storage.get_bucket(bucket_name)?.upgrade()?;
    let entry = bucket.get_or_create_entry(entry_name)?.upgrade()?;
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
    use crate::api::tests::{components, headers, path_to_entry_1};
    use reduct_base::error::ReductError;
    use rstest::*;
    #[rstest]
    #[tokio::test]
    async fn test_remove_query(
        #[future] components: Arc<Components>,
        path_to_entry_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let components = components.await;
        let mut params = HashMap::new();
        params.insert("start".to_string(), "0".to_string());
        params.insert("stop".to_string(), "2".to_string());

        let result = remove_query(
            State(components.clone()),
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
        #[future] components: Arc<Components>,
        path_to_entry_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let components = components.await;
        let mut params = HashMap::new();
        params.insert("start".to_string(), "0".to_string());
        params.insert("stop".to_string(), "xxx".to_string());

        let err = remove_query(
            State(components.clone()),
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
        #[future] components: Arc<Components>,
        path_to_entry_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let params = HashMap::new();
        let err = remove_query(
            State(components.await),
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
}
