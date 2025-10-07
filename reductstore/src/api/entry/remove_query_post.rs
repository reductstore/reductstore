// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::entry::RemoveQueryInfoAxum;
use crate::api::{Components, HttpError};
use crate::auth::policy::WriteAccessPolicy;
use reduct_base::error::ReductError;

use crate::api::StateKeeper;
use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;
use reduct_base::msg::entry_api::{QueryEntry, QueryType, RemoveQueryInfo};
use reduct_base::unprocessable_entity;
use std::collections::HashMap;
use std::sync::Arc;

// POST /:bucket/:entry/q
pub(super) async fn remove_query_json(
    State(keeper): State<Arc<StateKeeper>>,
    Path(path): Path<HashMap<String, String>>,
    request: QueryEntry,
    headers: HeaderMap,
) -> Result<RemoveQueryInfoAxum, HttpError> {
    assert_eq!(
        request.query_type,
        QueryType::Remove,
        "Query type must be Remove"
    );
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

    let empty_query = QueryEntry {
        query_type: QueryType::Remove,
        ..Default::default()
    };
    if request == empty_query {
        return Err(
            unprocessable_entity!("Define at least one query parameter to delete records").into(),
        );
    }

    let bucket = components.storage.get_bucket(bucket_name)?.upgrade()?;
    let entry = bucket.get_or_create_entry(entry_name)?.upgrade()?;

    let removed_records = entry.query_remove_records(request).await?;

    Ok(RemoveQueryInfoAxum::from(RemoveQueryInfo {
        removed_records,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tests::{headers, keeper, path_to_entry_1};
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
        let request = QueryEntry {
            query_type: QueryType::Remove,
            start: Some(0),
            stop: Some(2),
            ..Default::default()
        };

        let result = remove_query_json(State(keeper.clone()), path_to_entry_1, request, headers)
            .await
            .unwrap()
            .0;
        assert_eq!(result.removed_records, 1);
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_query_at_least_on(
        #[future] keeper: Arc<StateKeeper>,
        path_to_entry_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let request = QueryEntry {
            query_type: QueryType::Remove,
            ..Default::default()
        };
        let err = remove_query_json(State(keeper.clone()), path_to_entry_1, request, headers)
            .await
            .err()
            .unwrap();
        assert_eq!(
            err,
            unprocessable_entity!("Define at least one query parameter to delete records").into()
        );
    }
}
