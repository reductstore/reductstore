// Copyright 2023-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::entry::QueryInfoAxum;
use crate::api::{Components, HttpError};
use crate::auth::policy::ReadAccessPolicy;

use crate::api::entry::common::parse_query_params;
use crate::api::StateKeeper;
use axum::extract::{Path, Query, State};
use axum_extra::headers::HeaderMap;
use reduct_base::msg::entry_api::QueryInfo;
use std::collections::HashMap;
use std::sync::Arc;

// GET /:bucket/:entry/q?start=<number>&stop=<number>&continue=<number>&exclude-<label>=<value>&include-<label>=<value>&ttl=<number>
pub(super) async fn read_query(
    State(keeper): State<Arc<StateKeeper>>,
    Path(path): Path<HashMap<String, String>>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<QueryInfoAxum, HttpError> {
    let bucket_name = path.get("bucket_name").unwrap();
    let entry_name = path.get("entry_name").unwrap();
    let components = keeper
        .get_with_permissions(
            &headers,
            ReadAccessPolicy {
                bucket: bucket_name,
            },
        )
        .await?;

    let bucket = components.storage.get_bucket(bucket_name)?.upgrade()?;
    let entry = bucket.get_entry(entry_name)?.upgrade()?;
    let id = entry.query(parse_query_params(params, false)?).await?;

    Ok(QueryInfoAxum::from(QueryInfo { id }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tests::{headers, keeper, path_to_entry_1};
    use reduct_base::error::ErrorCode;
    use rstest::*;
    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_limited_query(
        #[future] keeper: Arc<StateKeeper>,
        path_to_entry_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let mut params = HashMap::new();
        params.insert("limit".to_string(), "1".to_string());

        let result = read_query(
            State(keeper.clone()),
            path_to_entry_1,
            Query(params),
            headers,
        )
        .await;

        let query: QueryInfo = result.unwrap().into();
        let components = keeper.get_anonymous().await.unwrap();
        let entry = components
            .storage
            .get_bucket("bucket-1")
            .unwrap()
            .upgrade()
            .unwrap()
            .get_entry("entry-1")
            .unwrap()
            .upgrade()
            .unwrap();

        let (rx, _) = entry.get_query_receiver(query.id).unwrap();
        let rx = rx.upgrade().unwrap();
        let mut rx = rx.write().await;
        assert!(rx.recv().await.unwrap().is_ok());

        assert_eq!(
            rx.recv().await.unwrap().err().unwrap().status,
            ErrorCode::NoContent
        );
    }
}
