// Copyright 2023-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::entry::QueryInfoAxum;
use crate::api::StateKeeper;
use crate::api::{Components, HttpError};
use crate::auth::policy::ReadAccessPolicy;

use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;
use reduct_base::msg::entry_api::{QueryEntry, QueryInfo};
use std::collections::HashMap;
use std::sync::Arc;

// POST /:bucket/:entry/q
pub(super) async fn read_query_json(
    State(keeper): State<Arc<StateKeeper>>,
    Path(path): Path<HashMap<String, String>>,
    request: QueryEntry,
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
    let id = entry.query(request.clone()).await?;

    components
        .ext_repo
        .register_query(id, bucket_name, entry_name, request)
        .await?;

    Ok(QueryInfoAxum::from(QueryInfo { id }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tests::{headers, keeper, path_to_entry_1};
    use crate::core::weak::Weak;
    use crate::storage::query::QueryRx;
    use reduct_base::error::{ErrorCode, ReductError};
    use reduct_base::msg::entry_api::QueryType;
    use rstest::*;
    use serde_json::json;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    #[rstest]
    #[tokio::test]
    async fn test_limited_query(
        #[future] keeper: Arc<StateKeeper>,
        path_to_entry_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let request = QueryEntry {
            query_type: QueryType::Query,
            limit: Some(1),
            ..Default::default()
        };

        let rx = get_query_receiver(path_to_entry_1, headers, keeper.clone(), request)
            .await
            .unwrap()
            .upgrade()
            .unwrap();
        let mut rx = rx.write().await;
        assert!(rx.recv().await.unwrap().is_ok());
        assert_eq!(
            rx.recv().await.unwrap().err().unwrap().status,
            ErrorCode::NoContent
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_strict_request(
        #[future] keeper: Arc<StateKeeper>,
        path_to_entry_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let request = QueryEntry {
            query_type: QueryType::Query,
            when: Some(json!({
                "$gt": [1, "&NOT_EXIST"]
            })),
            strict: Some(true),
            ..Default::default()
        };

        let rx = get_query_receiver(path_to_entry_1, headers, keeper.clone(), request)
            .await
            .unwrap()
            .upgrade()
            .unwrap();
        let mut rx = rx.write().await;
        assert_eq!(
            rx.recv().await.unwrap().err().unwrap().to_string(),
            "[NotFound] Reference 'NOT_EXIST' not found"
        );
    }

    async fn get_query_receiver(
        path_to_entry_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
        keeper: Arc<StateKeeper>,
        request: QueryEntry,
    ) -> Result<Weak<RwLock<QueryRx>>, ReductError> {
        let components = keeper.get_anonymous().await.unwrap();
        let response = read_query_json(State(keeper), path_to_entry_1, request, headers)
            .await
            .unwrap();
        let query: QueryInfo = response.into();
        let entry = components
            .storage
            .get_bucket("bucket-1")?
            .upgrade()?
            .get_entry("entry-1")?
            .upgrade()?;

        let (rx, _) = entry.get_query_receiver(query.id)?;
        Ok(rx)
    }
}
