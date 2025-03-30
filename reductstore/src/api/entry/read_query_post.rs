// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::entry::QueryInfoAxum;
use crate::api::middleware::check_permissions;
use crate::api::{Components, HttpError};
use crate::auth::policy::ReadAccessPolicy;

use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;
use reduct_base::msg::entry_api::{QueryEntry, QueryInfo, QueryType};
use std::collections::HashMap;
use std::sync::Arc;

// POST /:bucket/:entry/q
pub(crate) async fn read_query_json(
    State(components): State<Arc<Components>>,
    Path(path): Path<HashMap<String, String>>,
    request: QueryEntry,
    headers: HeaderMap,
) -> Result<QueryInfoAxum, HttpError> {
    let bucket_name = path.get("bucket_name").unwrap();
    let entry_name = path.get("entry_name").unwrap();

    check_permissions(
        &components,
        headers,
        ReadAccessPolicy {
            bucket: bucket_name.clone(),
        },
    )
    .await?;

    let bucket = components.storage.get_bucket(bucket_name)?.upgrade()?;
    let entry = bucket.get_entry(entry_name)?.upgrade()?;
    let id = entry.query(request).await?;

    Ok(QueryInfoAxum::from(QueryInfo { id }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tests::{components, headers, path_to_entry_1};
    use crate::core::weak::Weak;
    use crate::storage::query::QueryRx;
    use reduct_base::error::{ErrorCode, ReductError};
    use reduct_base::msg::entry_api::QueryType;
    use rstest::*;
    use serde_json::json;
    use tokio::sync::RwLock;

    #[rstest]
    #[tokio::test]
    async fn test_limited_query(
        #[future] components: Arc<Components>,
        path_to_entry_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let components = components.await;
        let request = QueryEntry {
            query_type: QueryType::Query,
            limit: Some(1),
            ..Default::default()
        };

        let rx = get_query_receiver(path_to_entry_1, headers, components, request)
            .await
            .unwrap()
            .upgrade()
            .unwrap();
        let mut rx = rx.write().await;
        assert!(rx.recv().await.unwrap().unwrap().last());
        assert_eq!(
            rx.recv().await.unwrap().err().unwrap().status,
            ErrorCode::NoContent
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_strict_request(
        #[future] components: Arc<Components>,
        path_to_entry_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let components = components.await;
        let request = QueryEntry {
            query_type: QueryType::Query,
            when: Some(json!({
                "$gt": [1, "&NOT_EXIST"]
            })),
            strict: Some(true),
            ..Default::default()
        };

        let rx = get_query_receiver(path_to_entry_1, headers, components, request)
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
        components: Arc<Components>,
        request: QueryEntry,
    ) -> Result<Weak<RwLock<QueryRx>>, ReductError> {
        let response =
            read_query_json(State(components.clone()), path_to_entry_1, request, headers)
                .await
                .unwrap();
        let query: QueryInfo = response.into();
        let entry = components
            .storage
            .get_bucket("bucket-1")?
            .upgrade()?
            .get_entry("entry-1")?
            .upgrade()?;

        entry.get_query_receiver(query.id)
    }
}
