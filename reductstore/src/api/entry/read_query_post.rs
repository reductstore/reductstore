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
    assert_eq!(
        request.query_type,
        QueryType::Query,
        "Query type must be Query"
    );

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
    use reduct_base::error::ErrorCode;
    use reduct_base::msg::entry_api::QueryType;
    use rstest::*;

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

        let result = read_query_json(
            State(Arc::clone(&components)),
            path_to_entry_1,
            request,
            headers,
        )
        .await;

        let query: QueryInfo = result.unwrap().into();
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

        let rx = entry
            .get_query_receiver(query.id)
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
}
