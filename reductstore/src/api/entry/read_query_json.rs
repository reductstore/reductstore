// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::entry::QueryInfoAxum;
use crate::api::middleware::check_permissions;
use crate::api::{Components, HttpError};
use crate::auth::policy::ReadAccessPolicy;

use crate::api::entry::common::{parse_query_params, parse_time_range};
use axum::extract::{Path, Query, State};
use axum_extra::headers::HeaderMap;
use reduct_base::msg::entry_api::QueryInfo;
use std::collections::HashMap;
use std::sync::Arc;

// POST /:bucket/:entry/q
pub(crate) async fn read_query_json(
    State(components): State<Arc<Components>>,
    Path(path): Path<HashMap<String, String>>,
    Query(params): Query<HashMap<String, String>>,
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
    let id = entry.query(parse_query_params(params, false)?).await?;

    Ok(QueryInfoAxum::from(QueryInfo { id }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tests::{components, headers, path_to_entry_1};
    use reduct_base::error::ErrorCode;
    use rstest::*;

    #[rstest]
    #[tokio::test]
    async fn test_limited_query(
        #[future] components: Arc<Components>,
        path_to_entry_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let components = components.await;
        let mut params = HashMap::new();
        params.insert("limit".to_string(), "1".to_string());

        let result = read_query_json(
            State(Arc::clone(&components)),
            path_to_entry_1,
            Query(params),
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
