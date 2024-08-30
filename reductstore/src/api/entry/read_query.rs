// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::entry::QueryInfoAxum;
use crate::api::middleware::check_permissions;
use crate::api::{Components, HttpError};
use crate::auth::policy::ReadAccessPolicy;
use crate::storage::query::base::QueryOptions;

use crate::api::entry::common::{parse_query_params, parse_time_range};
use axum::extract::{Path, Query, State};
use axum_extra::headers::HeaderMap;
use reduct_base::error::ErrorCode;
use reduct_base::msg::entry_api::{EntryInfo, QueryInfo};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

// GET /:bucket/:entry/q?start=<number>&stop=<number>&continue=<number>&exclude-<label>=<value>&include-<label>=<value>&ttl=<number>
pub(crate) async fn read_query(
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

    let mut storage = components.storage.write().await;
    let bucket = storage.get_bucket_mut(bucket_name)?;
    let entry_info = bucket.get_entry(entry_name)?.info().await?;
    let entry = bucket.get_or_create_entry(entry_name)?;

    let (start, stop) =
        parse_time_range(&params, entry_info.oldest_record, entry_info.latest_record)?;
    let id = entry.query(start, stop, parse_query_params(params, false)?)?;

    Ok(QueryInfoAxum::from(QueryInfo { id }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tests::{components, headers, path_to_entry_1};
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

        let result = read_query(
            State(Arc::clone(&components)),
            path_to_entry_1,
            Query(params),
            headers,
        )
        .await;

        let query: QueryInfo = result.unwrap().into();

        let mut storage = components.storage.write().await;
        let entry = storage
            .get_bucket_mut("bucket-1")
            .unwrap()
            .get_entry_mut("entry-1")
            .unwrap();

        let rx = entry.get_query_receiver(query.id).await.unwrap();
        assert!(rx.recv().await.unwrap().unwrap().last());

        assert_eq!(
            rx.recv().await.unwrap().err().unwrap().status,
            ErrorCode::NoContent
        );
    }
}
