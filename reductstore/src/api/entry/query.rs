// Copyright 2023-2024 ReductStore
// Licensed under the Business Source License 1.1

use crate::api::entry::QueryInfoAxum;
use crate::api::middleware::check_permissions;
use crate::api::{Components, HttpError};
use crate::auth::policy::ReadAccessPolicy;
use crate::storage::query::base::QueryOptions;

use axum::extract::{Path, Query, State};
use axum_extra::headers::HeaderMap;
use reduct_base::error::ErrorCode;
use reduct_base::msg::entry_api::QueryInfo;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

// GET /:bucket/:entry/q?start=<number>&stop=<number>&continue=<number>&exclude-<label>=<value>&include-<label>=<value>&ttl=<number>
pub(crate) async fn query(
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

    let entry_info = {
        let mut storage = components.storage.write().await;
        let bucket = storage.get_mut_bucket(bucket_name)?;
        bucket.get_entry(entry_name)?.info().await?
    };

    let start = match params.get("start") {
        Some(start) => start.parse::<u64>().map_err(|_| {
            HttpError::new(
                ErrorCode::UnprocessableEntity,
                "'start' must be an unix timestamp in microseconds",
            )
        })?,
        None => entry_info.oldest_record,
    };

    let stop = match params.get("stop") {
        Some(stop) => stop.parse::<u64>().map_err(|_| {
            HttpError::new(
                ErrorCode::UnprocessableEntity,
                "'stop' must be an unix timestamp in microseconds",
            )
        })?,
        None => entry_info.latest_record + 1,
    };

    let continuous = match params.get("continuous") {
        Some(continue_) => continue_.parse::<bool>().map_err(|_| {
            HttpError::new(
                ErrorCode::UnprocessableEntity,
                "'continue' must be an unix timestamp in microseconds",
            )
        })?,
        None => false,
    };

    let ttl = match params.get("ttl") {
        Some(ttl) => ttl.parse::<u64>().map_err(|_| {
            HttpError::new(
                ErrorCode::UnprocessableEntity,
                "'ttl' must be an unix timestamp in microseconds",
            )
        })?,
        None => 5,
    };

    let limit = match params.get("limit") {
        Some(limit) => Some(limit.parse::<usize>().map_err(|_| {
            HttpError::new(
                ErrorCode::UnprocessableEntity,
                "'limit' must unsigned integer",
            )
        })?),
        None => None,
    };

    let mut include = HashMap::new();
    let mut exclude = HashMap::new();

    for (k, v) in params.iter() {
        if k.starts_with("include-") {
            include.insert(k[8..].to_string(), v.to_string());
        } else if k.starts_with("exclude-") {
            exclude.insert(k[8..].to_string(), v.to_string());
        }
    }

    let mut storage = components.storage.write().await;
    let bucket = storage.get_mut_bucket(bucket_name)?;
    let entry = bucket.get_or_create_entry(entry_name)?;
    let id = entry.query(
        start,
        stop,
        QueryOptions {
            continuous,
            include,
            exclude,
            ttl: Duration::from_secs(ttl),
            limit,
        },
    )?;

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

        let result = query(
            State(Arc::clone(&components)),
            path_to_entry_1,
            Query(params),
            headers,
        )
        .await;

        let query: QueryInfo = result.unwrap().into();

        let mut storage = components.storage.write().await;
        let entry = storage
            .get_mut_bucket("bucket-1")
            .unwrap()
            .get_mut_entry("entry-1")
            .unwrap();

        let reader = entry.next(query.id).await.unwrap();
        assert!(reader.last());

        assert_eq!(
            entry.next(query.id).await.err().unwrap().status,
            ErrorCode::NoContent
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_bad_limit(
        #[future] components: Arc<Components>,
        path_to_entry_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let components = components.await;
        let mut params = HashMap::new();
        params.insert("limit".to_string(), "a".to_string());

        let result = query(
            State(Arc::clone(&components)),
            path_to_entry_1,
            Query(params),
            headers,
        )
        .await;

        assert_eq!(
            result.err().unwrap().0.status,
            ErrorCode::UnprocessableEntity
        );
    }
}
