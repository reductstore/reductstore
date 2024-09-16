// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::entry::RemoveQueryInfoAxum;
use crate::api::middleware::check_permissions;
use crate::api::{Components, HttpError};
use crate::auth::policy::WriteAccessPolicy;

use crate::api::entry::common::{parse_query_params, parse_time_range};
use axum::extract::{Path, Query, State};
use axum_extra::headers::HeaderMap;
use reduct_base::msg::entry_api::RemoveQueryInfo;
use std::collections::HashMap;
use std::sync::Arc;

// DELETE /:bucket/:entry/q?start=<number>&stop=<number>&continue=<number>&exclude-<label>=<value>&include-<label>=<value>&ttl=<number>
pub(crate) async fn remove_query(
    State(components): State<Arc<Components>>,
    Path(path): Path<HashMap<String, String>>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<RemoveQueryInfoAxum, HttpError> {
    let bucket_name = path.get("bucket_name").unwrap();
    let entry_name = path.get("entry_name").unwrap();

    check_permissions(
        &components,
        headers,
        WriteAccessPolicy {
            bucket: bucket_name.clone(),
        },
    )
    .await?;

    let bucket = components.storage.get_bucket(bucket_name)?.upgrade()?;
    let entry = bucket.get_or_create_entry(entry_name)?.upgrade()?;
    let entry_info = entry.info()?;

    let (start, stop) =
        parse_time_range(&params, entry_info.oldest_record, entry_info.latest_record)?;
    let removed_records =
        entry.query_remove_records(start, stop, parse_query_params(params, true)?)?;

    Ok(RemoveQueryInfoAxum::from(RemoveQueryInfo {
        removed_records,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tests::{components, headers, path_to_entry_1};
    use reduct_base::error::ReductError;
    use reduct_base::unprocessable_entity;
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
}
