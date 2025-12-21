// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::HttpError;
use crate::api::StateKeeper;
use crate::auth::policy::WriteAccessPolicy;
use axum::extract::{Path, State};
use axum::Json;
use axum_extra::headers::HeaderMap;
use reduct_base::msg::entry_api::RenameEntry;
use std::collections::HashMap;
use std::sync::Arc;

// PUT /b/:bucket_name/:entry_name
pub(super) async fn rename_entry(
    State(keeper): State<Arc<StateKeeper>>,
    Path(path): Path<HashMap<String, String>>,
    headers: HeaderMap,
    request: Json<RenameEntry>,
) -> Result<(), HttpError> {
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

    components
        .storage
        .get_bucket(bucket_name)?
        .upgrade()?
        .rename_entry(entry_name, &request.new_name.clone())
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tests::{headers, keeper};
    use axum::extract::State;
    use axum_extra::headers::HeaderMap;
    use reduct_base::error::ErrorCode;
    use rstest::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_rename_entry(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let keeper = keeper.await;
        let path = HashMap::from_iter(vec![
            ("bucket_name".to_string(), "bucket-1".to_string()),
            ("entry_name".to_string(), "entry-1".to_string()),
        ]);
        let request = RenameEntry {
            new_name: "entry-2".to_string(),
        };

        rename_entry(
            State(keeper.clone()),
            Path(path),
            headers.clone(),
            axum::Json(request.clone()),
        )
        .await
        .unwrap();

        let components = keeper.get_anonymous().await.unwrap();
        assert_eq!(
            components
                .storage
                .get_bucket("bucket-1")
                .unwrap()
                .upgrade_and_unwrap()
                .get_entry("entry-1")
                .err()
                .unwrap()
                .status(),
            ErrorCode::NotFound
        );
        assert_eq!(
            components
                .storage
                .get_bucket("bucket-1")
                .unwrap()
                .upgrade_and_unwrap()
                .get_entry("entry-2")
                .unwrap()
                .upgrade_and_unwrap()
                .name(),
            "entry-2"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_rename_bucket_not_found(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let keeper = keeper.await;
        let path = HashMap::from_iter(vec![
            ("bucket_name".to_string(), "XXX".to_string()),
            ("entry_name".to_string(), "entry-1".to_string()),
        ]);
        let request = RenameEntry {
            new_name: "entry-2".to_string(),
        };
        let result = rename_entry(
            State(keeper.clone()),
            Path(path),
            headers.clone(),
            axum::Json(request.clone()),
        )
        .await;
        let err = result.unwrap_err();
        assert_eq!(err.status(), ErrorCode::NotFound);
    }
}
