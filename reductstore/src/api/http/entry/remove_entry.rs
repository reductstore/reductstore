// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::http::HttpError;
use crate::auth::policy::WriteAccessPolicy;
use std::collections::HashMap;

use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;

use crate::api::http::StateKeeper;
use crate::storage::entry::validate_remove_entry;
use std::sync::Arc;

// DELETE /b/:bucket_name/:entry_name
pub(super) async fn remove_entry(
    State(keeper): State<Arc<StateKeeper>>,
    Path(path): Path<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<(), HttpError> {
    let bucket_name = path.get("bucket_name").unwrap();
    let entry_name = path.get("entry_name").unwrap();
    validate_remove_entry(entry_name)?;

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
        .get_bucket(bucket_name)
        .await?
        .upgrade()?
        .remove_entry(entry_name)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::tests::{headers, keeper};
    use axum::extract::State;
    use axum_extra::headers::HeaderMap;
    use reduct_base::error::ErrorCode;
    use rstest::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_remove_entry(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let keeper = keeper.await;
        let path = HashMap::from_iter(vec![
            ("bucket_name".to_string(), "bucket-1".to_string()),
            ("entry_name".to_string(), "entry-1".to_string()),
        ]);
        remove_entry(State(keeper.clone()), Path(path), headers.clone())
            .await
            .unwrap();
        let components = keeper.get_anonymous().await.unwrap();
        let status = components
            .storage
            .get_bucket("bucket-1")
            .await
            .unwrap()
            .upgrade_and_unwrap()
            .get_entry("entry-1")
            .await
            .err()
            .unwrap()
            .status();
        assert!(
            matches!(status, ErrorCode::NotFound | ErrorCode::Conflict),
            "Entry removal should finish quickly"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_entry_bucket_not_found(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let path = HashMap::from_iter(vec![
            ("bucket_name".to_string(), "XXX".to_string()),
            ("entry_name".to_string(), "entry-1".to_string()),
        ]);
        let result = remove_entry(State(keeper.clone()), Path(path), headers.clone()).await;
        assert_eq!(result.unwrap_err().status(), ErrorCode::NotFound);
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_meta_entry_forbidden(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let path = HashMap::from_iter(vec![
            ("bucket_name".to_string(), "bucket-1".to_string()),
            ("entry_name".to_string(), "entry-1/$meta".to_string()),
        ]);

        let err = remove_entry(State(keeper), Path(path), headers)
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), ErrorCode::UnprocessableEntity);
        assert_eq!(
            err.message(),
            "Can't delete system entry 'entry-1/$meta'; remove the parent entry instead"
        );
    }
}
