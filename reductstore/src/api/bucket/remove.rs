// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::HttpError;
use crate::api::StateKeeper;
use crate::auth::policy::FullAccessPolicy;

use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;

use std::sync::Arc;

// DELETE /b/:bucket_name
pub(super) async fn remove_bucket(
    State(keeper): State<Arc<StateKeeper>>,
    Path(bucket_name): Path<String>,
    headers: HeaderMap,
) -> Result<(), HttpError> {
    let components = keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;
    components.storage.remove_bucket(&bucket_name)?;
    components
        .token_repo
        .write()
        .await
        .remove_bucket_from_tokens(&bucket_name)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::api::tests::{headers, keeper};
    use reduct_base::error::ErrorCode;
    use rstest::rstest;
    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_remove_bucket(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        remove_bucket(State(keeper.await), Path("bucket-1".to_string()), headers)
            .await
            .unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_bucket_not_found(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let err = remove_bucket(State(keeper.await), Path("not-found".to_string()), headers)
            .await
            .err()
            .unwrap();
        assert_eq!(
            err,
            HttpError::new(ErrorCode::NotFound, "Bucket 'not-found' is not found",)
        )
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_bucket_from_permission(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        let token = components
            .token_repo
            .write()
            .await
            .get_token("test")
            .unwrap()
            .clone();
        assert_eq!(
            token.permissions.unwrap().read,
            vec!["bucket-1".to_string(), "bucket-2".to_string()]
        );

        remove_bucket(
            State(keeper.clone()),
            Path("bucket-1".to_string()),
            headers.clone(),
        )
        .await
        .unwrap();

        let components = keeper.get_anonymous().await.unwrap();
        let token = components
            .token_repo
            .write()
            .await
            .get_token("test")
            .unwrap()
            .clone();
        assert_eq!(
            token.permissions.unwrap().read,
            vec!["bucket-2".to_string()]
        );
    }
}
