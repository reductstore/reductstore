// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::bucket::FullBucketInfoAxum;
use crate::api::HttpError;
use crate::api::StateKeeper;
use crate::auth::policy::ReadAccessPolicy;
use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;
use std::sync::Arc;

// GET /b/:bucket_name

pub(super) async fn get_bucket(
    State(keeper): State<Arc<StateKeeper>>,
    Path(bucket_name): Path<String>,
    headers: HeaderMap,
) -> Result<FullBucketInfoAxum, HttpError> {
    let components = keeper
        .get_with_permissions(
            &headers,
            ReadAccessPolicy {
                bucket: &bucket_name,
            },
        )
        .await?;
    let bucket_info = components.storage.get_bucket(&bucket_name)?.upgrade()?;
    Ok(bucket_info.info()?.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::api::tests::{headers, keeper};
    use axum::http::HeaderValue;
    use reduct_base::error::ErrorCode;
    use reduct_base::msg::token_api::Permissions;
    use rstest::rstest;
    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_get_bucket(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let info = get_bucket(State(keeper.await), Path("bucket-1".to_string()), headers)
            .await
            .unwrap();
        assert_eq!(info.0.info.name, "bucket-1");
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_bucket_not_found(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let err = get_bucket(State(keeper.await), Path("not-found".to_string()), headers)
            .await
            .err()
            .unwrap();
        assert_eq!(
            err,
            HttpError::new(ErrorCode::NotFound, "Bucket 'not-found' is not found")
        )
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_bucket_unauthorized(
        #[future] keeper: Arc<StateKeeper>,
        mut headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        let token = components
            .token_repo
            .write()
            .await
            .generate_token(
                "test-token",
                Permissions {
                    full_access: false,
                    read: vec!["bucket-1".to_string()],
                    write: vec![],
                },
            )
            .unwrap();

        headers.insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {}", token.value)).unwrap(),
        );
        let err = get_bucket(State(keeper), Path("bucket-2".to_string()), headers)
            .await
            .err()
            .unwrap();
        assert_eq!(err.0.status(), ErrorCode::Forbidden);
    }
}
