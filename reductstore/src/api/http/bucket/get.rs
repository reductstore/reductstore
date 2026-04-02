// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::http::bucket::FullBucketInfoAxum;
use crate::api::http::HttpError;
use crate::api::http::StateKeeper;
use crate::auth::policy::ReadAccessPolicy;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
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
    let bucket_info = components
        .storage
        .get_bucket(&bucket_name)
        .await?
        .upgrade()?;
    Ok(bucket_info.info().await?.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::api::http::tests::{auth_keeper as keeper, headers};
    use crate::audit::AUDIT_BUCKET_NAME;
    use axum::http::HeaderValue;
    use reduct_base::error::ErrorCode;
    use reduct_base::msg::bucket_api::BucketSettings;
    use reduct_base::msg::token_api::{Permissions, TokenCreateRequest};
    use rstest::rstest;
    use std::sync::Arc;

    fn bearer_headers(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {}", token)).unwrap(),
        );
        headers
    }

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
            .unwrap()
            .generate_token(
                "test-token",
                TokenCreateRequest {
                    permissions: Permissions {
                        full_access: false,
                        read: vec!["bucket-1".to_string()],
                        write: vec![],
                    },
                    expires_at: None,
                },
            )
            .await
            .unwrap();

        headers.insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {}", token.value)).unwrap(),
        );
        let err = get_bucket(State(keeper), Path("bucket-2".to_string()), headers)
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), ErrorCode::Forbidden);
    }

    #[rstest]
    #[case(true, None, false)]
    #[case(false, Some(AUDIT_BUCKET_NAME), false)]
    #[case(false, Some("*"), true)]
    #[tokio::test]
    async fn test_get_system_bucket_permissions(
        #[future] keeper: Arc<StateKeeper>,
        #[case] full_access: bool,
        #[case] read_bucket: Option<&'static str>,
        #[case] should_forbid: bool,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        components
            .storage
            .create_system_bucket(AUDIT_BUCKET_NAME, BucketSettings::default())
            .await
            .unwrap();

        let token = components
            .token_repo
            .write()
            .await
            .unwrap()
            .generate_token(
                "system-bucket-reader",
                TokenCreateRequest {
                    permissions: Permissions {
                        full_access,
                        read: read_bucket
                            .into_iter()
                            .map(|bucket| bucket.to_string())
                            .collect(),
                        write: vec![],
                    },
                    expires_at: None,
                },
            )
            .await
            .unwrap();

        let result = get_bucket(
            State(keeper),
            Path(AUDIT_BUCKET_NAME.to_string()),
            bearer_headers(&token.value),
        )
        .await;

        if should_forbid {
            assert_eq!(result.err().unwrap().status(), ErrorCode::Forbidden);
        } else {
            assert_eq!(result.unwrap().0.info.name, AUDIT_BUCKET_NAME);
        }
    }
}
