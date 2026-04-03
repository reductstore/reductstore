// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::http::server::BucketInfoListAxum;
use crate::api::http::{HttpError, StateKeeper};
use crate::auth::policy::{AuthenticatedPolicy, ReadAccessPolicy};
use axum::extract::State;
use axum_extra::headers::HeaderMap;
use reduct_base::msg::server_api::BucketInfoList;
use std::sync::Arc;

// GET /list
pub(super) async fn list(
    State(keeper): State<Arc<StateKeeper>>,
    headers: HeaderMap,
) -> Result<BucketInfoListAxum, HttpError> {
    let components = keeper
        .get_with_permissions(&headers, AuthenticatedPolicy {})
        .await?;

    let mut filtered_by_read_permission = vec![];

    for bucket in components.storage.clone().get_bucket_list().await?.buckets {
        // Filter out buckets that are not visible to the user
        if keeper
            .get_with_permissions(
                &headers,
                ReadAccessPolicy {
                    bucket: &bucket.name,
                },
            )
            .await
            .is_ok()
        {
            filtered_by_read_permission.push(bucket);
        }
    }

    Ok(BucketInfoList {
        buckets: filtered_by_read_permission,
    }
    .into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::tests::{headers, keeper};
    use crate::audit::AUDIT_BUCKET_NAME;
    use axum::http::HeaderValue;
    use reduct_base::msg::bucket_api::BucketSettings;
    use reduct_base::msg::token_api::{Permissions, TokenCreateRequest};
    use rstest::rstest;

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
    async fn test_list(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let list = list(State(keeper.await), headers).await.unwrap();
        assert_eq!(list.0.buckets.len(), 2);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_filtered_list(#[future] keeper: Arc<StateKeeper>, mut headers: HeaderMap) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        let token = components
            .token_repo
            .write()
            .await
            .unwrap()
            .generate_token(
                "with-one-bucket",
                TokenCreateRequest {
                    permissions: Permissions {
                        read: vec!["bucket-1".to_string()],
                        ..Default::default()
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
        let list = list(State(keeper), headers).await.unwrap();
        assert_eq!(list.0.buckets.len(), 1);
        assert_eq!(list.0.buckets[0].name, "bucket-1");
    }

    #[rstest]
    #[case(true, None, true)]
    #[case(false, Some(AUDIT_BUCKET_NAME), true)]
    #[case(false, Some("*"), false)]
    #[tokio::test]
    async fn test_list_system_bucket_visibility(
        #[future] keeper: Arc<StateKeeper>,
        #[case] full_access: bool,
        #[case] read_bucket: Option<&'static str>,
        #[case] should_include: bool,
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
                        ip_allowlist: vec![],
                    },
                    expires_at: None,
                },
            )
            .await
            .unwrap();

        let list = list(State(keeper), bearer_headers(&token.value))
            .await
            .unwrap();
        let bucket_names = list
            .0
            .buckets
            .iter()
            .map(|bucket| bucket.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(bucket_names.contains(&AUDIT_BUCKET_NAME), should_include);
    }
}
