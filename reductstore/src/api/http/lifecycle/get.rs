// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::http::lifecycle::{FullLifecyclePolicyInfoAxum, PolicyPath};
use crate::api::http::{HttpError, StateKeeper};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use std::sync::Arc;

// GET /b/:bucket_name/lifecycle-policies/:policy_id
pub(super) async fn get_lifecycle_policy(
    State(keeper): State<Arc<StateKeeper>>,
    Path(PolicyPath {
        bucket_name: _,
        policy_id,
    }): Path<PolicyPath>,
    headers: HeaderMap,
) -> Result<FullLifecyclePolicyInfoAxum, HttpError> {
    let components = keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;

    let info = components
        .lifecycle_repo
        .read()
        .await?
        .get_info(&policy_id)
        .await?;
    Ok(info.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::tests::{headers, keeper};
    use reduct_base::error::ErrorCode::NotFound;
    use reduct_base::msg::lifecycle_api::LifecycleSettings;
    use rstest::rstest;
    use std::sync::Arc;

    async fn make_keeper_with_policy(keeper: Arc<StateKeeper>) -> Arc<StateKeeper> {
        let components = keeper.get_anonymous().await.unwrap();
        components
            .lifecycle_repo
            .write()
            .await
            .unwrap()
            .create_lifecycle(
                "test-policy",
                LifecycleSettings {
                    bucket: "bucket-1".to_string(),
                    max_age: "1d".to_string(),
                    interval: "1h".to_string(),
                    ..LifecycleSettings::default()
                },
            )
            .await
            .unwrap();
        keeper
    }

    fn policy_path(bucket: &str, policy: &str) -> Path<PolicyPath> {
        Path(PolicyPath {
            bucket_name: bucket.to_string(),
            policy_id: policy.to_string(),
        })
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_ok(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let keeper = make_keeper_with_policy(keeper.await).await;
        let info = get_lifecycle_policy(
            State(keeper),
            policy_path("bucket-1", "test-policy"),
            headers,
        )
        .await
        .unwrap()
        .0;
        assert_eq!(info.info.name, "test-policy");
        assert_eq!(info.settings.bucket, "bucket-1");
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_not_found(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let err = get_lifecycle_policy(
            State(keeper.await),
            policy_path("bucket-1", "no-such-policy"),
            headers,
        )
        .await
        .unwrap_err();
        assert_eq!(err.status(), NotFound);
    }

}
