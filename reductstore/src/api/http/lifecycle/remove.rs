// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::http::lifecycle::PolicyPath;
use crate::api::http::{HttpError, StateKeeper};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use std::sync::Arc;

// DELETE /b/:bucket_name/lifecycle-policies/:policy_id
pub(super) async fn remove_lifecycle_policy(
    State(keeper): State<Arc<StateKeeper>>,
    Path(PolicyPath {
        bucket_name: _,
        policy_id,
    }): Path<PolicyPath>,
    headers: HeaderMap,
) -> Result<(), HttpError> {
    let components = keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;

    components
        .lifecycle_repo
        .write()
        .await?
        .remove_lifecycle(&policy_id)
        .await?;
    Ok(())
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
    async fn test_remove_ok(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let keeper = make_keeper_with_policy(keeper.await).await;
        let components = keeper.get_anonymous().await.unwrap();

        remove_lifecycle_policy(
            State(Arc::clone(&keeper)),
            policy_path("bucket-1", "test-policy"),
            headers,
        )
        .await
        .unwrap();

        assert_eq!(
            components
                .lifecycle_repo
                .read()
                .await
                .unwrap()
                .get_info("test-policy")
                .await
                .err()
                .unwrap()
                .status,
            NotFound
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_not_found(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let err = remove_lifecycle_policy(
            State(keeper.await),
            policy_path("bucket-1", "no-such-policy"),
            headers,
        )
        .await
        .unwrap_err();
        assert_eq!(err.status(), NotFound);
    }

}
