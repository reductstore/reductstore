// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::http::lifecycle::{BucketLifecyclePolicyBodyAxum, PolicyPath};
use crate::api::http::{HttpError, StateKeeper};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use reduct_base::msg::lifecycle_api::LifecycleSettings;
use serde_json::json;
use std::sync::Arc;

// POST /b/:bucket_name/lifecycle-policies/:policy_id
pub(super) async fn create_lifecycle_policy(
    State(keeper): State<Arc<StateKeeper>>,
    Path(PolicyPath {
        bucket_name,
        policy_id,
    }): Path<PolicyPath>,
    headers: HeaderMap,
    body: BucketLifecyclePolicyBodyAxum,
) -> Result<(), HttpError> {
    let components = keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;
    let body = body.0;
    let settings: LifecycleSettings = serde_json::from_value(json!({
        "bucket": bucket_name,
        "entries": body.entries,
        "max_age": body.max_age,
        "when": body.when,
    }))
    .map_err(HttpError::from)?;
    components
        .lifecycle_repo
        .write()
        .await?
        .create_lifecycle(&policy_id, settings)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::lifecycle::tests::policy_body;
    use crate::api::http::lifecycle::BucketLifecyclePolicyBody;
    use crate::api::http::tests::{headers, keeper};
    use rstest::rstest;
    use std::sync::Arc;

    fn policy_path(bucket: &str, policy: &str) -> Path<PolicyPath> {
        Path(PolicyPath {
            bucket_name: bucket.to_string(),
            policy_id: policy.to_string(),
        })
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_ok(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
        policy_body: BucketLifecyclePolicyBody,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();

        create_lifecycle_policy(
            State(Arc::clone(&keeper)),
            policy_path("bucket-1", "test-policy"),
            headers,
            BucketLifecyclePolicyBodyAxum::from(policy_body),
        )
        .await
        .unwrap();

        assert!(components
            .lifecycle_repo
            .read()
            .await
            .unwrap()
            .get_info("test-policy")
            .await
            .is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_sets_bucket(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
        policy_body: BucketLifecyclePolicyBody,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();

        create_lifecycle_policy(
            State(Arc::clone(&keeper)),
            policy_path("bucket-1", "test-policy"),
            headers,
            BucketLifecyclePolicyBodyAxum::from(policy_body),
        )
        .await
        .unwrap();

        let settings = components
            .lifecycle_repo
            .read()
            .await
            .unwrap()
            .get_lifecycle_settings("test-policy")
            .await
            .unwrap();
        assert_eq!(settings.bucket, "bucket-1");
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_default_interval(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
        policy_body: BucketLifecyclePolicyBody,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();

        create_lifecycle_policy(
            State(Arc::clone(&keeper)),
            policy_path("bucket-1", "test-policy"),
            headers,
            BucketLifecyclePolicyBodyAxum::from(policy_body),
        )
        .await
        .unwrap();

        let settings = components
            .lifecycle_repo
            .read()
            .await
            .unwrap()
            .get_lifecycle_settings("test-policy")
            .await
            .unwrap();
        assert_eq!(settings.interval, "3600s");
    }

}
