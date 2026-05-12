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

// PUT /b/:bucket_name/lifecycle-policies/:policy_id
pub(super) async fn update_lifecycle_policy(
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

    let existing = components
        .lifecycle_repo
        .read()
        .await?
        .get_lifecycle_settings(&policy_id)
        .await?;
    let body = body.0;
    let settings: LifecycleSettings = serde_json::from_value(json!({
        "bucket": bucket_name,
        "entries": body.entries,
        "max_age": body.max_age,
        "when": body.when,
        "interval": existing.interval,
        "type": existing.lifecycle_type,
        "mode": existing.mode,
    }))
    ?;
    components
        .lifecycle_repo
        .write()
        .await?
        .update_lifecycle(&policy_id, settings)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::lifecycle::{BucketLifecyclePolicyBody, BucketLifecyclePolicyBodyAxum};
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

    fn update_body(max_age: &str) -> BucketLifecyclePolicyBodyAxum {
        BucketLifecyclePolicyBodyAxum::from(BucketLifecyclePolicyBody {
            entries: vec![],
            max_age: max_age.to_string(),
            when: None,
        })
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_ok(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let keeper = make_keeper_with_policy(keeper.await).await;
        let components = keeper.get_anonymous().await.unwrap();

        update_lifecycle_policy(
            State(Arc::clone(&keeper)),
            policy_path("bucket-1", "test-policy"),
            headers,
            update_body("7d"),
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
        assert_eq!(settings.max_age, "7d");
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_preserves_interval_when_omitted(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
    ) {
        let keeper = make_keeper_with_policy(keeper.await).await;
        let components = keeper.get_anonymous().await.unwrap();

        update_lifecycle_policy(
            State(Arc::clone(&keeper)),
            policy_path("bucket-1", "test-policy"),
            headers,
            update_body("7d"),
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
        assert_eq!(settings.interval, "1h");
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_not_found(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let err = update_lifecycle_policy(
            State(keeper.await),
            policy_path("bucket-1", "no-such-policy"),
            headers,
            update_body("1d"),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status(), NotFound);
    }

}
