// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::http::lifecycle::LifecyclePolicyListAxum;
use crate::api::http::{HttpError, StateKeeper};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use std::sync::Arc;

// GET /b/:bucket_name/lifecycle-policies
pub(super) async fn list_lifecycle_policies(
    State(keeper): State<Arc<StateKeeper>>,
    Path(bucket_name): Path<String>,
    headers: HeaderMap,
) -> Result<LifecyclePolicyListAxum, HttpError> {
    let components = keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;

    let mut list = LifecyclePolicyListAxum::default();
    for policy_info in components.lifecycle_repo.read().await?.lifecycles().await? {
        if components
            .lifecycle_repo
            .read()
            .await?
            .get_lifecycle_settings(&policy_info.name)
            .await?
            .bucket
            == bucket_name
        {
            list.0.lifecycles.push(policy_info);
        }
    }

    Ok(list)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::tests::{headers, keeper};
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

    #[rstest]
    #[tokio::test]
    async fn test_list_empty(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let list =
            list_lifecycle_policies(State(keeper.await), Path("bucket-1".to_string()), headers)
                .await
                .unwrap()
                .0;
        assert_eq!(list.lifecycles.len(), 0);
    }

    #[rstest]
    #[tokio::test]
    async fn test_list_with_policy(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let keeper = make_keeper_with_policy(keeper.await).await;
        let list = list_lifecycle_policies(State(keeper), Path("bucket-1".to_string()), headers)
            .await
            .unwrap()
            .0;
        assert_eq!(list.lifecycles.len(), 1);
        assert_eq!(list.lifecycles[0].name, "test-policy");
    }

    #[rstest]
    #[tokio::test]
    async fn test_list_filters_by_bucket(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let keeper = make_keeper_with_policy(keeper.await).await;
        let list = list_lifecycle_policies(State(keeper), Path("bucket-2".to_string()), headers)
            .await
            .unwrap()
            .0;
        assert_eq!(list.lifecycles.len(), 0);
    }
}
