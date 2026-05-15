// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::http::lifecycle::LifecyclePolicyListAxum;
use crate::api::http::{HttpError, StateKeeper};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::State;
use axum::http::HeaderMap;
use std::sync::Arc;

// GET /api/v1/lifecycles
pub(super) async fn list_lifecycle_policies(
    State(keeper): State<Arc<StateKeeper>>,
    headers: HeaderMap,
) -> Result<LifecyclePolicyListAxum, HttpError> {
    let components = keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;

    let mut list = LifecyclePolicyListAxum::default();
    for policy_info in components.lifecycle_repo.read().await?.lifecycles().await? {
        list.0.lifecycles.push(policy_info);
    }

    Ok(list)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::lifecycle::tests::keeper_with_policy;
    use crate::api::http::tests::{headers, keeper};
    use rstest::rstest;
    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_list_empty(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let list = list_lifecycle_policies(State(keeper.await), headers)
            .await
            .unwrap()
            .0;
        assert_eq!(list.lifecycles.len(), 0);
    }

    #[rstest]
    #[tokio::test]
    async fn test_list_with_policy(
        #[future] keeper_with_policy: Arc<StateKeeper>,
        headers: HeaderMap,
    ) {
        let keeper = keeper_with_policy.await;
        let list = list_lifecycle_policies(State(keeper), headers)
            .await
            .unwrap()
            .0;
        assert_eq!(list.lifecycles.len(), 1);
        assert_eq!(list.lifecycles[0].name, "test-policy");
    }
}
