// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::http::lifecycle::FullLifecyclePolicyInfoAxum;
use crate::api::http::{HttpError, StateKeeper};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use std::sync::Arc;

// GET /api/v1/lifecycles/:policy_name
pub(super) async fn get_lifecycle_policy(
    State(keeper): State<Arc<StateKeeper>>,
    Path(policy_name): Path<String>,
    headers: HeaderMap,
) -> Result<FullLifecyclePolicyInfoAxum, HttpError> {
    let components = keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;

    let info = components
        .lifecycle_repo
        .read()
        .await?
        .get_info(&policy_name)
        .await?;
    Ok(info.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::lifecycle::tests::keeper_with_policy;
    use crate::api::http::tests::{headers, keeper};
    use reduct_base::error::ErrorCode::NotFound;
    use rstest::rstest;
    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_get_ok(#[future] keeper_with_policy: Arc<StateKeeper>, headers: HeaderMap) {
        let keeper = keeper_with_policy.await;
        let info = get_lifecycle_policy(State(keeper), Path("test-policy".to_string()), headers)
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
            Path("no-such-policy".to_string()),
            headers,
        )
        .await
        .unwrap_err();
        assert_eq!(err.status(), NotFound);
    }
}
