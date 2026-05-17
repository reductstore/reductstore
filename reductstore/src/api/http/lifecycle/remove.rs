// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::http::{HttpError, StateKeeper};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use std::sync::Arc;

// DELETE /api/v1/lifecycles/:policy_name
pub(super) async fn remove_lifecycle_policy(
    State(keeper): State<Arc<StateKeeper>>,
    Path(policy_name): Path<String>,
    headers: HeaderMap,
) -> Result<(), HttpError> {
    let components = keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;

    components
        .lifecycle_repo
        .write()
        .await?
        .remove_lifecycle(&policy_name)
        .await?;
    Ok(())
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
    async fn test_remove_ok(#[future] keeper_with_policy: Arc<StateKeeper>, headers: HeaderMap) {
        let keeper = keeper_with_policy.await;
        let components = keeper.get_anonymous().await.unwrap();

        remove_lifecycle_policy(
            State(Arc::clone(&keeper)),
            Path("test-policy".to_string()),
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
            Path("no-such-policy".to_string()),
            headers,
        )
        .await
        .unwrap_err();
        assert_eq!(
            err.status(),
            NotFound,
            "Expected NotFound error when removing non-existent policy"
        );
    }
}
