// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::http::lifecycle::LifecycleSettingsAxum;
use crate::api::http::{HttpError, StateKeeper};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use std::sync::Arc;

// PUT /api/v1/lifecycles/:policy_name
pub(super) async fn update_lifecycle_policy(
    State(keeper): State<Arc<StateKeeper>>,
    Path(policy_name): Path<String>,
    headers: HeaderMap,
    settings: LifecycleSettingsAxum,
) -> Result<(), HttpError> {
    let components = keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;

    components
        .lifecycle_repo
        .write()
        .await?
        .update_lifecycle(&policy_name, settings.into())
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::lifecycle::tests::{keeper_with_policy, settings};
    use crate::api::http::tests::{headers, keeper};
    use reduct_base::error::ErrorCode::NotFound;
    use reduct_base::msg::lifecycle_api::LifecycleSettings;
    use rstest::rstest;
    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_update_ok(
        #[future] keeper_with_policy: Arc<StateKeeper>,
        headers: HeaderMap,
        mut settings: LifecycleSettings,
    ) {
        let keeper = keeper_with_policy.await;
        let components = keeper.get_anonymous().await.unwrap();
        settings.older_than = "7d".to_string();
        settings.interval = "2h".to_string();

        update_lifecycle_policy(
            State(Arc::clone(&keeper)),
            Path("test-policy".to_string()),
            headers,
            LifecycleSettingsAxum::from(settings),
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
        assert_eq!(settings.older_than, "7d");
        assert_eq!(settings.interval, "2h");
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_not_found(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
        settings: LifecycleSettings,
    ) {
        let err = update_lifecycle_policy(
            State(keeper.await),
            Path("no-such-policy".to_string()),
            headers,
            LifecycleSettingsAxum::from(settings),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status(), NotFound);
    }
}
