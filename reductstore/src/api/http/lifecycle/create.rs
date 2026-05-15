// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::http::lifecycle::LifecycleSettingsAxum;
use crate::api::http::{HttpError, StateKeeper};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use std::sync::Arc;

// POST /api/v1/lifecycles/:policy_name
pub(super) async fn create_lifecycle_policy(
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
        .create_lifecycle(&policy_name, settings.into())
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::lifecycle::tests::settings;
    use crate::api::http::tests::{headers, keeper};
    use reduct_base::msg::lifecycle_api::LifecycleSettings;
    use rstest::rstest;
    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_create_ok(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
        settings: LifecycleSettings,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();

        create_lifecycle_policy(
            State(Arc::clone(&keeper)),
            Path("test-policy".to_string()),
            headers,
            LifecycleSettingsAxum::from(settings),
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
        mut settings: LifecycleSettings,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        settings.bucket = "bucket-2".to_string();

        create_lifecycle_policy(
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
        assert_eq!(settings.bucket, "bucket-2");
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_invalid_bucket(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
        mut settings: LifecycleSettings,
    ) {
        settings.bucket = "no-such-bucket".to_string();

        let result = create_lifecycle_policy(
            State(keeper.await),
            Path("test-policy".to_string()),
            headers,
            LifecycleSettingsAxum::from(settings),
        )
        .await;

        assert!(result.is_err());
    }
}
