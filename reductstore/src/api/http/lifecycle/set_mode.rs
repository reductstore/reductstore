// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::http::lifecycle::LifecycleModePayloadAxum;
use crate::api::http::{HttpError, StateKeeper};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use std::sync::Arc;

// PATCH /api/v1/lifecycles/:policy_name/mode
pub(super) async fn set_mode(
    State(keeper): State<Arc<StateKeeper>>,
    Path(policy_name): Path<String>,
    headers: HeaderMap,
    payload: LifecycleModePayloadAxum,
) -> Result<(), HttpError> {
    let components = keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;

    components
        .lifecycle_repo
        .write()
        .await?
        .set_mode(&policy_name, payload.0.mode)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::lifecycle::tests::settings;
    use crate::api::http::tests::{headers, keeper};
    use reduct_base::msg::lifecycle_api::{LifecycleMode, LifecycleModePayload, LifecycleSettings};
    use rstest::rstest;
    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_set_mode(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
        settings: LifecycleSettings,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        components
            .lifecycle_repo
            .write()
            .await
            .unwrap()
            .create_lifecycle("test", settings)
            .await
            .unwrap();

        set_mode(
            State(Arc::clone(&keeper)),
            Path("test".to_string()),
            headers,
            LifecycleModePayload {
                mode: LifecycleMode::Disabled,
            }
            .into(),
        )
        .await
        .unwrap();

        let info = components
            .lifecycle_repo
            .read()
            .await
            .unwrap()
            .get_info("test")
            .await
            .unwrap();
        assert_eq!(info.info.mode, LifecycleMode::Disabled);
    }
}
