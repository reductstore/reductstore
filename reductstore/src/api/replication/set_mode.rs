// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::replication::ReplicationModePayloadAxum;
use crate::api::{HttpError, StateKeeper};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;
use std::sync::Arc;

// PATCH /api/v1/replications/:replication_name/mode
pub(super) async fn set_mode(
    State(keeper): State<Arc<StateKeeper>>,
    Path(replication_name): Path<String>,
    headers: HeaderMap,
    payload: ReplicationModePayloadAxum,
) -> Result<(), HttpError> {
    let components = keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;

    components
        .replication_repo
        .write()
        .await?
        .set_mode(&replication_name, payload.0.mode)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::replication::tests::settings;
    use crate::api::tests::{headers, keeper};
    use reduct_base::msg::replication_api::{
        ReplicationMode, ReplicationModePayload, ReplicationSettings,
    };
    use rstest::rstest;
    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_set_mode(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
        settings: ReplicationSettings,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        components
            .replication_repo
            .write()
            .await
            .unwrap()
            .create_replication("test", settings)
            .await
            .unwrap();

        set_mode(
            State(Arc::clone(&keeper)),
            Path("test".to_string()),
            headers,
            ReplicationModePayload {
                mode: ReplicationMode::Paused,
            }
            .into(),
        )
        .await
        .unwrap();

        let info = components
            .replication_repo
            .read()
            .await
            .unwrap()
            .get_info("test")
            .await
            .unwrap();
        assert_eq!(info.info.mode, ReplicationMode::Paused);
    }
}
