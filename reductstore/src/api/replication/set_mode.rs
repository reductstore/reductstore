// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::{HttpError, StateKeeper};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum::Json;
use axum_extra::headers::HeaderMap;
use reduct_base::msg::replication_api::ReplicationMode;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize)]
pub(super) struct ReplicationModePayload {
    pub mode: ReplicationMode,
}

// PATCH /api/v1/replications/:replication_name/mode
pub(super) async fn set_mode(
    State(keeper): State<Arc<StateKeeper>>,
    Path(replication_name): Path<String>,
    headers: HeaderMap,
    Json(payload): Json<ReplicationModePayload>,
) -> Result<(), HttpError> {
    let components = keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;

    components
        .replication_repo
        .write()
        .await
        .set_mode(&replication_name, payload.mode)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::replication::tests::settings;
    use crate::api::tests::{headers, keeper};
    use reduct_base::msg::replication_api::ReplicationSettings;
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
            .create_replication("test", settings)
            .unwrap();

        set_mode(
            State(Arc::clone(&keeper)),
            Path("test".to_string()),
            headers,
            Json(ReplicationModePayload {
                mode: ReplicationMode::Paused,
            }),
        )
        .await
        .unwrap();

        let repo = components.replication_repo.read().await;
        let repl = repo.get_replication("test").unwrap();
        assert_eq!(repl.mode(), ReplicationMode::Paused);
    }
}
