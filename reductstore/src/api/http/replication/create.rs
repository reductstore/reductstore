// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::http::replication::ReplicationSettingsAxum;
use crate::api::http::{HttpError, StateKeeper};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;
use std::sync::Arc;

// POST /api/v1/replications/:replication_name
pub(super) async fn create_replication(
    State(keeper): State<Arc<StateKeeper>>,
    Path(replication_name): Path<String>,
    headers: HeaderMap,
    settings: ReplicationSettingsAxum,
) -> Result<(), HttpError> {
    let components = keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;
    components
        .replication_repo
        .write()
        .await?
        .create_replication(&replication_name, settings.into())
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::replication::tests::settings;
    use crate::api::http::tests::{headers, keeper};
    use reduct_base::msg::replication_api::ReplicationSettings;
    use rstest::rstest;
    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_create_replication_ok(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
        settings: ReplicationSettings,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        create_replication(
            State(Arc::clone(&keeper)),
            Path("test".to_string()),
            headers,
            ReplicationSettingsAxum::from(settings),
        )
        .await
        .unwrap();

        assert!(components
            .replication_repo
            .read()
            .await
            .unwrap()
            .get_replication("test")
            .is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_replication_error(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
        mut settings: ReplicationSettings,
    ) {
        settings.dst_host = "BROKEN URL".to_string();

        let result = create_replication(
            State(keeper.await),
            Path("test".to_string()),
            headers,
            ReplicationSettingsAxum::from(settings),
        )
        .await;

        assert!(result.is_err());
    }
}
