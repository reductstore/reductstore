// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::replication::ReplicationSettingsAxum;
use crate::api::{HttpError, StateKeeper};
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
        .await
        .create_replication(&replication_name, settings.into())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::replication::tests::settings;
    use crate::api::tests::{components, headers};
    use reduct_base::msg::replication_api::ReplicationSettings;
    use rstest::rstest;
    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_create_replication_ok(
        #[future] components: Arc<Components>,
        headers: HeaderMap,
        settings: ReplicationSettings,
    ) {
        let components = components.await;
        create_replication(
            State(Arc::clone(&components)),
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
            .get_replication("test")
            .is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_replication_error(
        #[future] components: Arc<Components>,
        headers: HeaderMap,
        mut settings: ReplicationSettings,
    ) {
        let components = components.await;
        settings.dst_host = "BROKEN URL".to_string();

        let result = create_replication(
            State(Arc::clone(&components)),
            Path("test".to_string()),
            headers,
            ReplicationSettingsAxum::from(settings),
        )
        .await;

        assert!(result.is_err());
    }
}
