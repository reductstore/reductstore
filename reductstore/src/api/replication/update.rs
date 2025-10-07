// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::replication::ReplicationSettingsAxum;
use crate::api::{HttpError, StateKeeper};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;
use std::sync::Arc;

// PUT /api/v1/replications/:replication_name
pub(super) async fn update_replication(
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
        .update_replication(&replication_name, settings.into())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::replication::tests::settings;
    use crate::api::tests::{headers, keeper};
    use reduct_base::msg::replication_api::ReplicationSettings;
    use rstest::{fixture, rstest};
    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_update_replication_ok(
        #[future] keeper_with_bucket: Arc<StateKeeper>,
        headers: HeaderMap,
        mut settings: ReplicationSettings,
    ) {
        let keeper = keeper_with_bucket.await;
        let components = keeper.get_anonymous().await.unwrap();
        settings.dst_bucket = "bucket-3".to_string();

        update_replication(
            State(Arc::clone(&keeper)),
            Path("test".to_string()),
            headers,
            ReplicationSettingsAxum::from(settings),
        )
        .await
        .unwrap();

        assert_eq!(
            components
                .replication_repo
                .read()
                .await
                .get_replication("test")
                .unwrap()
                .settings()
                .dst_bucket,
            "bucket-3"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_replication_error(
        #[future] keeper_with_bucket: Arc<StateKeeper>,
        headers: HeaderMap,
        mut settings: ReplicationSettings,
    ) {
        settings.dst_host = "BROKEN URL".to_string();

        let result = update_replication(
            State(keeper_with_bucket.await),
            Path("test".to_string()),
            headers,
            ReplicationSettingsAxum::from(settings),
        )
        .await;

        assert!(result.is_err());
    }

    #[fixture]
    async fn keeper_with_bucket(
        #[future] keeper: Arc<StateKeeper>,
        settings: ReplicationSettings,
    ) -> Arc<StateKeeper> {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        components
            .replication_repo
            .write()
            .await
            .create_replication("test", settings)
            .unwrap();
        keeper
    }
}
