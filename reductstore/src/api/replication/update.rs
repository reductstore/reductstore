// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::middleware::check_permissions;
use crate::api::replication::ReplicationSettingsAxum;
use crate::api::{Components, HttpError};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;
use std::sync::Arc;

// PUT /api/v1/replications/:replication_name
pub(crate) async fn update_replication(
    State(components): State<Arc<Components>>,
    Path(replication_name): Path<String>,
    headers: HeaderMap,
    settings: ReplicationSettingsAxum,
) -> Result<(), HttpError> {
    check_permissions(&components, headers, FullAccessPolicy {}).await?;

    components
        .replication_repo
        .write()
        .await
        .update_replication(&replication_name, settings.into())
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::replication::tests::settings;
    use crate::api::tests::{components as base_components, headers};
    use reduct_base::msg::replication_api::ReplicationSettings;
    use rstest::{fixture, rstest};
    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_update_replication_ok(
        #[future] components: Arc<Components>,
        headers: HeaderMap,
        mut settings: ReplicationSettings,
    ) {
        let components = components.await;
        settings.dst_bucket = "bucket-3".to_string();

        update_replication(
            State(Arc::clone(&components)),
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
        #[future] components: Arc<Components>,
        headers: HeaderMap,
        mut settings: ReplicationSettings,
    ) {
        let components = components.await;
        settings.dst_host = "BROKEN URL".to_string();

        let result = update_replication(
            State(Arc::clone(&components)),
            Path("test".to_string()),
            headers,
            ReplicationSettingsAxum::from(settings),
        )
        .await;

        assert!(result.is_err());
    }

    #[fixture]
    async fn components(
        #[future] base_components: Arc<Components>,
        settings: ReplicationSettings,
    ) -> Arc<Components> {
        let components = base_components.await;
        components
            .replication_repo
            .write()
            .await
            .create_replication("test", settings)
            .await
            .unwrap();
        components
    }
}
