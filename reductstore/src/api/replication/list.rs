// Copyright 2024 ReductStore
// Licensed under the Business Source License 1.1

use crate::api::middleware::check_permissions;
use crate::api::replication::{ReplicationListAxum, ReplicationSettingsAxum};
use crate::api::{Components, HttpError};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum::headers::HeaderMap;
use std::sync::Arc;

// GET /api/v1/replications/
pub(crate) async fn list_replications(
    State(components): State<Arc<Components>>,
    headers: HeaderMap,
) -> Result<ReplicationListAxum, HttpError> {
    check_permissions(&components, headers, FullAccessPolicy {}).await?;

    let mut list = ReplicationListAxum::default();

    for x in components
        .replication_repo
        .read()
        .await
        .replications()
        .await
    {
        list.0.replications.push((x).clone());
    }

    Ok(list)
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
    async fn test_list_replications_ok(
        #[future] components: Arc<Components>,
        headers: HeaderMap,
        settings: ReplicationSettings,
    ) {
        let components = components.await;
        components
            .replication_repo
            .write()
            .await
            .create_replication("test", settings)
            .await
            .unwrap();

        let list = list_replications(State(Arc::clone(&components)), headers)
            .await
            .unwrap()
            .0;

        assert_eq!(list.replications.len(), 1);
        assert_eq!(list.replications[0].name, "test");
        assert_eq!(list.replications[0].is_active, false);
        assert_eq!(list.replications[0].is_provisioned, false);
    }
}
