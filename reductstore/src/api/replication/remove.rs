// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::middleware::check_permissions;

use crate::api::{Components, HttpError};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;
use std::sync::Arc;

// DElETE /api/v1/replications/:replication_name
pub(crate) async fn remove_replication(
    State(components): State<Arc<Components>>,
    Path(replication_name): Path<String>,
    headers: HeaderMap,
) -> Result<(), HttpError> {
    check_permissions(&components, &headers, FullAccessPolicy {}).await?;

    components
        .replication_repo
        .write()
        .await
        .remove_replication(&replication_name)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::replication::tests::settings;
    use crate::api::tests::{components, headers};
    use reduct_base::error::ErrorCode::NotFound;
    use reduct_base::msg::replication_api::ReplicationSettings;
    use rstest::rstest;
    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_remove_replication_ok(
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
            .unwrap();

        remove_replication(
            State(Arc::clone(&components)),
            Path("test".to_string()),
            headers,
        )
        .await
        .unwrap();

        assert_eq!(
            components
                .replication_repo
                .read()
                .await
                .get_replication("test")
                .err()
                .unwrap()
                .status,
            NotFound
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_replication_error(
        #[future] components: Arc<Components>,
        headers: HeaderMap,
    ) {
        let components = components.await;
        let err = remove_replication(
            State(Arc::clone(&components)),
            Path("test".to_string()),
            headers,
        )
        .await
        .err()
        .unwrap();

        assert_eq!(err.0.status, NotFound, "Should handle errors");
    }
}
