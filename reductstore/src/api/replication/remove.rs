// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::{HttpError, StateKeeper};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;
use std::sync::Arc;

// DElETE /api/v1/replications/:replication_name
pub(super) async fn remove_replication(
    State(keeper): State<Arc<StateKeeper>>,
    Path(replication_name): Path<String>,
    headers: HeaderMap,
) -> Result<(), HttpError> {
    let components = keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;
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
    use crate::api::tests::{headers, keeper};
    use reduct_base::error::ErrorCode::NotFound;
    use reduct_base::msg::replication_api::ReplicationSettings;
    use rstest::rstest;
    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_remove_replication_ok(
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

        remove_replication(
            State(Arc::clone(&keeper)),
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
    async fn test_remove_replication_error(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let err = remove_replication(State(keeper.await), Path("test".to_string()), headers)
            .await
            .err()
            .unwrap();

        assert_eq!(err.0.status, NotFound, "Should handle errors");
    }
}
