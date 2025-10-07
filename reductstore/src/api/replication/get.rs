// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::replication::ReplicationFullInfoAxum;
use crate::api::{HttpError, StateKeeper};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;
use std::sync::Arc;

// GET /api/v1/replications/:replication_name
pub(super) async fn get_replication(
    State(keeper): State<Arc<StateKeeper>>,
    Path(replication_name): Path<String>,
    headers: HeaderMap,
) -> Result<ReplicationFullInfoAxum, HttpError> {
    let components = keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;

    let info = components
        .replication_repo
        .read()
        .await
        .get_info(&replication_name)?;
    Ok(info.into())
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::api::replication::tests::settings;
    use crate::api::tests::{headers, keeper};
    use reduct_base::error::ErrorCode::NotFound;
    use reduct_base::msg::replication_api::{FullReplicationInfo, ReplicationSettings};
    use rstest::rstest;
    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_get_replication_ok(
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

        let info = get_replication(
            State(Arc::clone(&keeper)),
            Path("test".to_string()),
            headers,
        )
        .await
        .unwrap();

        let repo = components.replication_repo.read().await;
        let repl = repo.get_replication("test").unwrap();

        assert_eq!(
            info.0,
            FullReplicationInfo {
                info: repl.info(),
                settings: repl.masked_settings().clone(),
                diagnostics: repl.diagnostics(),
            }
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_replication_error(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let err = get_replication(State(keeper.await), Path("test".to_string()), headers)
            .await
            .err()
            .unwrap();

        assert_eq!(err.0.status, NotFound);
    }
}
