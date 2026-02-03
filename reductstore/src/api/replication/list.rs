// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use std::sync::Arc;

use axum::extract::State;
use axum_extra::headers::HeaderMap;

use crate::api::replication::ReplicationListAxum;
use crate::api::{HttpError, StateKeeper};
use crate::auth::policy::FullAccessPolicy;

// GET /api/v1/replications/
pub(super) async fn list_replications(
    State(keeper): State<Arc<StateKeeper>>,
    headers: HeaderMap,
) -> Result<ReplicationListAxum, HttpError> {
    let components = keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;
    let mut list = ReplicationListAxum::default();

    for x in components
        .replication_repo
        .read()
        .await?
        .replications()
        .await?
    {
        list.0.replications.push((x).clone());
    }

    Ok(list)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use std::sync::Arc;

    use crate::api::tests::{headers, keeper};
    use reduct_base::msg::replication_api::ReplicationMode;

    use super::*;

    #[rstest]
    #[tokio::test]
    async fn test_list_replications_ok(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let list = list_replications(State(keeper.await), headers)
            .await
            .unwrap()
            .0;

        assert_eq!(list.replications.len(), 1);
        assert_eq!(list.replications[0].name, "api-test");
        assert_eq!(list.replications[0].mode, ReplicationMode::Enabled);
        assert_eq!(list.replications[0].is_active, true);
        assert_eq!(list.replications[0].is_provisioned, false);
    }
}
