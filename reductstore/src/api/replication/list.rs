// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use std::sync::Arc;

use axum::extract::State;
use axum_extra::headers::HeaderMap;

use crate::api::middleware::check_permissions;
use crate::api::replication::ReplicationListAxum;
use crate::api::{Components, HttpError};
use crate::auth::policy::FullAccessPolicy;

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
    use std::sync::Arc;

    use rstest::rstest;

    use crate::api::tests::{components, headers};

    use super::*;

    #[rstest]
    #[tokio::test]
    async fn test_list_replications_ok(#[future] components: Arc<Components>, headers: HeaderMap) {
        let components = components.await;
        let list = list_replications(State(Arc::clone(&components)), headers)
            .await
            .unwrap()
            .0;

        assert_eq!(list.replications.len(), 1);
        assert_eq!(list.replications[0].name, "api-test");
        assert_eq!(list.replications[0].is_active, false);
        assert_eq!(list.replications[0].is_provisioned, false);
    }
}
