// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::server::ServerInfoAxum;
use crate::api::{HttpError, StateKeeper};
use crate::auth::policy::AuthenticatedPolicy;
use axum::extract::State;
use axum_extra::headers::HeaderMap;

use std::sync::Arc;

// GET /info
pub(super) async fn info(
    State(keeper): State<Arc<StateKeeper>>,
    headers: HeaderMap,
) -> Result<ServerInfoAxum, HttpError> {
    let components = keeper
        .get_with_permissions(&headers, AuthenticatedPolicy {})
        .await?;
    Ok(components.storage.info()?.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tests::{components, headers};
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_info(#[future] components: Arc<Components>, headers: HeaderMap) {
        let info = info(State(components.await), headers).await.unwrap();
        assert_eq!(info.0.bucket_count, 2);
    }
}
