// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::middleware::check_permissions;
use crate::api::server::ServerInfoAxum;
use crate::api::{Components, HttpError};
use crate::auth::policy::AuthenticatedPolicy;
use axum::extract::State;
use axum_extra::headers::HeaderMap;

use std::sync::Arc;

// GET /info
pub(crate) async fn info(
    State(components): State<Arc<Components>>,
    headers: HeaderMap,
) -> Result<ServerInfoAxum, HttpError> {
    check_permissions(&components, headers, AuthenticatedPolicy {}).await?;

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
