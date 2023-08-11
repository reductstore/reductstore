// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::auth::policy::AuthenticatedPolicy;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::server_api::ServerInfoAxum;
use crate::http_frontend::{HttpError, HttpServerState};
use axum::extract::State;
use axum::headers::HeaderMap;

use std::sync::Arc;

// GET /info
pub async fn info(
    State(components): State<Arc<HttpServerState>>,
    headers: HeaderMap,
) -> Result<ServerInfoAxum, HttpError> {
    check_permissions(&components, headers, AuthenticatedPolicy {}).await?;
    Ok(ServerInfoAxum::from(
        components.storage.read().await.info()?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_frontend::tests::{components, headers};
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_info(components: Arc<HttpServerState>, headers: HeaderMap) {
        let info = info(State(components), headers).await.unwrap();
        assert_eq!(info.0.bucket_count, 2);
    }
}
