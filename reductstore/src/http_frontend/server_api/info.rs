// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::auth::policy::AuthenticatedPolicy;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::server_api::ServerInfoAxum;
use crate::http_frontend::{HttpError, HttpServerState};
use axum::extract::State;
use axum::headers::HeaderMap;
use axum::Json;
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
