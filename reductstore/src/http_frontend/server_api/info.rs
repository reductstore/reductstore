// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::auth::policy::AuthenticatedPolicy;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::{HttpError, HttpServerState};
use crate::storage::proto::ServerInfo;
use axum::extract::State;
use axum::headers::HeaderMap;
use std::sync::{Arc, RwLock};

// GET /info
pub async fn info(
    State(components): State<Arc<HttpServerState>>,
    headers: HeaderMap,
) -> Result<ServerInfo, HttpError> {
    check_permissions(&components, headers, AuthenticatedPolicy {}).await?;
    Ok(components.storage.read().await.info()?)
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
        assert_eq!(info.bucket_count, 2);
    }
}
