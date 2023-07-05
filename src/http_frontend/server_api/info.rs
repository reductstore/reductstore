// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::auth::policy::AuthenticatedPolicy;
use crate::core::status::HttpError;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::HttpServerState;
use crate::storage::proto::ServerInfo;
use axum::extract::State;
use axum::headers::HeaderMap;
use std::sync::{Arc, RwLock};

// GET /info
pub async fn info(
    State(components): State<Arc<RwLock<HttpServerState>>>,
    headers: HeaderMap,
) -> Result<ServerInfo, HttpError> {
    check_permissions(Arc::clone(&components), headers, AuthenticatedPolicy {})?;
    components.read().unwrap().storage.info()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_frontend::server_api::tests::components;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_info(components: Arc<RwLock<HttpServerState>>) {
        let info = info(State(components), HeaderMap::new()).await.unwrap();
        assert_eq!(info.bucket_count, 2);
    }
}
