// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::auth::policy::AuthenticatedPolicy;
use crate::auth::proto::Token;
use crate::core::status::HttpError;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::HttpServerState;
use axum::extract::State;
use axum::headers::HeaderMap;
use std::sync::{Arc, RwLock};

// // GET /me
pub async fn me(
    State(components): State<Arc<RwLock<HttpServerState>>>,
    headers: HeaderMap,
) -> Result<Token, HttpError> {
    check_permissions(
        Arc::clone(&components),
        headers.clone(),
        AuthenticatedPolicy {},
    )?;
    let header = match headers.get("Authorization") {
        Some(header) => header.to_str().ok(),
        None => None,
    };
    components.read().unwrap().token_repo.validate_token(header)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_frontend::server_api::tests::tmp_components;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_me(tmp_components: Arc<RwLock<HttpServerState>>) {
        let token = me(State(tmp_components), HeaderMap::new()).await.unwrap();
        assert_eq!(token.name, "AUTHENTICATION-DISABLED");
    }
}
