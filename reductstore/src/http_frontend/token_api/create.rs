// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::auth::policy::FullAccessPolicy;
use crate::auth::proto::token::Permissions;
use crate::auth::proto::TokenCreateResponse;
use crate::core::status::HttpError;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::HttpServerState;
use axum::extract::{Path, State};
use axum::headers::HeaderMap;
use std::sync::{Arc, RwLock};

// POST /tokens/:token_name
pub async fn create_token(
    State(components): State<Arc<HttpServerState>>,
    Path(token_name): Path<String>,
    headers: HeaderMap,
    permissions: Permissions,
) -> Result<TokenCreateResponse, HttpError> {
    check_permissions(&components, headers, FullAccessPolicy {}).await?;

    components
        .token_repo
        .write()
        .await
        .create_token(&token_name, permissions)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::http_frontend::tests::{components, headers};

    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_create_token(components: HttpServerState, headers: HeaderMap) {
        let token = create_token(
            State(components),
            Path("new-token".to_string()),
            headers,
            Permissions::default(),
        )
        .await
        .unwrap();
        assert!(token.value.starts_with("new-token"));
    }
}
