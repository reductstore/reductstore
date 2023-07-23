// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::auth::policy::FullAccessPolicy;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::token_api::TokenAxum;
use crate::http_frontend::{HttpError, HttpServerState};
use axum::extract::{Path, State};
use axum::headers::HeaderMap;
use std::sync::Arc;

// GET /tokens/:token_name
pub async fn get_token(
    State(components): State<Arc<HttpServerState>>,
    Path(token_name): Path<String>,
    headers: HeaderMap,
) -> Result<TokenAxum, HttpError> {
    check_permissions(&components, headers, FullAccessPolicy {}).await?;

    Ok(TokenAxum::from(
        components
            .token_repo
            .read()
            .await
            .find_by_name(&token_name)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::http_frontend::tests::{components, headers};

    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_get_token(components: Arc<HttpServerState>, headers: HeaderMap) {
        let token = get_token(State(components), Path("test".to_string()), headers)
            .await
            .unwrap()
            .0;
        assert_eq!(token.name, "test");
    }
}
