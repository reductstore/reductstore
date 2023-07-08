// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::auth::policy::FullAccessPolicy;
use crate::auth::proto::Token;
use crate::core::status::HttpError;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::HttpServerState;
use axum::extract::{Path, State};
use axum::headers::HeaderMap;
use std::sync::{Arc, RwLock};

// GET /tokens/:token_name
pub async fn get_token(
    State(components): State<Arc<RwLock<HttpServerState>>>,
    Path(token_name): Path<String>,
    headers: HeaderMap,
) -> Result<Token, HttpError> {
    check_permissions(Arc::clone(&components), headers, FullAccessPolicy {})?;

    let components = components.read().unwrap();
    components.token_repo.find_by_name(&token_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::http_frontend::token_api::tests::{components, headers};

    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_get_token(components: Arc<RwLock<HttpServerState>>, headers: HeaderMap) {
        let token = get_token(State(components), Path("test".to_string()), headers)
            .await
            .unwrap();
        assert_eq!(token.name, "test");
    }
}
