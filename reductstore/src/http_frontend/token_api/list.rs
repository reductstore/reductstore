// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::auth::policy::FullAccessPolicy;
use crate::auth::proto::TokenRepo;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::{HttpError, HttpServerState};
use axum::extract::State;
use axum::headers::HeaderMap;
use std::sync::{Arc, RwLock};

// GET /tokens
pub async fn list_tokens(
    State(components): State<Arc<HttpServerState>>,
    headers: HeaderMap,
) -> Result<TokenRepo, HttpError> {
    check_permissions(&components, headers, FullAccessPolicy {}).await?;
    let token_repo = components.token_repo.read().await;

    let mut list = TokenRepo::default();
    for x in token_repo.get_token_list()?.iter() {
        list.tokens.push(x.clone());
    }
    list.tokens.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(list)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::http_frontend::tests::{components, headers};

    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_token_list(components: Arc<HttpServerState>, headers: HeaderMap) {
        let list = list_tokens(State(components), headers).await.unwrap();
        assert_eq!(list.tokens.len(), 2);
        assert_eq!(list.tokens[0].name, "init-token");
        assert_eq!(list.tokens[1].name, "test");
    }
}
