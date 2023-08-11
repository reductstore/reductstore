// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::auth::policy::FullAccessPolicy;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::token_api::TokenListAxum;
use crate::http_frontend::{HttpError, HttpServerState};
use axum::extract::State;
use axum::headers::HeaderMap;

use std::sync::Arc;

// GET /tokens
pub async fn list_tokens(
    State(components): State<Arc<HttpServerState>>,
    headers: HeaderMap,
) -> Result<TokenListAxum, HttpError> {
    check_permissions(&components, headers, FullAccessPolicy {}).await?;
    let token_repo = components.token_repo.read().await;

    let mut list = TokenListAxum::default();
    for x in token_repo.get_token_list()?.iter() {
        list.0.tokens.push((*x).clone());
    }
    list.0.tokens.sort_by(|a, b| a.name.cmp(&b.name));
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
        let list = list_tokens(State(components), headers).await.unwrap().0;
        assert_eq!(list.tokens.len(), 2);
        assert_eq!(list.tokens[0].name, "init-token");
        assert_eq!(list.tokens[1].name, "test");
    }
}
