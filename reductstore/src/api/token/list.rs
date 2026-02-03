// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::token::TokenListAxum;
use crate::api::{HttpError, StateKeeper};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::State;
use axum::http::HeaderMap;

use std::sync::Arc;

// GET /tokens
pub(super) async fn list_tokens(
    State(keeper): State<Arc<StateKeeper>>,
    headers: HeaderMap,
) -> Result<TokenListAxum, HttpError> {
    let components = keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;
    let mut token_repo = components.token_repo.write().await?;

    let mut list = TokenListAxum::default();
    for token in token_repo.get_token_list().await?.iter() {
        let mut x = token.clone();
        x.value.clear();
        list.0.tokens.push(x);
    }
    list.0.tokens.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(list)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::api::tests::{headers, keeper};

    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_token_list(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let list = list_tokens(State(keeper.await), headers).await.unwrap().0;
        assert_eq!(list.tokens.len(), 2);
        assert_eq!(list.tokens[0].name, "init-token");
        assert!(list.tokens[0].value.is_empty(), "Token value MUST be empty");
        assert_eq!(list.tokens[1].name, "test");
        assert!(list.tokens[1].value.is_empty(), "Token value MUST be empty");
    }
}
