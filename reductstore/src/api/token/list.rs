// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::api::middleware::check_permissions;
use crate::api::token::TokenListAxum;
use crate::api::{Componentes, HttpError};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::State;
use axum::headers::HeaderMap;

use std::sync::Arc;

// GET /tokens
pub async fn list_tokens(
    State(components): State<Arc<Componentes>>,
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

    use crate::api::tests::{components, headers};

    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_token_list(components: Arc<Componentes>, headers: HeaderMap) {
        let list = list_tokens(State(components), headers).await.unwrap().0;
        assert_eq!(list.tokens.len(), 2);
        assert_eq!(list.tokens[0].name, "init-token");
        assert!(list.tokens[0].value.is_empty(), "Token value MUST be empty");
        assert_eq!(list.tokens[1].name, "test");
        assert!(list.tokens[1].value.is_empty(), "Token value MUST be empty");
    }
}
