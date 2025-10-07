// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::token::TokenAxum;
use crate::api::{HttpError, StateKeeper};
use crate::auth::policy::AuthenticatedPolicy;

use axum::extract::State;
use axum_extra::headers::HeaderMap;
use std::sync::Arc;

// // GET /me
pub(in crate::api) async fn me(
    State(keeper): State<Arc<StateKeeper>>,
    headers: HeaderMap,
) -> Result<TokenAxum, HttpError> {
    let components = keeper
        .get_with_permissions(&headers, AuthenticatedPolicy {})
        .await?;
    let header = match headers.get("Authorization") {
        Some(header) => header.to_str().ok(),
        None => None,
    };
    let mut token = components.token_repo.read().await.validate_token(header)?;
    token.value.clear();
    Ok(TokenAxum::from(token))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tests::{headers, keeper};
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_me(#[future] components: Arc<Components>, headers: HeaderMap) {
        let token = me(State(components.await), headers).await.unwrap().0;
        assert_eq!(token.name, "init-token");
    }
}
