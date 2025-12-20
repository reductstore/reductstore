// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::token::TokenAxum;
use crate::api::{HttpError, StateKeeper};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;
use std::sync::Arc;

// GET /tokens/:token_name
pub(super) async fn get_token(
    State(keeper): State<Arc<StateKeeper>>,
    Path(token_name): Path<String>,
    headers: HeaderMap,
) -> Result<TokenAxum, HttpError> {
    let components = keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;

    let mut token = components
        .token_repo
        .write()
        .await?
        .get_token(&token_name)?
        .clone();
    token.value.clear();
    Ok(TokenAxum::from(token))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::api::tests::{headers, keeper};

    use reduct_base::error::ErrorCode;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_get_token(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let token = get_token(State(keeper.await), Path("test".to_string()), headers)
            .await
            .unwrap()
            .0;
        assert_eq!(token.name, "test");
        assert!(token.value.is_empty(), "Token value MUST be empty")
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_token_not_found(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let err = get_token(State(keeper.await), Path("not-found".to_string()), headers)
            .await
            .err()
            .unwrap();
        assert_eq!(
            err,
            HttpError::new(ErrorCode::NotFound, "Token 'not-found' doesn't exist")
        )
    }
}
