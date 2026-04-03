// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::http::token::TokenCreateResponseAxum;
use crate::api::http::{HttpError, StateKeeper};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;
use std::sync::Arc;

// POST /tokens/:name/rotate
pub(super) async fn rotate_token(
    State(keeper): State<Arc<StateKeeper>>,
    Path(token_name): Path<String>,
    headers: HeaderMap,
) -> Result<TokenCreateResponseAxum, HttpError> {
    let components = keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;

    let token = components
        .token_repo
        .write()
        .await?
        .rotate_token(&token_name)
        .await?;

    Ok(TokenCreateResponseAxum(token))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::tests::{headers, keeper};
    use reduct_base::error::ErrorCode;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_rotate_token(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let response = rotate_token(State(keeper.await), Path("test".to_string()), headers)
            .await
            .unwrap()
            .0;
        assert!(response.value.starts_with("test-"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_rotate_token_not_found(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let err = rotate_token(State(keeper.await), Path("not-found".to_string()), headers)
            .await
            .err()
            .unwrap();

        assert_eq!(
            err,
            HttpError::new(ErrorCode::NotFound, "Token 'not-found' doesn't exist")
        );
    }
}
