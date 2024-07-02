// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::middleware::check_permissions;
use crate::api::token::TokenAxum;
use crate::api::{Components, HttpError};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;
use std::sync::Arc;

// GET /tokens/:token_name
pub(crate) async fn get_token(
    State(components): State<Arc<Components>>,
    Path(token_name): Path<String>,
    headers: HeaderMap,
) -> Result<TokenAxum, HttpError> {
    check_permissions(&components, headers, FullAccessPolicy {}).await?;

    let mut token = components
        .token_repo
        .read()
        .await
        .get_token(&token_name)?
        .clone();
    token.value.clear();
    Ok(TokenAxum::from(token))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::api::tests::{components, headers};

    use reduct_base::error::ErrorCode;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_get_token(#[future] components: Arc<Components>, headers: HeaderMap) {
        let token = get_token(State(components.await), Path("test".to_string()), headers)
            .await
            .unwrap()
            .0;
        assert_eq!(token.name, "test");
        assert!(token.value.is_empty(), "Token value MUST be empty")
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_token_not_found(#[future] components: Arc<Components>, headers: HeaderMap) {
        let err = get_token(
            State(components.await),
            Path("not-found".to_string()),
            headers,
        )
        .await
        .err()
        .unwrap();
        assert_eq!(
            err,
            HttpError::new(ErrorCode::NotFound, "Token 'not-found' doesn't exist")
        )
    }
}
