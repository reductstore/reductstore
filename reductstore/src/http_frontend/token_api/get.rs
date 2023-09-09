// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::auth::policy::FullAccessPolicy;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::token_api::TokenAxum;
use crate::http_frontend::{Componentes, HttpError};
use axum::extract::{Path, State};
use axum::headers::HeaderMap;
use std::sync::Arc;

// GET /tokens/:token_name
pub async fn get_token(
    State(components): State<Arc<Componentes>>,
    Path(token_name): Path<String>,
    headers: HeaderMap,
) -> Result<TokenAxum, HttpError> {
    check_permissions(&components, headers, FullAccessPolicy {}).await?;

    Ok(TokenAxum::from(
        components
            .token_repo
            .read()
            .await
            .get_token(&token_name)?
            .clone(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::http_frontend::tests::{components, headers};

    use reduct_base::error::ErrorCode;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_get_token(components: Arc<Componentes>, headers: HeaderMap) {
        let token = get_token(State(components), Path("test".to_string()), headers)
            .await
            .unwrap()
            .0;
        assert_eq!(token.name, "test");
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_token_not_found(components: Arc<Componentes>, headers: HeaderMap) {
        let err = get_token(State(components), Path("not-found".to_string()), headers)
            .await
            .err()
            .unwrap();
        assert_eq!(
            err,
            HttpError::new(ErrorCode::NotFound, "Token 'not-found' doesn't exist")
        )
    }
}
