// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::auth::policy::FullAccessPolicy;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::token_api::{PermissionsAxum, TokenCreateResponseAxum};
use crate::http_frontend::{HttpError, HttpServerState};
use axum::extract::{Path, State};
use axum::headers::HeaderMap;

use std::sync::Arc;

// POST /tokens/:token_name
pub async fn create_token(
    State(components): State<Arc<HttpServerState>>,
    Path(token_name): Path<String>,
    headers: HeaderMap,
    permissions: PermissionsAxum,
) -> Result<TokenCreateResponseAxum, HttpError> {
    check_permissions(&components, headers, FullAccessPolicy {}).await?;

    Ok(TokenCreateResponseAxum(
        components
            .token_repo
            .write()
            .await
            .create_token(&token_name, permissions.into())?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::http_frontend::tests::{components, headers};

    use reduct_base::error::ErrorCode;
    use reduct_base::msg::token_api::Permissions;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_create_token(components: Arc<HttpServerState>, headers: HeaderMap) {
        let token = create_token(
            State(components),
            Path("new-token".to_string()),
            headers,
            PermissionsAxum(Permissions::default()),
        )
        .await
        .unwrap()
        .0;
        assert!(token.value.starts_with("new-token"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_token_already_exists(
        components: Arc<HttpServerState>,
        headers: HeaderMap,
    ) {
        let err = create_token(
            State(components),
            Path("test".to_string()),
            headers,
            PermissionsAxum(Permissions::default()),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(
            err,
            HttpError::new(ErrorCode::Conflict, "Token 'test' already exists")
        );
    }
}
