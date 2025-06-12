// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::middleware::check_permissions;
use crate::api::token::{PermissionsAxum, TokenCreateResponseAxum};
use crate::api::{Components, HttpError};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;
use std::sync::Arc;

// POST /tokens/:token_name
pub(crate) async fn create_token(
    State(components): State<Arc<Components>>,
    Path(token_name): Path<String>,
    headers: HeaderMap,
    permissions: PermissionsAxum,
) -> Result<TokenCreateResponseAxum, HttpError> {
    check_permissions(&components, &headers, FullAccessPolicy {}).await?;

    Ok(TokenCreateResponseAxum(
        components
            .token_repo
            .write()
            .await
            .generate_token(&token_name, permissions.into())?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::api::tests::{components, headers};

    use reduct_base::error::ErrorCode;
    use reduct_base::msg::token_api::Permissions;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_create_token(#[future] components: Arc<Components>, headers: HeaderMap) {
        let token = create_token(
            State(components.await),
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
        #[future] components: Arc<Components>,
        headers: HeaderMap,
    ) {
        let err = create_token(
            State(components.await),
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
