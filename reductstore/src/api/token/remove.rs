// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::api::middleware::check_permissions;
use crate::api::{Components, HttpError};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum::headers::HeaderMap;
use std::sync::Arc;

// DELETE /tokens/:name
pub async fn remove_token(
    State(components): State<Arc<Components>>,
    Path(token_name): Path<String>,
    headers: HeaderMap,
) -> Result<(), HttpError> {
    check_permissions(&components, headers, FullAccessPolicy {}).await?;

    Ok(components
        .token_repo
        .write()
        .await
        .remove_token(&token_name)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tests::{components, headers};

    use reduct_base::error::ErrorCode;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_remove_token(components: Arc<Components>, headers: HeaderMap) {
        let token = remove_token(State(components), Path("test".to_string()), headers).await;
        assert!(token.is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_token_not_found(components: Arc<Components>, headers: HeaderMap) {
        let err = remove_token(State(components), Path("not-found".to_string()), headers)
            .await
            .err()
            .unwrap();
        assert_eq!(
            err,
            HttpError::new(ErrorCode::NotFound, "Token 'not-found' doesn't exist")
        )
    }
}
