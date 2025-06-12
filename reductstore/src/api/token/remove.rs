// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::middleware::check_permissions;
use crate::api::{Components, HttpError};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;
use std::sync::Arc;

// DELETE /tokens/:name
pub(crate) async fn remove_token(
    State(components): State<Arc<Components>>,
    Path(token_name): Path<String>,
    headers: HeaderMap,
) -> Result<(), HttpError> {
    check_permissions(&components, &headers, FullAccessPolicy {}).await?;

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
    async fn test_remove_token(#[future] components: Arc<Components>, headers: HeaderMap) {
        let token = remove_token(State(components.await), Path("test".to_string()), headers).await;
        assert!(token.is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_token_not_found(
        #[future] components: Arc<Components>,
        headers: HeaderMap,
    ) {
        let err = remove_token(
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
