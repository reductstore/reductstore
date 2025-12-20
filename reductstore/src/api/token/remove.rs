// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::{HttpError, StateKeeper};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;
use std::sync::Arc;

// DELETE /tokens/:name
pub(super) async fn remove_token(
    State(keeper): State<Arc<StateKeeper>>,
    Path(token_name): Path<String>,
    headers: HeaderMap,
) -> Result<(), HttpError> {
    let components = keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;
    components
        .token_repo
        .write()
        .await?
        .remove_token(&token_name)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tests::{headers, keeper};

    use reduct_base::error::ErrorCode;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_remove_token(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let token = remove_token(State(keeper.await), Path("test".to_string()), headers).await;
        assert!(token.is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_token_not_found(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let err = remove_token(State(keeper.await), Path("not-found".to_string()), headers)
            .await
            .err()
            .unwrap();
        assert_eq!(
            err,
            HttpError::new(ErrorCode::NotFound, "Token 'not-found' doesn't exist")
        )
    }
}
