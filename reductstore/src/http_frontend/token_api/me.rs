// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::auth::policy::AuthenticatedPolicy;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::token_api::TokenAxum;
use crate::http_frontend::{Componentes, HttpError};

use axum::extract::State;
use axum::headers::HeaderMap;
use std::sync::Arc;

// // GET /me
pub async fn me(
    State(components): State<Arc<Componentes>>,
    headers: HeaderMap,
) -> Result<TokenAxum, HttpError> {
    check_permissions(&components, headers.clone(), AuthenticatedPolicy {}).await?;
    let header = match headers.get("Authorization") {
        Some(header) => header.to_str().ok(),
        None => None,
    };
    Ok(TokenAxum::from(
        components.token_repo.read().await.validate_token(header)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::http_frontend::tests::{components, headers};
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_me(components: Arc<Componentes>, headers: HeaderMap) {
        let token = me(State(components), headers).await.unwrap().0;
        assert_eq!(token.name, "init-token");
    }
}
