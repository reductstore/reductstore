// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::components::CLIENT_IP_HEADER;
use crate::api::http::token::{populate_token_status, TokenAxum};
use crate::api::http::{HttpError, StateKeeper};
use crate::auth::policy::AuthenticatedPolicy;

use axum::extract::State;
use axum_extra::headers::HeaderMap;
use std::sync::Arc;

// // GET /me
pub(in crate::api::http) async fn me(
    State(keeper): State<Arc<StateKeeper>>,
    headers: HeaderMap,
) -> Result<TokenAxum, HttpError> {
    let components = keeper
        .get_with_permissions(&headers, AuthenticatedPolicy {})
        .await?;
    let header = match headers.get("Authorization") {
        Some(header) => header.to_str().ok(),
        None => None,
    };
    let client_ip = headers
        .get(CLIENT_IP_HEADER)
        .and_then(|header| header.to_str().ok())
        .and_then(|ip| ip.parse().ok());
    let mut token = components
        .token_repo
        .write()
        .await?
        .validate_token(header, client_ip)
        .await?;
    token.value.clear();
    populate_token_status(&mut token);
    Ok(TokenAxum::from(token))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::tests::{headers, keeper};
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_me(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let token = me(State(keeper.await), headers).await.unwrap().0;
        assert_eq!(token.name, "init-token");
    }
}
