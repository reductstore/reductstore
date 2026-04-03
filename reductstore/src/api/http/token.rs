// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

mod create;
mod get;
mod list;
pub mod me;
mod remove;
mod rotate;

use axum::extract::FromRequest;
use axum::http::Request;
use bytes::Bytes;

use axum_extra::headers::HeaderMapExt;

use axum::body::Body;
use axum::routing::{delete, get, post};
use reduct_base::error::ErrorCode;
use std::sync::Arc;

use crate::api::http::token::create::create_token;
use crate::api::http::token::get::get_token;
use crate::api::http::token::list::list_tokens;
use crate::api::http::token::remove::remove_token;
use crate::api::http::token::rotate::rotate_token;
use crate::api::http::{HttpError, StateKeeper};

use reduct_base::msg::token_api::{
    Permissions, Token, TokenCreateRequest, TokenCreateResponse, TokenList,
};
use reduct_macros::{IntoResponse, Twin};
use serde::Deserialize;

#[derive(IntoResponse, Twin)]
pub struct TokenAxum(Token);

#[derive(IntoResponse, Twin)]
pub struct TokenCreateResponseAxum(TokenCreateResponse);

#[derive(IntoResponse, Twin, Default)]
pub struct TokenListAxum(TokenList);

#[derive(IntoResponse, Twin)]
pub struct TokenCreateRequestAxum(TokenCreateRequest);

impl<S> FromRequest<S> for TokenCreateRequestAxum
where
    Bytes: FromRequest<S>,
    S: Send + Sync,
{
    type Rejection = HttpError;

    async fn from_request(req: Request<Body>, state: &S) -> Result<Self, Self::Rejection> {
        let bytes = Bytes::from_request(req, state)
            .await
            .map_err(|_| HttpError::new(ErrorCode::UnprocessableEntity, "Invalid body"))?;
        parse_token_create_request(bytes).map(TokenCreateRequestAxum::from)
    }
}

pub(super) fn create_token_api_routes() -> axum::Router<Arc<StateKeeper>> {
    axum::Router::new()
        .route("/", get(list_tokens))
        .route("/{token_name}", post(create_token))
        .route("/{token_name}", get(get_token))
        .route("/{token_name}", delete(remove_token))
        .route("/{token_name}/rotate", post(rotate_token))
}

// compatibility with v1, remove in v2
fn parse_token_create_request(body: Bytes) -> Result<TokenCreateRequest, HttpError> {
    serde_json::from_slice::<CompatTokenCreateRequest>(&body)
        .map(Into::into)
        .map_err(|_| HttpError::new(ErrorCode::UnprocessableEntity, "Invalid body"))
}

#[cfg_attr(not(test), allow(dead_code))]
fn parse_token_create_request_v2(body: Bytes) -> Result<TokenCreateRequest, HttpError> {
    serde_json::from_slice::<V2TokenCreateRequestBody>(&body)
        .map(Into::into)
        .map_err(|_| HttpError::new(ErrorCode::UnprocessableEntity, "Invalid body"))
}

#[derive(Deserialize)]
#[serde(untagged)]
enum CompatTokenCreateRequest {
    V2(V2TokenCreateRequestBody),
    V1(V1PermissionsRequest),
}

impl From<CompatTokenCreateRequest> for TokenCreateRequest {
    fn from(value: CompatTokenCreateRequest) -> Self {
        match value {
            CompatTokenCreateRequest::V2(request) => request.into(),
            CompatTokenCreateRequest::V1(request) => TokenCreateRequest {
                permissions: request.permissions,
                expires_at: None,
            },
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct V2TokenCreateRequestBody {
    permissions: Permissions,
    #[serde(default)]
    expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<V2TokenCreateRequestBody> for TokenCreateRequest {
    fn from(value: V2TokenCreateRequestBody) -> Self {
        TokenCreateRequest {
            permissions: value.permissions,
            expires_at: value.expires_at,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct V1PermissionsRequest {
    #[serde(flatten)]
    permissions: Permissions,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_token_create_request_from_request() {
        let req = Request::builder()
            .method("POST")
            .uri("/tokens/new")
            .body(Body::from(
                r#"{"permissions":{"full_access":true,"read":["bucket-1"],"write":[]},"expires_at":"2026-03-16T10:00:00Z"}"#,
            ))
            .unwrap();

        let parsed = TokenCreateRequestAxum::from_request(req, &())
            .await
            .unwrap();
        assert_eq!(parsed.0.permissions.full_access, true);
        assert_eq!(parsed.0.permissions.read, vec!["bucket-1"]);
        assert_eq!(
            parsed.0.expires_at,
            Some("2026-03-16T10:00:00Z".parse().unwrap())
        );
    }

    #[tokio::test]
    async fn test_token_create_request_from_request_invalid() {
        let req = Request::builder()
            .method("POST")
            .uri("/tokens/new")
            .body(Body::from(r#"{"permissions":"invalid"}"#))
            .unwrap();

        let err = TokenCreateRequestAxum::from_request(req, &())
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), ErrorCode::UnprocessableEntity);
    }

    #[tokio::test]
    async fn test_token_create_request_from_request_missing_permissions() {
        let req = Request::builder()
            .method("POST")
            .uri("/tokens/new")
            .body(Body::from(r#"{"expires_at":"2026-03-16T10:00:00Z"}"#))
            .unwrap();

        let err = TokenCreateRequestAxum::from_request(req, &())
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), ErrorCode::UnprocessableEntity);
    }

    #[tokio::test]
    async fn test_token_create_request_from_request_v1_permissions_only() {
        let req = Request::builder()
            .method("POST")
            .uri("/tokens/new")
            .body(Body::from(
                r#"{"full_access":true,"read":["bucket-1"],"write":[]}"#,
            ))
            .unwrap();

        let parsed = TokenCreateRequestAxum::from_request(req, &())
            .await
            .unwrap();
        assert_eq!(parsed.0.permissions.full_access, true);
        assert_eq!(parsed.0.permissions.read, vec!["bucket-1"]);
        assert_eq!(parsed.0.permissions.write.len(), 0);
        assert_eq!(parsed.0.expires_at, None);
    }

    #[test]
    fn test_parse_token_create_request_v2_strict() {
        let parsed = parse_token_create_request_v2(Bytes::from(
            r#"{"permissions":{"full_access":true,"read":["bucket-1"],"write":[]},"expires_at":"2026-03-16T10:00:00Z"}"#,
        ))
        .unwrap();

        assert!(parsed.permissions.full_access);
        assert_eq!(parsed.permissions.read, vec!["bucket-1"]);
        assert_eq!(
            parsed.expires_at,
            Some("2026-03-16T10:00:00Z".parse().unwrap())
        );
    }

    #[test]
    fn test_parse_token_create_request_v2_rejects_v1_shape() {
        let err = parse_token_create_request_v2(Bytes::from(
            r#"{"full_access":true,"read":["bucket-1"],"write":[]}"#,
        ))
        .unwrap_err();

        assert_eq!(err.status(), ErrorCode::UnprocessableEntity);
    }

    #[test]
    fn test_parse_token_create_request_v2_rejects_expires_in() {
        let err = parse_token_create_request_v2(Bytes::from(
            r#"{"permissions":{"full_access":true,"read":["bucket-1"],"write":[]},"expires_in":"5d"}"#,
        ))
        .unwrap_err();

        assert_eq!(err.status(), ErrorCode::UnprocessableEntity);
    }

    #[test]
    fn test_parse_token_create_request_v1_uses_permissions_structure() {
        let parsed = parse_token_create_request(Bytes::from(
            r#"{"full_access":true,"read":["bucket-1"],"write":[]}"#,
        ))
        .unwrap();

        assert!(parsed.permissions.full_access);
        assert_eq!(parsed.permissions.read, vec!["bucket-1"]);
        assert_eq!(parsed.expires_at, None);
    }
}
