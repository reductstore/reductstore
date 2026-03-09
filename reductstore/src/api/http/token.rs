// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

mod create;
mod get;
mod list;
pub mod me;
mod remove;

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
use crate::api::http::{HttpError, StateKeeper};

use reduct_base::msg::token_api::{
    Permissions, Token, TokenCreateRequest, TokenCreateResponse, TokenList,
};
use reduct_macros::{IntoResponse, Twin};

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
}

// compatibility with v1, remove in v2
fn parse_token_create_request(body: Bytes) -> Result<TokenCreateRequest, HttpError> {
    let v: serde_json::Value = serde_json::from_slice(&body)
        .map_err(|_| HttpError::new(ErrorCode::UnprocessableEntity, "Invalid body"))?;

    if v.get("permissions").is_some() {
        // v2 format
        serde_json::from_value::<TokenCreateRequest>(v)
            .map_err(|_| HttpError::new(ErrorCode::UnprocessableEntity, "Invalid body"))
    } else {
        // v1 format: body IS permissions
        let permissions_value = match v {
            serde_json::Value::Object(ref map) => {
                let is_permissions_shape = map
                    .keys()
                    .all(|key| matches!(key.as_str(), "full_access" | "read" | "write"));
                if !is_permissions_shape {
                    return Err(HttpError::new(
                        ErrorCode::UnprocessableEntity,
                        "Invalid body",
                    ));
                }
                v
            }
            _ => {
                return Err(HttpError::new(
                    ErrorCode::UnprocessableEntity,
                    "Invalid body",
                ));
            }
        };

        let permissions: Permissions = serde_json::from_value(permissions_value)
            .map_err(|_| HttpError::new(ErrorCode::UnprocessableEntity, "Invalid body"))?;

        Ok(TokenCreateRequest {
            permissions,
            expires_in: None,
        })
    }
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
                r#"{"permissions":{"full_access":true,"read":["bucket-1"],"write":[]}, "expires_in":"5D"}"#,
            ))
            .unwrap();

        let parsed = TokenCreateRequestAxum::from_request(req, &())
            .await
            .unwrap();
        assert_eq!(parsed.0.permissions.full_access, true);
        assert_eq!(parsed.0.permissions.read, vec!["bucket-1"]);
        assert_eq!(parsed.0.expires_in, Some("5D".to_string()));
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
            .body(Body::from(r#"{"expires_in":"5D"}"#))
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
        assert_eq!(parsed.0.expires_in, None);
    }
}
