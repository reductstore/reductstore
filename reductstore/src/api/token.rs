// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

mod create;
mod get;
mod list;
pub mod me;
mod remove;

use axum::async_trait;
use axum::extract::FromRequest;
use axum::http::Request;
use axum::response::IntoResponse;
use bytes::Bytes;

use axum::headers::HeaderMapExt;

use axum::routing::{delete, get, post};
use reduct_base::error::ErrorCode;
use std::sync::Arc;

use crate::api::token::create::create_token;
use crate::api::token::get::get_token;
use crate::api::token::list::list_tokens;
use crate::api::token::remove::remove_token;
use crate::api::{Components, HttpError};

use reduct_base::msg::token_api::{Permissions, Token, TokenCreateResponse, TokenList};
use reduct_macros::{IntoResponse, Twin};

#[derive(IntoResponse, Twin)]
pub struct TokenAxum(Token);

#[derive(IntoResponse, Twin)]
pub struct TokenCreateResponseAxum(TokenCreateResponse);

#[derive(IntoResponse, Twin, Default)]
pub struct TokenListAxum(TokenList);

#[derive(IntoResponse, Twin)]
pub struct PermissionsAxum(Permissions);

#[async_trait]
impl<S, B> FromRequest<S, B> for PermissionsAxum
where
    Bytes: FromRequest<S, B>,
    B: Send + 'static,
    S: Send + Sync,
{
    type Rejection = HttpError;

    async fn from_request(req: Request<B>, state: &S) -> Result<Self, Self::Rejection> {
        let bytes = Bytes::from_request(req, state)
            .await
            .map_err(|_| HttpError::new(ErrorCode::UnprocessableEntity, "Invalid body"))?;
        let response = match serde_json::from_slice::<Permissions>(&*bytes) {
            Ok(x) => Ok(PermissionsAxum::from(x)),
            Err(e) => Err(HttpError::from(e)),
        };

        response
    }
}

pub(crate) fn create_token_api_routes() -> axum::Router<Arc<Components>> {
    axum::Router::new()
        .route("/", get(list_tokens))
        .route("/:token_name", post(create_token))
        .route("/:token_name", get(get_token))
        .route("/:token_name", delete(remove_token))
}
