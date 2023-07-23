// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod create;
mod get;
mod list;
pub mod me;
mod remove;

use axum::extract::FromRequest;
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{async_trait, headers};
use bytes::Bytes;

use axum::headers::HeaderMapExt;
use hyper::HeaderMap;

use axum::routing::{delete, get, post};
use reduct_base::error::ErrorCode;
use std::sync::Arc;

use crate::http_frontend::token_api::create::create_token;
use crate::http_frontend::token_api::get::get_token;
use crate::http_frontend::token_api::list::list_tokens;
use crate::http_frontend::token_api::remove::remove_token;
use crate::http_frontend::{HttpError, HttpServerState};

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

pub fn create_token_api_routes() -> axum::Router<Arc<HttpServerState>> {
    axum::Router::new()
        .route("/", get(list_tokens))
        .route("/:token_name", post(create_token))
        .route("/:token_name", get(get_token))
        .route("/:token_name", delete(remove_token))
        .route("/me", get(me::me))
}
