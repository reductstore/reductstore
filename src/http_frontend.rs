// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
//

use crate::asset::asset_manager::ZipAssetManager;
use crate::auth::token_auth::TokenAuthorization;
use crate::auth::token_repository::TokenRepository;
use crate::core::status::HttpError;
use crate::storage::storage::Storage;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use prost::DecodeError;
use serde::de::StdError;

pub mod bucket_api;
pub mod entry_api;
pub mod middleware;
pub mod server_api;
pub mod token_api;

pub struct HttpServerComponents {
    pub storage: Storage,
    pub auth: TokenAuthorization,
    pub token_repo: TokenRepository,
    pub console: ZipAssetManager,
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let body = format!("{{\"detail\": \"{}\"}}", self.message.to_string());

        // its often easiest to implement `IntoResponse` by calling other implementations
        let mut resp = (StatusCode::from_u16(self.status as u16).unwrap(), body).into_response();
        resp.headers_mut()
            .insert("content-type", "application/json".parse().unwrap());
        resp.headers_mut()
            .insert("x-reduct-error", self.message.parse().unwrap());
        resp
    }
}

impl From<DecodeError> for HttpError {
    fn from(err: DecodeError) -> Self {
        HttpError::unprocessable_entity(&format!("Failed to serialize data: {}", err))
    }
}

impl From<serde_json::Error> for HttpError {
    fn from(err: serde_json::Error) -> Self {
        HttpError::unprocessable_entity(&format!("Invalid JSON: {}", err))
    }
}

impl StdError for HttpError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        None
    }
}
