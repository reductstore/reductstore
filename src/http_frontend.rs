// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
//

use crate::asset::asset_manager::ZipAssetManager;
use crate::auth::token_auth::TokenAuthorization;
use crate::auth::token_repository::ManageTokens;
use crate::core::status::HttpError;
use crate::http_frontend::bucket_api::BucketApi;
use crate::http_frontend::entry_api::EntryApi;
use crate::http_frontend::middleware::{default_headers, print_statuses};
use crate::http_frontend::server_api::create_server_api_routes;
use crate::http_frontend::token_api::{create_token_api_routes, TokenApi};
use crate::http_frontend::ui_api::UiApi;
use crate::storage::storage::Storage;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, head, post, put};
use axum::{middleware::from_fn, Router};
use prost::DecodeError;
use serde::de::StdError;
use std::sync::{Arc, RwLock};

mod bucket_api;
mod entry_api;
mod middleware;
mod server_api;
mod token_api;
mod ui_api;

pub struct HttpServerState {
    pub storage: Storage,
    pub auth: TokenAuthorization,
    pub token_repo: Box<dyn ManageTokens + Send + Sync>,
    pub console: ZipAssetManager,
    pub base_path: String,
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

impl From<axum::Error> for HttpError {
    fn from(err: axum::Error) -> Self {
        HttpError::internal_server_error(&format!("Internal server error: {}", err))
    }
}

pub fn create_axum_app(api_base_path: &String, components: HttpServerState) -> Router {
    let app = Router::new()
        // Server API
        .nest(
            &format!("{}api/v1/", api_base_path),
            create_server_api_routes(),
        )
        // Token API
        .nest(
            &format!("{}api/v1/tokens/", api_base_path),
            create_token_api_routes(),
        )
        // Bucket API
        .route(
            &format!("{}api/v1/b/:bucket_name", api_base_path),
            get(BucketApi::get_bucket),
        )
        .route(
            &format!("{}api/v1/b/:bucket_name", api_base_path),
            head(BucketApi::head_bucket),
        )
        .route(
            &format!("{}api/v1/b/:bucket_name", api_base_path),
            post(BucketApi::create_bucket),
        )
        .route(
            &format!("{}api/v1/b/:bucket_name", api_base_path),
            put(BucketApi::update_bucket),
        )
        .route(
            &format!("{}api/v1/b/:bucket_name", api_base_path),
            delete(BucketApi::remove_bucket),
        )
        // Entry API
        .route(
            &format!("{}api/v1/b/:bucket_name/:entry_name", api_base_path),
            post(EntryApi::write_record),
        )
        .route(
            &format!("{}api/v1/b/:bucket_name/:entry_name", api_base_path),
            get(EntryApi::read_single_record),
        )
        .route(
            &format!("{}api/v1/b/:bucket_name/:entry_name", api_base_path),
            head(EntryApi::read_single_record),
        )
        .route(
            &format!("{}api/v1/b/:bucket_name/:entry_name/q", api_base_path),
            get(EntryApi::query),
        )
        .route(
            &format!("{}api/v1/b/:bucket_name/:entry_name/batch", api_base_path),
            get(EntryApi::read_batched_records),
        )
        .route(
            &format!("{}api/v1/b/:bucket_name/:entry_name/batch", api_base_path),
            head(EntryApi::read_batched_records),
        )
        // UI
        .route(&format!("{}", api_base_path), get(UiApi::redirect_to_index))
        .fallback(get(UiApi::show_ui))
        .layer(from_fn(default_headers))
        .layer(from_fn(print_statuses))
        .with_state(Arc::new(RwLock::new(components)));
    app
}
