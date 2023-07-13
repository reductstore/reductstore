// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
//

use std::sync::{Arc, RwLock};

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{middleware::from_fn, Router};
use prost::DecodeError;
use serde::de::StdError;

use crate::asset::asset_manager::ZipAssetManager;
use crate::auth::token_auth::TokenAuthorization;
use crate::auth::token_repository::ManageTokens;
use crate::core::status::HttpError;
use crate::http_frontend::bucket_api::create_bucket_api_routes;
use crate::http_frontend::entry_api::create_entry_api_routes;
use crate::http_frontend::middleware::{default_headers, print_statuses};
use crate::http_frontend::server_api::create_server_api_routes;
use crate::http_frontend::token_api::create_token_api_routes;
use crate::http_frontend::ui_api::{redirect_to_index, show_ui};
use crate::storage::storage::Storage;

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
        HttpError::internal_server_error(&format!("Internal reductsore error: {}", err))
    }
}

pub fn create_axum_app(api_base_path: &String, components: HttpServerState) -> Router {
    let b_route = create_bucket_api_routes().merge(create_entry_api_routes());

    let app = Router::new()
        // Server API
        .nest(
            &format!("{}api/v1", api_base_path),
            create_server_api_routes(),
        )
        // Token API
        .nest(
            &format!("{}api/v1/tokens", api_base_path),
            create_token_api_routes(),
        )
        // Bucket API + Entry API
        .nest(&format!("{}api/v1/b", api_base_path), b_route)
        // UI
        .route(&format!("{}", api_base_path), get(redirect_to_index))
        .fallback(get(show_ui))
        .layer(from_fn(default_headers))
        .layer(from_fn(print_statuses))
        .with_state(Arc::new(RwLock::new(components)));
    app
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use axum::extract::Path;
    use axum::headers::{Authorization, HeaderMap, HeaderMapExt};
    use bytes::Bytes;
    use rstest::fixture;

    use crate::asset::asset_manager::ZipAssetManager;
    use crate::auth::proto::token::Permissions;
    use crate::auth::token_auth::TokenAuthorization;
    use crate::auth::token_repository::create_token_repository;
    use crate::storage::proto::BucketSettings;
    use crate::storage::storage::Storage;
    use crate::storage::writer::Chunk;

    use super::*;

    #[fixture]
    pub(crate) fn components() -> Arc<RwLock<HttpServerState>> {
        let data_path = tempfile::tempdir().unwrap().into_path();

        let mut components = HttpServerState {
            storage: Storage::new(PathBuf::from(data_path.clone())),
            auth: TokenAuthorization::new("inti-token"),
            token_repo: create_token_repository(data_path.clone(), "init-token"),
            console: ZipAssetManager::new(include_bytes!(concat!(env!("OUT_DIR"), "/console.zip"))),
            base_path: "/".to_string(),
        };

        components
            .storage
            .create_bucket("bucket-1", BucketSettings::default())
            .unwrap();
        components
            .storage
            .create_bucket("bucket-2", BucketSettings::default())
            .unwrap();

        let labels = HashMap::from_iter(vec![
            ("x".to_string(), "y".to_string()),
            ("b".to_string(), "[a,b]".to_string()),
        ]);
        components
            .storage
            .get_bucket("bucket-1")
            .unwrap()
            .begin_write("entry-1", 0, 6, "text/plain".to_string(), labels)
            .unwrap()
            .write()
            .unwrap()
            .write(Chunk::Last(Bytes::from("Hey!!!")))
            .unwrap();

        let permissions = Permissions {
            read: vec!["bucket-1".to_string(), "bucket-2".to_string()],
            ..Default::default()
        };
        components
            .token_repo
            .create_token("test", permissions)
            .unwrap();

        Arc::new(RwLock::new(components))
    }

    #[fixture]
    pub(crate) fn headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.typed_insert(Authorization::bearer("init-token").unwrap());
        headers
    }

    #[fixture]
    pub fn path_to_entry_1() -> Path<HashMap<String, String>> {
        let path = Path(HashMap::from_iter(vec![
            ("bucket_name".to_string(), "bucket-1".to_string()),
            ("entry_name".to_string(), "entry-1".to_string()),
        ]));
        path
    }
}
