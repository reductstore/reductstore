// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub mod asset;
pub mod auth;
pub mod core;
pub mod http_frontend;
pub mod storage;

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;

use std::str::FromStr;
use std::sync::{Arc, RwLock};

use crate::asset::asset_manager::ZipAssetManager;
use crate::auth::token_auth::TokenAuthorization;
use crate::auth::token_repository::TokenRepository;
use crate::core::env::Env;
use crate::core::logger::Logger;
use crate::core::status::HttpError;
use crate::http_frontend::http_server::HttpServerComponents;
use crate::http_frontend::server_api::ServerApi;
use crate::http_frontend::token_api::TokenApi;
use crate::storage::storage::Storage;
use axum::http::Response;
use axum::{
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, head, post},
    Json, Router,
};
use log::info;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    // todo: check graceful shutdown
    let version: &str = env!("CARGO_PKG_VERSION");

    info!("ReductStore {}", version);

    let mut env = Env::new();

    let log_level = env.get::<String>("LOG_LEVEL", "INFO".to_string(), false);
    let host = env.get::<String>("RS_HOST", "0.0.0.0".to_string(), false);
    let port = env.get::<i32>("RS_PORT", 8383, false);
    let api_base_path = env.get::<String>("RS_API_BASE_PATH", "/".to_string(), false);
    let data_path = env.get::<String>("RS_DATA_PATH", "/data".to_string(), false);
    let api_token = env.get::<String>("RS_API_TOKEN", "".to_string(), true);
    let cert_path = env.get::<String>("RS_CERT_PATH", "".to_string(), true);
    let cert_key_path = env.get::<String>("RS_CERT_KEY_PATH", "".to_string(), true);

    Logger::init(&log_level);

    info!("Configuration: \n {}", env.message());

    let components = HttpServerComponents {
        storage: Storage::new(PathBuf::from(data_path.clone())),
        auth: TokenAuthorization::new(&api_token),
        token_repo: TokenRepository::new(PathBuf::from(data_path), &api_token),
        console: ZipAssetManager::new(""),
    };

    let scheme = if cert_path.is_empty() {
        "http"
    } else {
        "https"
    };
    info!(
        "Run HTTP server on {}://{}:{}{}",
        scheme, host, port, api_base_path
    );

    let addr = SocketAddr::new(
        IpAddr::from_str(&host).expect("Invalid host address"),
        port as u16,
    );

    let app = Router::new()
        // Server API
        .route(
            &format!("{}api/v1/info", api_base_path),
            get(ServerApi::info),
        )
        .route(
            &format!("{}api/v1/list", api_base_path),
            get(ServerApi::list),
        )
        .route(&format!("{}api/v1/me", api_base_path), get(ServerApi::me))
        .route(
            &format!("{}api/v1/alive", api_base_path),
            head(|| async { StatusCode::OK }),
        )
        // Token API
        .route(
            &format!("{}api/v1/tokens", api_base_path),
            post(TokenApi::token_list),
        )
        .route(
            &format!("{}api/v1/tokens/:token_name", api_base_path),
            post(TokenApi::create_token),
        )
        .route(
            &format!("{}api/v1/tokens/:token_name", api_base_path),
            get(TokenApi::get_token),
        )
        .route(
            &format!("{}api/v1/tokens/:token_name", api_base_path),
            delete(TokenApi::remove_token),
        )
        .with_state(Arc::new(RwLock::new(components)));
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}
