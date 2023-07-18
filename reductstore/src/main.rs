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

use axum_server::tls_rustls::RustlsConfig;

use axum_server::Handle;
use log::info;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::asset::asset_manager::ZipAssetManager;
use crate::auth::token_auth::TokenAuthorization;
use crate::auth::token_repository::create_token_repository;
use crate::storage::storage::Storage;

use crate::core::env::Env;
use crate::core::logger::Logger;

use crate::http_frontend::{create_axum_app, HttpServerState};

#[tokio::main]
async fn main() {
    let version: &str = env!("CARGO_PKG_VERSION");

    Logger::init("INFO");
    info!(
        "ReductStore {} [{} at {}]",
        version,
        env!("COMMIT"),
        env!("BUILD_TIME")
    );
    info!("License: MPL-2.0");

    let mut env = Env::new();

    let log_level = env.get::<String>("RS_LOG_LEVEL", "INFO".to_string(), false);
    let host = env.get::<String>("RS_HOST", "0.0.0.0".to_string(), false);
    let port = env.get::<i32>("RS_PORT", 8383, false);
    let api_base_path = env.get::<String>("RS_API_BASE_PATH", "/".to_string(), false);
    let data_path = env.get::<String>("RS_DATA_PATH", "/data".to_string(), false);
    let api_token = env.get::<String>("RS_API_TOKEN", "".to_string(), true);
    let cert_path = env.get::<String>("RS_CERT_PATH", "".to_string(), true);
    let cert_key_path = env.get::<String>("RS_CERT_KEY_PATH", "".to_string(), true);

    Logger::init(&log_level);

    info!("Configuration: \n {}", env.message());

    let components = HttpServerState {
        storage: RwLock::new(Storage::new(PathBuf::from(data_path.clone()))),
        auth: TokenAuthorization::new(&api_token),
        token_repo: RwLock::new(create_token_repository(
            PathBuf::from(data_path),
            &api_token,
        )),
        console: ZipAssetManager::new(include_bytes!(concat!(env!("OUT_DIR"), "/console.zip"))),
        base_path: api_base_path.clone(),
    };

    let scheme = if cert_path.is_empty() {
        "http"
    } else {
        "https"
    };
    info!(
        "Run HTTP reductstore on {}://{}:{}{}",
        scheme, host, port, api_base_path
    );

    let addr = SocketAddr::new(
        IpAddr::from_str(&host).expect("Invalid host address"),
        port as u16,
    );

    let app = create_axum_app(&api_base_path, Arc::new(components));

    let handle = Handle::new();
    tokio::spawn(shutdown_ctrl_c(handle.clone()));
    tokio::spawn(shutdown_signal(handle.clone()));

    if cert_path.is_empty() {
        axum_server::bind(addr)
            .handle(handle)
            .serve(app.into_make_service())
            .await
            .unwrap();
    } else {
        let config = RustlsConfig::from_pem_file(cert_path, cert_key_path)
            .await
            .unwrap();
        axum_server::bind_rustls(addr, config)
            .handle(handle)
            .serve(app.into_make_service())
            .await
            .unwrap();
    };
}

async fn shutdown_ctrl_c(handle: Handle) {
    tokio::signal::ctrl_c().await.unwrap();
    info!("Ctrl-C received, shutting down...");
    handle.shutdown();
}

#[cfg(unix)]
async fn shutdown_signal(handle: Handle) {
    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .unwrap()
        .recv()
        .await;
    info!("SIGTERM received, shutting down...");
    handle.shutdown();
}

#[cfg(not(unix))]
async fn shutdown_signal(handle: Handle) {
    tokio::signal::ctrl_c().await.unwrap();
    info!("Ctrl-C received, shutting down...");
    handle.shutdown();
}
