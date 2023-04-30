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
use crate::http_frontend::http_server::{HttpServer, HttpServerComponents};
use crate::storage::storage::Storage;
use hyper::server::conn::http1;
use log::info;
use tokio::net::TcpListener;

fn main() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build runtime");

    // Combine it with a `LocalSet,  which means it can spawn !Send futures...
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, run())
}

async fn run() {
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

    let server = HttpServer::new(
        Arc::new(RwLock::new(components)),
        api_base_path,
        cert_path,
        cert_key_path,
    );

    let addr = SocketAddr::new(
        IpAddr::from_str(&host).expect("Invalid host address"),
        port as u16,
    );

    let listener = TcpListener::bind(addr)
        .await
        .expect("Failed to bind to address");

    loop {
        let (stream, _) = listener
            .accept()
            .await
            .expect("Failed to accept connection");
        let server = server.clone();
        tokio::task::spawn_local(async move {
            if let Err(err) = http1::Builder::new().serve_connection(stream, server).await {
                println!("Failed to serve connection: {:?}", err);
            }
        });
    }
}
