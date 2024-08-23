// Copyright 2023 ReductSoftware UG
// Licensed under the Business Source License 1.1

use std::net::{IpAddr, SocketAddr};

use axum_server::tls_rustls::RustlsConfig;

use axum_server::Handle;
use log::info;
use reductstore::cfg::Cfg;
use reductstore::core::env::StdEnvGetter;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;

use reductstore::api::create_axum_app;
use reductstore::core::logger::Logger;
use reductstore::storage::storage::Storage;

async fn launch_server() {
    let version: &str = env!("CARGO_PKG_VERSION");

    Logger::init("INFO");
    info!(
        "ReductStore {} [{} at {}]",
        version,
        env!("COMMIT"),
        env!("BUILD_TIME")
    );

    let git_ref = if version.ends_with("-dev") {
        env!("COMMIT").to_string()
    } else {
        format!("v{}", version)
    };

    let cfg = Cfg::from_env(StdEnvGetter::default());
    Logger::init(&cfg.log_level);
    info!("Configuration: \n {}", cfg);

    let components = cfg.build().await.expect("Failed to build components");
    let info = components
        .storage
        .read()
        .await
        .info()
        .await
        .expect("Failed to get server info");
    if let Some(license) = &info.license {
        info!("License Information: {}", license);
    } else {
        info!(
            "License: BUSL-1.1 [https://github.com/reductstore/reductstore/blob/{}/LICENSE]",
            git_ref
        );
    }

    let scheme = if cfg.cert_path.is_empty() {
        "http"
    } else {
        "https"
    };

    info!(
        "Run HTTP reductstore on {}://{}:{}{}",
        scheme, cfg.host, cfg.port, cfg.api_base_path
    );

    let handle = Handle::new();
    tokio::spawn(shutdown_ctrl_c(handle.clone(), components.storage.clone()));
    #[cfg(unix)]
    tokio::spawn(shutdown_signal(handle.clone(), components.storage.clone()));

    let addr = SocketAddr::new(
        IpAddr::from_str(&cfg.host).expect("Invalid host address"),
        cfg.port as u16,
    );

    let app = create_axum_app(
        &cfg.api_base_path,
        &cfg.cors_allow_origin,
        Arc::new(components),
    );

    // Ensure that the process exits with a non-zero exit code on panic.
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    if cfg.cert_path.is_empty() {
        axum_server::bind(addr)
            .handle(handle)
            .serve(app.into_make_service())
            .await
            .unwrap();
    } else {
        rustls::crypto::ring::default_provider()
            .install_default()
            .expect("Failed to install rustls crypto provider");
        let config = RustlsConfig::from_pem_file(cfg.cert_path, cfg.cert_key_path)
            .await
            .expect("Failed to load TLS certificate");
        axum_server::bind_rustls(addr, config)
            .handle(handle)
            .serve(app.into_make_service())
            .await
            .expect("Failed to start HTTPS server");
    };
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    launch_server().await;
}

async fn shutdown_ctrl_c(handle: Handle, storage: Arc<RwLock<Storage>>) {
    tokio::signal::ctrl_c().await.unwrap();
    info!("Ctrl-C received, shutting down...");

    storage
        .read()
        .await
        .sync_fs()
        .await
        .expect("Failed to shutdown storage");
    handle.shutdown();
}

#[cfg(unix)]
async fn shutdown_signal(handle: Handle, storage: Arc<RwLock<Storage>>) {
    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .unwrap()
        .recv()
        .await;
    info!("SIGTERM received, shutting down...");
    storage
        .read()
        .await
        .sync_fs()
        .await
        .expect("Failed to shutdown storage");
    handle.shutdown();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::path::PathBuf;
    use std::thread::spawn;
    use tempfile::tempdir;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_launch_http() {
        let path = tempdir().unwrap().into_path();
        env::set_var("RS_DATA_PATH", path.to_str().unwrap());

        spawn(|| {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                launch_server().await;
            });
        });

        sleep(std::time::Duration::from_millis(100)).await;
        reqwest::get("http://127.0.0.1:8383/api/v1/info")
            .await
            .expect("Failed to get info")
            .error_for_status()
            .expect("Failed to get info");
    }

    #[tokio::test]
    async fn test_launch_https() {
        let path = tempdir().unwrap().into_path();

        let project_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

        env::set_var("RS_DATA_PATH", path.to_str().unwrap());
        env::set_var(
            "RS_CERT_PATH",
            project_path
                .join("../misc/certificate.crt")
                .to_str()
                .unwrap(),
        );
        env::set_var(
            "RS_CERT_KEY_PATH",
            project_path
                .join("../misc/privateKey.key")
                .to_str()
                .unwrap(),
        );

        spawn(|| {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                launch_server().await;
            });
        });

        sleep(std::time::Duration::from_millis(100)).await;
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();

        client
            .get("https://127.0.0.1:8383/api/v1/info")
            .send()
            .await
            .expect("Failed to get info")
            .error_for_status()
            .expect("Failed to get info");
    }
}
