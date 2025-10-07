// Copyright 2023-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use axum_server::tls_rustls::RustlsConfig;
use std::net::{IpAddr, SocketAddr};

use axum_server::Handle;
use log::info;
use reduct_base::logger::Logger;
use reductstore::api::AxumAppBuilder;
use reductstore::cfg::CfgParser;
use reductstore::core::env::StdEnvGetter;
use reductstore::lock_file::BoxedLockFile;
use reductstore::storage::engine::StorageEngine;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

#[derive(Clone)]
struct ContextGuard {
    server_handle: Handle,
    storage: Arc<StorageEngine>,
    lock_file: Arc<BoxedLockFile>,
}

impl ContextGuard {
    fn shutdown(self) {
        self.server_handle
            .graceful_shutdown(Some(Duration::from_secs(5)));
        self.storage.sync_fs().expect("Failed to shutdown storage");
        self.lock_file.release();
    }
}

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

    let parser = CfgParser::from_env(StdEnvGetter::default());
    Logger::init(&parser.cfg.log_level);
    info!("Configuration: \n {}", parser);

    if let Some(license) = &parser.license {
        info!("License Information: {}", license);
    } else {
        info!(
            "License: BUSL-1.1 [https://github.com/reductstore/reductstore/blob/{}/LICENSE]",
            git_ref
        );
    }

    let handle = Handle::new();
    let lock_file = Arc::new(parser.build_lock_file().unwrap());

    // Run initialization in a separate thread to avoid blocking HTTP server startup
    // if waiting for the lock file.
    let config_lock = Arc::clone(&lock_file);
    let signal_handle = handle.clone();
    let cfg = parser.cfg.clone();
    let (tx, rx) = mpsc::channel(1);
    tokio::spawn(async move {
        while config_lock.is_waiting().await {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        if config_lock.is_failed().await {
            panic!("Another ReductStore instance is holding the lock. Exiting.");
        }

        let components = parser.build().unwrap();
        let ctx = ContextGuard {
            server_handle: signal_handle.clone(),
            storage: components.storage.clone(),
            lock_file: config_lock.clone(),
        };

        tokio::spawn(shutdown_ctrl_c(ctx.clone()));
        #[cfg(unix)]
        tokio::spawn(shutdown_signal(ctx));
        #[cfg(test)]
        tokio::spawn(tests::shutdown_server(signal_handle.clone()));

        tx.send(components).await.unwrap();
    });

    info!("Public URL: {}", cfg.public_url);

    let addr = SocketAddr::new(
        IpAddr::from_str(&cfg.host).expect("Invalid host address"),
        cfg.port,
    );

    let app = AxumAppBuilder::new()
        .with_cfg(cfg.clone())
        .with_component_receiver(rx)
        .with_lock_file(lock_file)
        .build();

    // Ensure that the process exits with a non-zero exit code on panic.
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    macro_rules! apply_http_settings {
        ($server:expr) => {{
            let mut server = $server.handle(handle);
            server
                .http_builder()
                .http1()
                .max_headers(cfg.io_conf.batch_max_records + 15);
            server
                .http_builder()
                .http1()
                .max_buf_size(cfg.io_conf.batch_max_metadata_size);
            server
        }};
    }

    if cfg.cert_path.is_none() {
        apply_http_settings!(axum_server::bind(addr))
            .serve(app.into_make_service())
            .await
            .expect("Failed to start HTTP server");
    } else {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("Failed to install rustls crypto provider");
        let config = RustlsConfig::from_pem_file(
            cfg.cert_path.expect("Cert path must be set"),
            cfg.cert_key_path.expect("Cert key path must be set"),
        )
        .await
        .expect("Failed to load TLS certificate");
        apply_http_settings!(axum_server::bind_rustls(addr, config))
            .serve(app.into_make_service())
            .await
            .expect("Failed to start HTTPS server");
    };
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    launch_server().await;
}

async fn shutdown_ctrl_c(ctx: ContextGuard) {
    tokio::signal::ctrl_c().await.unwrap();
    info!("Ctrl-C received, shutting down...");
    ctx.shutdown();
}

#[cfg(unix)]
async fn shutdown_signal(ctx: ContextGuard) {
    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .unwrap()
        .recv()
        .await;
    ctx.shutdown()
}

#[cfg(test)]
mod tests {
    use super::*;
    use log::warn;
    use rstest::rstest;
    use serial_test::serial;
    use std::collections::HashMap;
    use std::env;

    use std::path::PathBuf;
    use std::sync::LazyLock;
    use std::thread::{spawn, JoinHandle};
    use tempfile::tempdir;
    use tokio::sync::Mutex;
    use tokio::time::sleep;

    static STOP_SERVER: LazyLock<Mutex<bool>> = LazyLock::new(|| Mutex::new(false));
    pub(super) async fn shutdown_server(handle: Handle) {
        while !*STOP_SERVER.lock().await {
            sleep(Duration::from_millis(10)).await;
        }
        warn!("Shutting down server");
        handle.shutdown();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_launch_http() {
        let task = set_env_and_run(HashMap::new()).await;

        reqwest::get("http://127.0.0.1:8383/api/v1/info")
            .await
            .expect("Failed to get info")
            .error_for_status()
            .expect("Failed to get info");

        // send shutdown signal
        *STOP_SERVER.lock().await = true;
        task.join().unwrap();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_launch_https() {
        let project_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let mut cfg = HashMap::new();
        cfg.insert(
            "RS_CERT_PATH".to_string(),
            project_path
                .join("../misc/certificate.crt")
                .to_str()
                .unwrap()
                .to_string(),
        );
        cfg.insert(
            "RS_CERT_KEY_PATH".to_string(),
            project_path
                .join("../misc/privateKey.key")
                .to_str()
                .unwrap()
                .to_string(),
        );

        let task = set_env_and_run(cfg).await;
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

        // send shutdown signal
        *STOP_SERVER.lock().await = true;
        task.join().unwrap();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_shutdown() {
        let data_path = tempdir().unwrap().keep();
        env::set_var("RS_DATA_PATH", data_path.to_str().unwrap());

        let handle = Handle::new();
        let cfg = CfgParser::from_env(StdEnvGetter::default()); // init file cache
        let storage = cfg.build().unwrap().storage;
        let ctx = ContextGuard {
            server_handle: handle.clone(),
            storage,
            lock_file: Arc::new(cfg.build_lock_file().unwrap()),
        };

        ctx.shutdown();
    }

    async fn set_env_and_run(cfg: HashMap<String, String>) -> JoinHandle<()> {
        let data_path = tempdir().unwrap().keep();

        env::set_var("RS_DATA_PATH", data_path.to_str().unwrap());
        env::set_var("RS_CERT_PATH", "");
        env::set_var("RS_CERT_KEY_PATH", "");

        for (key, value) in cfg {
            env::set_var(key, value);
        }

        let task = spawn(|| {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                *STOP_SERVER.lock().await = false;
                launch_server().await;
            });
        });

        sleep(Duration::from_secs(1)).await;
        task
    }
}
