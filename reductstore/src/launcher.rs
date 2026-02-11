// Copyright 2023-2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::AxumAppBuilder;
use crate::cfg::{CfgParser, ExtCfgParser};
use crate::core::env::StdEnvGetter;
use crate::storage::engine::StorageEngine;
use axum_server::tls_rustls::RustlsConfig;
use axum_server::Handle;
use log::{error, info};
use reduct_base::logger::Logger;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

pub async fn launch_server<Parser: ExtCfgParser>(ext_cfg_pareser: Parser) {
    let version: &str = env!("CARGO_PKG_VERSION");

    Logger::init("INFO");
    info!(
        "ReductStore {} [{} at {}]",
        version,
        env!("COMMIT"),
        env!("BUILD_TIME")
    );

    let parser = CfgParser::from_env_with_ext(StdEnvGetter::default(), &ext_cfg_pareser, version).await;
    let handle = Handle::new();
    let lock_file = Arc::new(parser.build_lock_file().unwrap());

    // Run initialization in a separate thread to avoid blocking HTTP server startup
    // if waiting for the lock file.
    let config_lock = Arc::clone(&lock_file);
    let signal_handle = handle.clone();
    let cfg = parser.cfg.clone();
    let engine_config = cfg.engine_config.clone();
    let (tx, rx) = mpsc::channel(1);
    tokio::spawn(async move {
        while config_lock.is_waiting().await.unwrap_or(false) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        if config_lock.is_failed().await.unwrap_or(true) {
            panic!("Another ReductStore instance is holding the lock. Exiting.");
        }

        let components = parser.build().await.unwrap();

        if !engine_config.compaction_interval.is_zero() {
            tokio::spawn(periodical_compact_storage(
                components.storage.clone(),
                engine_config.compaction_interval,
            ));
        }

        tokio::spawn(shutdown_ctrl_c(signal_handle.clone()));
        #[cfg(unix)]
        tokio::spawn(shutdown_signal(signal_handle.clone()));
        #[cfg(test)]
        tokio::spawn(tests::shutdown_server(signal_handle.clone()));

        tx.send(components).await.unwrap();
    });

    info!("Public URL: {}", cfg.public_url);

    let addr = SocketAddr::new(
        IpAddr::from_str(&cfg.host).expect("Invalid host address"),
        cfg.port,
    );

    let (app, state_keeper) = AxumAppBuilder::new()
        .with_cfg(cfg.clone())
        .with_component_receiver(rx)
        .with_lock_file(lock_file.clone())
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
            .unwrap_or_else(|e| error!("Server error: {}", e));
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
            .unwrap_or_else(|e| error!("Server error: {}", e));
    };

    // shutdown procedure
    state_keeper
        .get_anonymous()
        .await
        .expect("Failed to access storage engine")
        .storage
        .sync_fs()
        .await
        .expect("Failed to shutdown storage");
    lock_file.release().await;
    info!("Server has been shut down.");
}

async fn shutdown_ctrl_c(server_handle: Handle<SocketAddr>) {
    tokio::signal::ctrl_c().await.unwrap();
    info!("Received Ctrl-C, shutting down server...");
    server_handle.shutdown();
}

#[cfg(unix)]
async fn shutdown_signal(server_handle: Handle<SocketAddr>) {
    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .unwrap()
        .recv()
        .await;
    info!("Received termination signal, shutting down server...");
    server_handle.shutdown();
}

#[cfg(test)]
mod test_observer {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{LazyLock, Mutex};

    pub static COMPACTION_OBSERVER: LazyLock<Mutex<Option<Arc<AtomicUsize>>>> =
        LazyLock::new(|| Mutex::new(None));

    pub fn set_compaction_observer(observer: Option<Arc<AtomicUsize>>) {
        *COMPACTION_OBSERVER.lock().unwrap() = observer;
    }

    pub fn observe_compaction_tick() {
        if let Some(observer) = COMPACTION_OBSERVER.lock().unwrap().as_ref() {
            observer.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn periodical_compact_storage(storage: Arc<StorageEngine>, sync_interval: Duration) {
    loop {
        tokio::time::sleep(sync_interval).await;
        #[cfg(test)]
        test_observer::observe_compaction_tick();
        if let Err(e) = storage.compact().await {
            log::error!("Failed to sync storage: {}", e);
        }
    }
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, LazyLock};
    use std::thread::{spawn, JoinHandle};
    use tempfile::tempdir;
    use tokio::sync::Mutex;
    use tokio::time::sleep;

    static STOP_SERVER: LazyLock<Mutex<bool>> = LazyLock::new(|| Mutex::new(false));
    pub(super) async fn shutdown_server(handle: Handle<SocketAddr>) {
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
    async fn test_compaction_task_runs_when_interval_non_zero() {
        let compactions = Arc::new(AtomicUsize::new(0));
        test_observer::set_compaction_observer(Some(compactions.clone()));

        let data_path = tempdir().unwrap().keep();
        env::set_var("RS_DATA_PATH", data_path.to_str().unwrap());
        let parser = CfgParser::from_env(StdEnvGetter::default(), "0.0.0").await;
        let storage = parser.build().await.unwrap().storage;

        let handler = tokio::spawn(periodical_compact_storage(
            storage,
            Duration::from_millis(50),
        ));

        sleep(Duration::from_millis(120)).await;
        handler.abort();
        let _ = handler.await;

        test_observer::set_compaction_observer(None);

        assert!(
            compactions.load(Ordering::Relaxed) > 0,
            "periodical_compact_storage should run when interval is non-zero"
        );
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
