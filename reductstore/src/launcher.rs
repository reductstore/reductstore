// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::components::{Components, StateKeeper};
use crate::api::http::AxumAppBuilder;
#[cfg(feature = "zenoh-api")]
use crate::api::zenoh;
use crate::cfg::{Cfg, CfgParser, ExtCfgBounds, ExtCfgParser, InstanceRole};
use crate::core::env::StdEnvGetter;
use crate::core::file_cache::FILE_CACHE;
use crate::core::sync::set_rwlock_timeout;
use crate::lock_file::BoxedLockFile;
use crate::storage::engine::StorageEngine;
use axum::Router;
use axum_server::tls_rustls::RustlsConfig;
use axum_server::Handle;
use log::{error, info, warn};
use reduct_base::error::ReductError;
use reduct_base::logger::Logger;
use std::net::{IpAddr, SocketAddr};
use std::process::exit;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

static SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(test)]
static RW_LOCK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(not(test))]
static RW_LOCK_SHUTDOWN_TIMEOUT: Duration = Duration::from_hours(1);

pub fn maybe_print_version_and_exit() {
    if std::env::args()
        .into_iter()
        .skip(1)
        .any(|arg| matches!(arg.as_ref(), "--version" | "-V"))
    {
        println!(env!("CARGO_PKG_VERSION"));
        exit(0)
    }
}

struct ListenerTask {
    handle: Handle<SocketAddr>,
    task: JoinHandle<()>,
}

impl ListenerTask {
    async fn wait(&mut self) {
        (&mut self.task).await.expect("HTTP server task panicked");
    }
}

impl Drop for ListenerTask {
    fn drop(&mut self) {
        self.handle.shutdown();
        self.task.abort();
    }
}

struct ServerRuntime {
    component_sender: Option<mpsc::Sender<Components>>,
    listener: Option<ListenerTask>,
    state_keeper: Option<Arc<StateKeeper>>,
}

impl ServerRuntime {
    fn into_parts(mut self) -> (mpsc::Sender<Components>, ListenerTask, Arc<StateKeeper>) {
        (
            self.component_sender
                .take()
                .expect("Server runtime component sender must exist"),
            self.listener
                .take()
                .expect("Server runtime listener must exist"),
            self.state_keeper
                .take()
                .expect("Server runtime state keeper must exist"),
        )
    }
}

pub struct PreparedServer<ExtCfg: ExtCfgBounds> {
    cfg: Cfg,
    ext_cfg: ExtCfg,
    components: Components,
    lock_file: Arc<BoxedLockFile>,
    runtime: ServerRuntime,
}

impl<ExtCfg: ExtCfgBounds> PreparedServer<ExtCfg> {
    pub fn ext_cfg(&self) -> &ExtCfg {
        &self.ext_cfg
    }

    pub fn components(&self) -> &Components {
        &self.components
    }

    pub async fn launch(self) {
        let PreparedServer {
            cfg,
            ext_cfg: _ext_cfg,
            components,
            lock_file,
            runtime,
        } = self;
        let (component_sender, mut listener, state_keeper) = runtime.into_parts();
        let handle = listener.handle.clone();
        let engine_config = cfg.engine_config.clone();
        let instance_role = cfg.role.clone();

        #[cfg(not(test))]
        {
            components.replication_repo.write().await.unwrap().start();
            components
                .lifecycle_repo
                .write()
                .await
                .unwrap()
                .start()
                .await
                .unwrap();
        }

        if !engine_config.compaction_interval.is_zero() {
            tokio::spawn(periodical_compact_storage(
                components.storage.clone(),
                engine_config.compaction_interval,
            ));
        }

        if instance_role == InstanceRole::Replica
            && !engine_config.replica_update_interval.is_zero()
        {
            tokio::spawn(periodical_replica_reload(
                components.storage.clone(),
                engine_config.replica_update_interval,
            ));
        }

        tokio::spawn(shutdown_ctrl_c(handle.clone()));
        #[cfg(unix)]
        tokio::spawn(shutdown_signal(handle.clone()));
        #[cfg(test)]
        tokio::spawn(tests::shutdown_server(handle.clone()));

        #[cfg(feature = "zenoh-api")]
        let zenoh_runtime = zenoh::spawn_runtime(cfg.zenoh_api.clone(), state_keeper.clone());

        #[cfg(not(test))]
        {
            let default_panic = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info| {
                default_panic(info);
                std::process::exit(1);
            }));
        }

        component_sender.send(components).await.unwrap();
        listener.wait().await;

        #[cfg(feature = "zenoh-api")]
        if let Some(handle) = zenoh_runtime {
            handle.shutdown().await;
        }

        set_rwlock_timeout(RW_LOCK_SHUTDOWN_TIMEOUT);
        state_keeper.shutdown().await;
        FILE_CACHE.stop_sync_worker();
        drop(lock_file);
        info!("Server has been shut down.");
    }
}

pub async fn prepare_server<Parser, ExtCfg: ExtCfgBounds>(
    ext_cfg_parser: Parser,
) -> Result<PreparedServer<ExtCfg>, ReductError>
where
    Parser: ExtCfgParser<StdEnvGetter, Cfg = ExtCfg>,
{
    let version = env!("CARGO_PKG_VERSION");
    Logger::init("INFO");
    info!(
        "ReductStore Core {} [{} at {}]",
        version,
        env!("COMMIT"),
        env!("BUILD_TIME")
    );

    let parser =
        CfgParser::from_env_with_ext(StdEnvGetter::default(), &ext_cfg_parser, version).await;
    let lock_file = Arc::new(parser.build_lock_file()?);
    let runtime = start_listener(parser.cfg.clone(), Arc::clone(&lock_file));

    while lock_file.is_waiting().await.unwrap_or(false) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    if lock_file.is_failed().await.unwrap_or(true) {
        panic!("Another ReductStore instance is holding the lock. Exiting.");
    }

    let components = parser.build().await?;

    Ok(PreparedServer {
        cfg: parser.cfg,
        ext_cfg: parser.ext_cfg,
        components,
        lock_file,
        runtime,
    })
}

fn start_listener(cfg: Cfg, lock_file: Arc<BoxedLockFile>) -> ServerRuntime {
    let handle = Handle::new();
    let (component_sender, component_receiver) = mpsc::channel(1);
    let (app, state_keeper) = AxumAppBuilder::new()
        .with_cfg(cfg.clone())
        .with_component_receiver(component_receiver)
        .with_lock_file(lock_file)
        .build();

    info!("Public URL: {}", cfg.public_url);
    let server_task = tokio::spawn(serve_http(app, cfg, handle.clone()));
    ServerRuntime {
        component_sender: Some(component_sender),
        listener: Some(ListenerTask {
            handle,
            task: server_task,
        }),
        state_keeper: Some(state_keeper),
    }
}

async fn serve_http(app: Router, cfg: Cfg, handle: Handle<SocketAddr>) {
    let addr = SocketAddr::new(
        IpAddr::from_str(&cfg.host).expect("Invalid host address"),
        cfg.port,
    );

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
            .serve(app.into_make_service_with_connect_info::<SocketAddr>())
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
            .serve(app.into_make_service_with_connect_info::<SocketAddr>())
            .await
            .unwrap_or_else(|e| error!("Server error: {}", e));
    };
}

async fn shutdown_ctrl_c(server_handle: Handle<SocketAddr>) {
    tokio::signal::ctrl_c().await.unwrap();
    info!("Received Ctrl-C, shutting down server...");
    server_handle.graceful_shutdown(Some(SHUTDOWN_TIMEOUT));
}

#[cfg(unix)]
async fn shutdown_signal(server_handle: Handle<SocketAddr>) {
    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .unwrap()
        .recv()
        .await;
    info!("Received termination signal, shutting down server...");
    server_handle.graceful_shutdown(Some(SHUTDOWN_TIMEOUT));
}

#[cfg(test)]
mod test_observer {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{LazyLock, Mutex};

    pub static COMPACTION_OBSERVER: LazyLock<Mutex<Option<Arc<AtomicUsize>>>> =
        LazyLock::new(|| Mutex::new(None));
    pub static REPLICA_RELOAD_OBSERVER: LazyLock<Mutex<Option<Arc<AtomicUsize>>>> =
        LazyLock::new(|| Mutex::new(None));

    pub fn set_compaction_observer(observer: Option<Arc<AtomicUsize>>) {
        *COMPACTION_OBSERVER.lock().unwrap() = observer;
    }

    pub fn set_replica_reload_observer(observer: Option<Arc<AtomicUsize>>) {
        *REPLICA_RELOAD_OBSERVER.lock().unwrap() = observer;
    }

    pub fn observe_compaction_tick() {
        if let Some(observer) = COMPACTION_OBSERVER.lock().unwrap().as_ref() {
            observer.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn observe_replica_reload_tick() {
        if let Some(observer) = REPLICA_RELOAD_OBSERVER.lock().unwrap().as_ref() {
            observer.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn periodical_compact_storage(storage: Arc<StorageEngine>, sync_interval: Duration) {
    run_periodic_task(sync_interval, "compaction", || {
        let storage = storage.clone();
        async move {
            #[cfg(test)]
            test_observer::observe_compaction_tick();

            if let Err(e) = storage.compact().await {
                log::error!("Failed to sync storage: {}", e);
            }
        }
    })
    .await;
}

async fn periodical_replica_reload(storage: Arc<StorageEngine>, sync_interval: Duration) {
    run_periodic_task(sync_interval, "replica reload", || {
        let storage = storage.clone();
        async move {
            #[cfg(test)]
            test_observer::observe_replica_reload_tick();

            if let Err(e) = storage.reload_replica().await {
                log::error!("Failed to reload replica state: {}", e);
            }
        }
    })
    .await;
}

async fn run_periodic_task<F, Fut>(interval: Duration, task_name: &'static str, mut task: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut next_tick = tokio::time::Instant::now() + interval;

    loop {
        tokio::time::sleep_until(next_tick).await;
        let started_at = std::time::Instant::now();

        task().await;

        let execution_time = started_at.elapsed();
        if execution_time > interval {
            warn!(
                "Periodic {} took {:?}, exceeding configured interval {:?}",
                task_name, execution_time, interval
            );
        }

        next_tick += interval;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::storage_engine::StorageEngineConfig;
    use crate::cfg::Cfg;
    use crate::cfg::CoreExtCfgParser;
    use log::warn;
    use reduct_base::msg::bucket_api::BucketSettings;
    use rstest::rstest;
    use serial_test::serial;
    use std::collections::HashMap;
    use std::env;
    use std::ffi::OsString;
    use std::net::TcpListener;

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, LazyLock};
    use std::thread::{spawn, JoinHandle};
    use tempfile::tempdir;
    use tokio::sync::Mutex;
    use tokio::time::{sleep, Instant};

    static STOP_SERVER: LazyLock<Mutex<bool>> = LazyLock::new(|| Mutex::new(false));

    pub(super) async fn shutdown_server(handle: Handle<SocketAddr>) {
        while !*STOP_SERVER.lock().await {
            sleep(Duration::from_millis(10)).await;
        }
        warn!("Shutting down server");
        handle.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn prepare_server_exposes_configuration_and_unready_listener_before_launch() {
        struct EnvRestore([(&'static str, Option<OsString>); 5]);

        impl Drop for EnvRestore {
            fn drop(&mut self) {
                for (key, value) in &self.0 {
                    match value {
                        Some(value) => env::set_var(key, value),
                        None => env::remove_var(key),
                    }
                }
            }
        }

        let _env_restore = EnvRestore([
            ("RS_DATA_PATH", env::var_os("RS_DATA_PATH")),
            ("RS_HOST", env::var_os("RS_HOST")),
            ("RS_PORT", env::var_os("RS_PORT")),
            ("RS_INSTANCE_ROLE", env::var_os("RS_INSTANCE_ROLE")),
            ("RS_DISABLE_AUTH", env::var_os("RS_DISABLE_AUTH")),
        ]);

        let data_path = tempdir().unwrap().keep();
        let port_reservation = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = port_reservation.local_addr().unwrap().port();
        drop(port_reservation);
        env::set_var("RS_DATA_PATH", data_path.to_str().unwrap());
        env::set_var("RS_HOST", "127.0.0.1");
        env::set_var("RS_PORT", port.to_string());
        env::set_var("RS_INSTANCE_ROLE", "STANDALONE");
        env::set_var("RS_DISABLE_AUTH", "true");

        let prepared = prepare_server(CoreExtCfgParser).await.unwrap();

        assert_eq!(prepared.ext_cfg().role, InstanceRole::Standalone);
        assert_eq!(prepared.ext_cfg().data_path, data_path);
        assert_eq!(prepared.components().storage.info().await.unwrap().usage, 0);

        let url = format!("http://127.0.0.1:{port}/api/v1/ready");
        let deadline = Instant::now() + Duration::from_secs(5);
        let response = loop {
            match reqwest::get(&url).await {
                Ok(response) => break response,
                Err(_) if Instant::now() < deadline => sleep(Duration::from_millis(10)).await,
                Err(error) => panic!("Prepared server listener did not start: {error}"),
            }
        };
        assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
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
        let cert_path = resolve_misc_file("certificate.crt");
        let cert_key_path = resolve_misc_file("privateKey.key");
        let mut cfg = HashMap::new();
        cfg.insert(
            "RS_CERT_PATH".to_string(),
            cert_path.to_string_lossy().to_string(),
        );
        cfg.insert(
            "RS_CERT_KEY_PATH".to_string(),
            cert_key_path.to_string_lossy().to_string(),
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
        env::set_var("RS_DISABLE_AUTH", "true");
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

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_replica_reload_task_runs_when_interval_non_zero() {
        let reloads = Arc::new(AtomicUsize::new(0));
        test_observer::set_replica_reload_observer(Some(reloads.clone()));

        let data_path = tempdir().unwrap().keep();
        let cfg = Cfg {
            data_path: data_path.clone(),
            role: InstanceRole::Primary,
            engine_config: StorageEngineConfig {
                replica_update_interval: Duration::from_millis(50),
                ..StorageEngineConfig::default()
            },
            ..Cfg::default()
        };
        let primary_storage = StorageEngine::builder()
            .with_cfg(cfg.clone())
            .with_data_path(cfg.data_path.clone())
            .build()
            .await;
        primary_storage
            .create_bucket("bucket-1", BucketSettings::default())
            .await
            .unwrap();

        let mut replica_cfg = cfg.clone();
        replica_cfg.role = InstanceRole::Replica;
        let replica_storage = Arc::new(
            StorageEngine::builder()
                .with_cfg(replica_cfg.clone())
                .with_data_path(replica_cfg.data_path.clone())
                .build()
                .await,
        );
        primary_storage
            .create_bucket("bucket-2", BucketSettings::default())
            .await
            .unwrap();

        let handler = tokio::spawn(periodical_replica_reload(
            replica_storage.clone(),
            Duration::from_millis(50),
        ));

        sleep(Duration::from_millis(120)).await;
        handler.abort();
        let _ = handler.await;

        test_observer::set_replica_reload_observer(None);

        assert!(
            reloads.load(Ordering::Relaxed) > 0,
            "periodical_replica_reload should run when interval is non-zero"
        );
        let bucket_names = replica_storage
            .bucket_list_snapshot()
            .await
            .unwrap()
            .into_iter()
            .map(|bucket| bucket.name().to_string())
            .collect::<Vec<_>>();
        assert!(bucket_names.contains(&"bucket-1".to_string()));
        assert!(bucket_names.contains(&"bucket-2".to_string()));
    }

    async fn set_env_and_run(cfg: HashMap<String, String>) -> JoinHandle<()> {
        let data_path = tempdir().unwrap().keep();

        env::set_var("RS_DATA_PATH", data_path.to_str().unwrap());
        env::set_var("RS_CERT_PATH", "");
        env::set_var("RS_CERT_KEY_PATH", "");
        env::set_var("RS_INSTANCE_ROLE", "STANDALONE");
        env::set_var("RS_ENGINE_REPLICA_UPDATE_INTERVAL", "60");
        env::set_var("RS_DISABLE_AUTH", "true");

        for (key, value) in cfg {
            env::set_var(key, value);
        }

        let task = spawn(|| {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                *STOP_SERVER.lock().await = false;
                prepare_server(CoreExtCfgParser)
                    .await
                    .expect("Failed to prepare server")
                    .launch()
                    .await;
            });
        });

        sleep(Duration::from_secs(1)).await;
        task
    }

    fn resolve_misc_file(file_name: &str) -> std::path::PathBuf {
        let candidates = [format!("misc/{file_name}"), format!("../misc/{file_name}")];

        for candidate in candidates {
            let path = std::path::PathBuf::from(candidate);
            if path.exists() {
                return std::fs::canonicalize(path)
                    .expect("Failed to resolve path in misc directory");
            }
        }

        panic!("Failed to find misc/{file_name}");
    }
}
