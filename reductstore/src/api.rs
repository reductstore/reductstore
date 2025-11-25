// Copyright 2023-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1
//
mod bucket;
mod entry;
mod links;
mod middleware;
mod replication;
mod server;
mod token;
mod ui;
mod utils;

use crate::api::ui::{redirect_to_index, show_ui};
use crate::asset::asset_manager::ManageStaticAsset;
use crate::auth::policy::Policy;
use crate::auth::token_auth::TokenAuthorization;
use crate::auth::token_repository::ManageTokens;
use crate::cfg::Cfg;
use crate::core::cache::Cache;
use crate::ext::ext_repository::ManageExtensions;
use crate::lock_file::BoxedLockFile;
use crate::replication::ManageReplications;
use crate::storage::engine::StorageEngine;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{middleware::from_fn, Router};
use bucket::create_bucket_api_routes;
use entry::create_entry_api_routes;
use hyper::http::HeaderValue;
use log::{error, warn};
use middleware::{default_headers, print_statuses};
pub use reduct_base::error::ErrorCode;
use reduct_base::error::ReductError;
use reduct_base::error::ReductError as BaseHttpError;
use reduct_base::io::BoxedReadRecord;
use reduct_base::service_unavailable;
use reduct_macros::Twin;
use replication::create_replication_api_routes;
use serde::de::StdError;
use server::create_server_api_routes;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;
use token::create_token_api_routes;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{Mutex, RwLock};
use tower_http::cors::{Any, CorsLayer};

pub struct Components {
    pub storage: Arc<StorageEngine>,
    pub(crate) auth: TokenAuthorization,
    pub(crate) token_repo: RwLock<Box<dyn ManageTokens + Send + Sync>>,
    pub(crate) console: Box<dyn ManageStaticAsset + Send + Sync>,
    pub(crate) replication_repo: RwLock<Box<dyn ManageReplications + Send + Sync>>,
    pub(crate) ext_repo: Box<dyn ManageExtensions + Send + Sync>,
    pub(crate) query_link_cache: RwLock<Cache<String, Arc<Mutex<BoxedReadRecord>>>>,

    pub(crate) cfg: Cfg,
}

pub struct StateKeeper {
    rx: RwLock<Receiver<Components>>,
    components: RwLock<Option<Arc<Components>>>,
    lock_file: Arc<BoxedLockFile>,
}

impl StateKeeper {
    pub fn new(lock_file: Arc<BoxedLockFile>, rx: Receiver<Components>) -> Self {
        StateKeeper {
            rx: RwLock::new(rx),
            components: RwLock::new(None),
            lock_file,
        }
    }

    pub async fn get_with_permissions<P>(
        &self,
        headers: &HeaderMap,
        policy: P,
    ) -> Result<Arc<Components>, HttpError>
    where
        P: Policy,
    {
        let components = self.wait_components().await?;

        components.auth.check(
            headers
                .get("Authorization")
                .map(|header| header.to_str().unwrap_or("")),
            components.token_repo.read().await.as_ref(),
            policy,
        )?;

        Ok(components)
    }

    async fn wait_components(&self) -> Result<Arc<Components>, HttpError> {
        if !self.lock_file.is_locked().await {
            return Err(
                service_unavailable!("The server is starting up, please try again later").into(),
            );
        }

        {
            let mut lock = self.components.write().await;
            // it's important to check again after acquiring the lock and lock must be exclusive to avoid rice conditions
            if lock.is_none() {
                let components = self
                    .rx
                    .write()
                    .await
                    .recv()
                    .await
                    .expect("Failed to receive components from channel");
                lock.replace(Arc::new(components));
            }
        }
        Ok(self.components.read().await.as_ref().unwrap().clone())
    }

    pub async fn get_anonymous(&self) -> Result<Arc<Components>, HttpError> {
        self.wait_components().await
    }
}

#[derive(Twin, PartialEq)]
pub struct HttpError(BaseHttpError);

impl HttpError {
    pub fn new(status: ErrorCode, message: &str) -> Self {
        HttpError(BaseHttpError::new(status, message))
    }
}

impl Debug for HttpError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl Display for HttpError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl StdError for HttpError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        None
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let err: BaseHttpError = self.into();
        let converted_quotes = err.message.to_string().replace("\"", "'");
        let body = format!("{{\"detail\": \"{}\"}}", converted_quotes);

        // its often easiest to implement `IntoResponse` by calling other implementations
        let http_code = if (err.status as i16) < 0 {
            warn!("Invalid status code: {}", err.status);
            StatusCode::INTERNAL_SERVER_ERROR
        } else {
            StatusCode::from_u16(err.status as u16).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
        };

        let err_msg = if let Ok(header_value) = HeaderValue::from_str(&err.message) {
            header_value
        } else {
            error!("Invalid error message: {}", err.message);
            HeaderValue::from_str("Unparsable message").unwrap()
        };
        let mut resp = (http_code, body).into_response();
        resp.headers_mut()
            .insert("content-type", "application/json".parse().unwrap());
        resp.headers_mut().insert("x-reduct-error", err_msg);
        resp
    }
}

impl From<axum::Error> for HttpError {
    fn from(err: axum::Error) -> Self {
        HttpError::from(BaseHttpError::bad_request(&format!("{}", err)))
    }
}

impl From<serde_json::Error> for HttpError {
    fn from(err: serde_json::Error) -> Self {
        HttpError::new(
            ErrorCode::UnprocessableEntity,
            &format!("Invalid JSON: {}", err),
        )
    }
}

pub struct AxumAppBuilder {
    rx: Option<Receiver<Components>>,
    cfg: Option<Cfg>,
    lc: Option<Arc<BoxedLockFile>>,
}

impl AxumAppBuilder {
    pub fn new() -> Self {
        AxumAppBuilder {
            rx: None,
            cfg: None,
            lc: None,
        }
    }

    pub fn with_component_receiver(mut self, components: Receiver<Components>) -> Self {
        self.rx = Some(components);
        self
    }

    pub fn with_lock_file(mut self, lock_file: Arc<BoxedLockFile>) -> Self {
        self.lc = Some(lock_file);
        self
    }

    pub fn with_cfg(mut self, cfg: Cfg) -> Self {
        self.cfg = Some(cfg);
        self
    }

    pub fn build(self) -> Router {
        if self.rx.is_none() || self.cfg.is_none() {
            panic!("Components and Cfg must be set before building the app");
        }

        let cfg = self.cfg.unwrap();
        let b_route = create_bucket_api_routes().merge(create_entry_api_routes());
        let cors = Self::configure_cors(&cfg.cors_allow_origin);

        Router::new()
            // Server API
            .nest(
                &format!("{}api/v1", cfg.api_base_path),
                create_server_api_routes(),
            )
            // Token API
            .nest(
                &format!("{}api/v1/tokens", cfg.api_base_path),
                create_token_api_routes(),
            )
            // Bucket API + Entry API
            .nest(&format!("{}api/v1/b", cfg.api_base_path), b_route)
            // Replication API
            .nest(
                &format!("{}api/v1/replications", cfg.api_base_path),
                create_replication_api_routes(),
            )
            .nest(
                &format!("{}api/v1/links", cfg.api_base_path),
                links::create_query_link_api_routes(),
            )
            // UI
            .route(&format!("{}", cfg.api_base_path), get(redirect_to_index))
            .fallback(get(show_ui))
            .layer(from_fn(default_headers))
            .layer(from_fn(print_statuses))
            .layer(cors)
            .with_state(Arc::new(StateKeeper::new(
                self.lc.expect("Lock file must be set"),
                self.rx.expect("Components must be set"),
            )))
    }

    fn configure_cors(cors_allow_origin: &Vec<String>) -> CorsLayer {
        let cors_layer = CorsLayer::new()
            .allow_methods(Any)
            .allow_headers(Any)
            .expose_headers(Any);

        if cors_allow_origin.contains(&"*".to_string()) {
            cors_layer.allow_origin(Any)
        } else {
            let parsed_origins: Vec<HeaderValue> = cors_allow_origin
                .iter()
                .filter_map(|origin| origin.parse().ok())
                .collect();
            cors_layer.allow_origin(parsed_origins)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::asset_manager::create_asset_manager;
    use crate::auth::proto::TokenRepo;
    use crate::auth::token_repository::TokenRepositoryBuilder;
    use crate::backend::Backend;
    use crate::core::file_cache::FILE_CACHE;
    use crate::ext::ext_repository::create_ext_repository;
    use crate::lock_file::{LockFile, LockFileBuilder};
    use crate::replication::ReplicationRepoBuilder;
    use axum::body::Body;
    use axum::extract::Path;
    use axum_extra::headers::{Authorization, HeaderMap, HeaderMapExt};
    use bytes::Bytes;
    use reduct_base::ext::ExtSettings;
    use reduct_base::msg::bucket_api::BucketSettings;
    use reduct_base::msg::replication_api::ReplicationSettings;
    use reduct_base::msg::server_api::ServerInfo;
    use reduct_base::msg::token_api::Permissions;
    use rstest::fixture;
    use std::collections::HashMap;
    use std::time::Duration;

    mod http_error {
        use super::*;
        use axum::body::to_bytes;
        use rstest::rstest;
        use tokio;

        #[rstest]
        #[tokio::test]
        async fn test_http_error() {
            let error = HttpError::new(ErrorCode::BadRequest, "Test error");
            let resp = error.into_response();
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
            assert_eq!(
                resp.headers().get("content-type").unwrap(),
                HeaderValue::from_static("application/json")
            );
            assert_eq!(
                resp.headers().get("x-reduct-error").unwrap(),
                HeaderValue::from_static("Test error")
            );

            let body: Bytes = to_bytes(resp.into_body(), 1000).await.unwrap();
            assert_eq!(body, Bytes::from(r#"{"detail": "Test error"}"#))
        }

        #[rstest]
        #[tokio::test]
        async fn test_no_http_error() {
            let error = HttpError::new(ErrorCode::Interrupt, "Test error");
            let resp = error.into_response();
            assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
            assert_eq!(
                resp.headers().get("content-type").unwrap(),
                HeaderValue::from_static("application/json")
            );
            assert_eq!(
                resp.headers().get("x-reduct-error").unwrap(),
                HeaderValue::from_static("Test error")
            );

            let body: Bytes = to_bytes(resp.into_body(), 1000).await.unwrap();
            assert_eq!(body, Bytes::from(r#"{"detail": "Test error"}"#))
        }

        #[rstest]
        #[tokio::test]
        async fn test_http_json_format() {
            let error = HttpError::new(ErrorCode::BadRequest, "Test \"error\"");
            let resp = error.into_response();
            assert_eq!(
                resp.headers().get("x-reduct-error").unwrap(),
                HeaderValue::from_static("Test \"error\"")
            );

            let body: Bytes = to_bytes(resp.into_body(), 1000).await.unwrap();
            assert_eq!(body, Bytes::from(r#"{"detail": "Test 'error'"}"#))
        }

        #[rstest]
        #[tokio::test]
        async fn test_http_error_unparsable_message() {
            let error = HttpError::new(
                ErrorCode::BadRequest,
                &String::from_utf8_lossy(b"Test \x7f"),
            );
            let resp = error.into_response();
            assert_eq!(
                resp.headers().get("x-reduct-error").unwrap(),
                HeaderValue::from_static("Unparsable message")
            );
        }
    }

    mod state_keeper {
        use super::*;
        use crate::auth::policy::FullAccessPolicy;
        use rstest::rstest;
        use tokio;

        #[rstest]
        #[tokio::test]
        async fn test_get_anonymous(#[future] keeper: Arc<StateKeeper>) {
            let keeper = keeper.await;
            let components = keeper.get_anonymous().await.unwrap();
            assert!(components.storage.info().is_ok());
        }

        #[rstest]
        #[tokio::test]
        async fn test_get_with_permissions_ok(
            #[future] keeper: Arc<StateKeeper>,
            headers: HeaderMap,
        ) {
            let keeper = keeper.await;
            let components = keeper
                .get_with_permissions(&headers, FullAccessPolicy {})
                .await
                .unwrap();
            assert!(components.storage.info().is_ok());
        }

        #[rstest]
        #[tokio::test]
        async fn test_get_with_permissions_error(#[future] keeper: Arc<StateKeeper>) {
            let mut headers = HeaderMap::new();
            headers.typed_insert(Authorization::bearer("bad-token").unwrap());

            let keeper = keeper.await;
            let err = keeper
                .get_with_permissions(&headers, FullAccessPolicy {})
                .await
                .err()
                .unwrap();
            assert_eq!(
                err,
                HttpError::new(ErrorCode::Unauthorized, "Invalid token")
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_wait_components_locked(#[future] waiting_keeper: Arc<StateKeeper>) {
            let keeper = waiting_keeper.await;
            let err = keeper.get_anonymous().await.err().unwrap();
            assert_eq!(
                err,
                HttpError::new(
                    ErrorCode::ServiceUnavailable,
                    "The server is starting up, please try again later"
                )
            );
        }
    }

    #[fixture]
    pub(crate) async fn keeper() -> Arc<StateKeeper> {
        let cfg = Cfg {
            data_path: tempfile::tempdir().unwrap().keep(),
            api_token: "inti-token".to_string(),
            ..Cfg::default()
        };

        FILE_CACHE.set_storage_backend(
            Backend::builder()
                .local_data_path(cfg.data_path.clone())
                .try_build()
                .unwrap(),
        );

        let storage = StorageEngine::builder()
            .with_data_path(cfg.data_path.clone())
            .with_cfg(cfg.clone())
            .build();
        let mut token_repo = TokenRepositoryBuilder::new(cfg.clone()).build(cfg.data_path.clone());

        storage
            .create_bucket("bucket-1", BucketSettings::default())
            .unwrap();
        storage
            .create_bucket("bucket-2", BucketSettings::default())
            .unwrap();

        let labels = HashMap::from_iter(vec![
            ("x".to_string(), "y".to_string()),
            ("b".to_string(), "[a,b]".to_string()),
        ]);

        let mut sender = storage
            .get_bucket("bucket-1")
            .unwrap()
            .upgrade_and_unwrap()
            .begin_write("entry-1", 0, 6, "text/plain".to_string(), labels)
            .await
            .unwrap();
        sender.send(Ok(Some(Bytes::from("Hey!!!")))).await.unwrap();
        sender.send(Ok(None)).await.unwrap();

        let permissions = Permissions {
            read: vec!["bucket-1".to_string(), "bucket-2".to_string()],
            ..Default::default()
        };

        token_repo.generate_token("test", permissions).unwrap();

        let storage = Arc::new(storage);
        let mut replication_repo =
            ReplicationRepoBuilder::new(cfg.clone()).build(Arc::clone(&storage));
        replication_repo
            .create_replication(
                "api-test",
                ReplicationSettings {
                    src_bucket: "bucket-1".to_string(),
                    dst_bucket: "bucket-2".to_string(),
                    dst_host: "http://localhost:8080".to_string(),
                    dst_token: None,
                    entries: vec![],
                    include: Default::default(),
                    exclude: Default::default(),
                    each_n: None,
                    each_s: None,
                    when: None,
                },
            )
            .unwrap();

        let components = Components {
            storage: Arc::clone(&storage),
            auth: TokenAuthorization::new("inti-token"),
            token_repo: RwLock::new(token_repo),
            console: create_asset_manager(include_bytes!(concat!(env!("OUT_DIR"), "/console.zip"))),
            replication_repo: RwLock::new(replication_repo),
            ext_repo: create_ext_repository(
                None,
                vec![],
                ExtSettings::builder()
                    .server_info(ServerInfo::default())
                    .build(),
                cfg.io_conf.clone(),
            )
            .expect("Failed to create extension repo"),
            cfg: Cfg::default(),
            query_link_cache: RwLock::new(Cache::new(8, Duration::from_secs(60))),
        };

        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tx.send(components).await.unwrap();

        Arc::new(StateKeeper::new(Arc::new(LockFileBuilder::noop()), rx))
    }

    #[fixture]
    pub(crate) async fn waiting_keeper(#[future] keeper: Arc<StateKeeper>) -> Arc<StateKeeper> {
        let mut keeper = Arc::try_unwrap(keeper.await).ok().unwrap();
        keeper.lock_file = Arc::new(Box::new(WaitingLockFile {}));
        Arc::new(keeper)
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

    #[fixture]
    pub async fn empty_body() -> Body {
        Body::empty()
    }

    struct WaitingLockFile {}

    #[async_trait::async_trait]
    impl LockFile for WaitingLockFile {
        async fn is_locked(&self) -> bool {
            false
        }

        async fn is_failed(&self) -> bool {
            false
        }

        async fn is_waiting(&self) -> bool {
            true
        }

        fn release(&self) {}
    }
}
