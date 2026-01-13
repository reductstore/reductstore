// Copyright 2023-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1
//
mod bucket;
mod entry;
mod io;
mod links;
mod middleware;
mod replication;
mod server;
mod token;
mod ui;
mod utils;

use crate::api::io::create_io_api_routes;
use crate::api::ui::{redirect_to_index, show_ui};
use crate::asset::asset_manager::ManageStaticAsset;
use crate::auth::policy::Policy;
use crate::auth::token_auth::TokenAuthorization;
use crate::auth::token_repository::ManageTokens;
use crate::cfg::Cfg;
use crate::core::cache::Cache;
use crate::core::sync::AsyncRwLock;
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
use reduct_base::io::BoxedReadRecord;
use reduct_base::service_unavailable;
use replication::create_replication_api_routes;
use serde::de::StdError;
use server::create_server_api_routes;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;
use token::create_token_api_routes;
use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex;
use tower_http::cors::{Any, CorsLayer};

pub struct Components {
    pub storage: Arc<StorageEngine>,
    pub(crate) auth: TokenAuthorization,
    pub(crate) token_repo: AsyncRwLock<Box<dyn ManageTokens + Send + Sync>>,
    pub(crate) console: Box<dyn ManageStaticAsset + Send + Sync>,
    pub(crate) replication_repo: AsyncRwLock<Box<dyn ManageReplications + Send + Sync>>,
    pub(crate) ext_repo: Box<dyn ManageExtensions + Send + Sync>,
    pub(crate) query_link_cache: AsyncRwLock<Cache<String, Arc<Mutex<BoxedReadRecord>>>>,

    pub(crate) cfg: Cfg,
}

pub struct StateKeeper {
    rx: AsyncRwLock<Receiver<Components>>,
    components: AsyncRwLock<Option<Arc<Components>>>,
    lock_file: Arc<BoxedLockFile>,
}

impl StateKeeper {
    pub fn new(lock_file: Arc<BoxedLockFile>, rx: Receiver<Components>) -> Self {
        StateKeeper {
            rx: AsyncRwLock::new(rx),
            components: AsyncRwLock::new(None),
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

        components
            .auth
            .check(
                headers
                    .get("Authorization")
                    .map(|header| header.to_str().unwrap_or("")),
                components.token_repo.write().await?.as_mut(),
                policy,
            )
            .await?;

        Ok(components)
    }

    async fn wait_components(&self) -> Result<Arc<Components>, HttpError> {
        let locked = self
            .lock_file
            .is_locked()
            .await
            .map_err(|err| HttpError::new(ErrorCode::InternalServerError, &err.to_string()))?;

        if !locked {
            return Err(HttpError::from(service_unavailable!(
                "The server is starting up, please try again later"
            ))
            .with_log_hint(LogHint::SkipErrorLogging));
        }

        {
            let mut lock = self.components.write().await?;
            // it's important to check again after acquiring the lock and lock must be exclusive to avoid race conditions
            if lock.is_none() {
                // check if there are components in the channel
                if self.rx.read().await?.capacity() != 0 {
                    return Err(service_unavailable!(
                        "The server is starting up, please try again later"
                    )
                    .into());
                }

                let components = match self.rx.write().await?.recv().await {
                    Some(cmp) => cmp,
                    None => {
                        return Err(service_unavailable!(
                            "The server is starting up, please try again later"
                        )
                        .into())
                    }
                };
                // ensure background services (like replication) start after HTTP is ready to accept connections
                // however, in tests we want to control when these services start
                #[cfg(not(test))]
                components.replication_repo.write().await?.start();
                lock.replace(Arc::new(components));
            }
        }
        let components = self.components.read().await?;
        let components = components
            .as_ref()
            .cloned()
            .expect("Components must be initialized before use");
        Ok(components)
    }

    pub async fn get_anonymous(&self) -> Result<Arc<Components>, HttpError> {
        self.wait_components().await
    }
}

#[derive(PartialEq, Clone, Copy, Debug, Eq)]
pub enum LogHint {
    Default,
    SkipErrorLogging,
}

#[derive(PartialEq, Clone)]
pub struct HttpError {
    inner: ReductError,
    log_hint: LogHint,
}

impl HttpError {
    pub fn new(status: ErrorCode, message: &str) -> Self {
        HttpError {
            inner: ReductError::new(status, message),
            log_hint: LogHint::Default,
        }
    }

    pub fn with_log_hint(mut self, log_hint: LogHint) -> Self {
        self.log_hint = log_hint;
        self
    }

    pub fn status(&self) -> ErrorCode {
        self.inner.status
    }

    pub fn message(&self) -> &str {
        &self.inner.message
    }

    pub fn log_hint(&self) -> LogHint {
        self.log_hint
    }

    pub fn into_inner(self) -> ReductError {
        self.inner
    }

    pub fn inner(&self) -> &ReductError {
        &self.inner
    }
}

impl Debug for HttpError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl Display for HttpError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl StdError for HttpError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        None
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let log_hint = self.log_hint;
        let err: ReductError = self.into();
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
        if log_hint == LogHint::SkipErrorLogging {
            resp.headers_mut().insert(
                "x-reduct-log-hint",
                HeaderValue::from_static("skip-error-log"),
            );
        }
        resp
    }
}

impl From<ReductError> for HttpError {
    fn from(st: ReductError) -> Self {
        Self {
            inner: st,
            log_hint: LogHint::Default,
        }
    }
}

impl From<HttpError> for ReductError {
    fn from(err: HttpError) -> ReductError {
        err.inner
    }
}

impl From<axum::Error> for HttpError {
    fn from(err: axum::Error) -> Self {
        HttpError::from(ReductError::bad_request(&format!("{}", err)))
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
                &format!("{}api/v1/io", cfg.api_base_path),
                create_io_api_routes(),
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
    use reduct_base::error::ReductError as BaseHttpError;
    use reduct_base::ext::ExtSettings;
    use reduct_base::msg::bucket_api::BucketSettings;
    use reduct_base::msg::replication_api::{ReplicationMode, ReplicationSettings};
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

        #[rstest]
        #[tokio::test]
        async fn test_http_error_negative_status() {
            let error = HttpError::new(ErrorCode::Unknown, "neg");
            let resp = error.into_response();
            assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        }

        #[rstest]
        #[tokio::test]
        async fn test_http_error_from_axum_error() {
            let axum_err = axum::Error::new(std::io::Error::new(std::io::ErrorKind::Other, "boom"));
            let http_err: HttpError = axum_err.into();
            let resp = http_err.into_response();
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        }

        #[rstest]
        #[tokio::test]
        async fn test_http_error_from_serde_json() {
            let err = serde_json::from_str::<serde_json::Value>("not json")
                .err()
                .unwrap();
            let http_err: HttpError = err.into();
            assert_eq!(http_err.status(), ErrorCode::UnprocessableEntity);
        }

        #[rstest]
        #[tokio::test]
        async fn test_http_error_display_debug_and_source() {
            let err = HttpError::new(ErrorCode::BadRequest, "boom");
            let debug = format!("{err:?}");
            assert!(debug.contains("BadRequest"));
            assert_eq!(
                format!("{err}"),
                "ReductError { status: BadRequest, message: \"boom\" }"
            );
            assert!(StdError::source(&err).is_none());
        }

        #[rstest]
        #[tokio::test]
        async fn test_http_error_skip_log_header() {
            let err = HttpError::new(ErrorCode::ServiceUnavailable, "starting up")
                .with_log_hint(LogHint::SkipErrorLogging);

            assert_eq!(err.inner().message, "starting up");

            let resp = err.into_response();
            assert_eq!(
                resp.headers()
                    .get("x-reduct-log-hint")
                    .map(HeaderValue::as_bytes),
                Some("skip-error-log".as_bytes())
            );
        }
    }

    mod axum_builder {
        use super::*;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        #[test]
        #[should_panic(expected = "Components and Cfg must be set before building the app")]
        fn test_builder_panics_without_state() {
            let _ = AxumAppBuilder::new().build();
        }

        #[test]
        fn test_configure_cors_any() {
            let _ = AxumAppBuilder::configure_cors(&vec!["*".into()]);
        }

        #[test]
        fn test_configure_cors_specific() {
            let _ = AxumAppBuilder::configure_cors(&vec!["http://example.com".into()]);
        }

        #[test]
        fn test_configure_cors_ignores_invalid_origins() {
            let _ = AxumAppBuilder::configure_cors(&vec![
                "not-a-uri".into(),
                "http://example.com".into(),
            ]);
        }

        #[tokio::test]
        async fn test_builder_builds_and_redirects_to_ui() {
            let cfg = Cfg {
                data_path: tempfile::tempdir().unwrap().keep(),
                api_token: "init-token".to_string(),
                api_base_path: "/".to_string(),
                ..Cfg::default()
            };

            let (tx, rx) = tokio::sync::mpsc::channel(1);
            tx.send(test_components(cfg.clone()).await).await.unwrap();

            let app = AxumAppBuilder::new()
                .with_cfg(cfg)
                .with_lock_file(Arc::new(LockFileBuilder::noop()))
                .with_component_receiver(rx)
                .build();

            let response = app
                .oneshot(Request::get("/").body(Body::empty()).unwrap())
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::FOUND);
            assert_eq!(response.headers().get("location").unwrap(), "/ui/");
        }
    }

    mod state_keeper {
        use super::*;
        use crate::auth::policy::{
            AuthenticatedPolicy, FullAccessPolicy, ReadAccessPolicy, WriteAccessPolicy,
        };
        use rstest::rstest;
        use tokio;

        #[rstest]
        #[tokio::test]
        async fn test_get_anonymous(#[future] keeper: Arc<StateKeeper>) {
            let keeper = keeper.await;
            let components = keeper.get_anonymous().await.unwrap();
            assert!(components.storage.info().await.is_ok());
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
            assert!(components.storage.info().await.is_ok());
        }

        #[rstest]
        #[tokio::test]
        async fn test_get_with_permissions_authenticated_policy(
            #[future] keeper: Arc<StateKeeper>,
            headers: HeaderMap,
        ) {
            let keeper = keeper.await;
            let components = keeper
                .get_with_permissions(&headers, AuthenticatedPolicy {})
                .await
                .unwrap();
            assert!(components.storage.info().await.is_ok());
        }

        #[rstest]
        #[tokio::test]
        async fn test_get_with_permissions_read_policy(
            #[future] keeper: Arc<StateKeeper>,
            headers: HeaderMap,
        ) {
            let keeper = keeper.await;
            let components = keeper
                .get_with_permissions(&headers, ReadAccessPolicy { bucket: "bucket-1" })
                .await
                .unwrap();
            assert!(components.storage.info().await.is_ok());
        }

        #[rstest]
        #[tokio::test]
        async fn test_get_with_permissions_write_policy(
            #[future] keeper: Arc<StateKeeper>,
            headers: HeaderMap,
        ) {
            let keeper = keeper.await;
            let components = keeper
                .get_with_permissions(&headers, WriteAccessPolicy { bucket: "bucket-1" })
                .await
                .unwrap();
            assert!(components.storage.info().await.is_ok());
        }

        #[rstest]
        #[tokio::test]
        async fn test_get_with_permissions_missing_header(#[future] keeper: Arc<StateKeeper>) {
            let keeper = keeper.await;
            let headers = HeaderMap::new();

            let err = keeper
                .get_with_permissions(&headers, AuthenticatedPolicy {})
                .await
                .err()
                .unwrap();

            assert_eq!(
                err,
                HttpError::new(ErrorCode::Unauthorized, "No bearer token in request header")
            );
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
            assert_eq!(err.status(), ErrorCode::ServiceUnavailable);
            assert_eq!(err.log_hint(), LogHint::SkipErrorLogging);
        }

        #[rstest]
        #[tokio::test]
        async fn test_wait_components_returns_503_if_no_components(
            #[future] not_ready_keeper: Arc<StateKeeper>,
        ) {
            // Channel has capacity but no components yet; should return 503 quickly.
            let err = not_ready_keeper.await.get_anonymous().await.err().unwrap();
            let err: BaseHttpError = err.into();
            assert_eq!(err.status(), ErrorCode::ServiceUnavailable);
        }

        #[rstest]
        #[tokio::test]
        async fn test_wait_components_lockfile_err() {
            struct ErrLockFile;
            #[async_trait::async_trait]
            impl LockFile for ErrLockFile {
                async fn is_locked(&self) -> Result<bool, ReductError> {
                    Err(ReductError::internal_server_error("boom"))
                }
                async fn is_failed(&self) -> Result<bool, ReductError> {
                    Err(ReductError::internal_server_error("boom"))
                }
                async fn is_waiting(&self) -> Result<bool, ReductError> {
                    Err(ReductError::internal_server_error("boom"))
                }
                async fn release(&self) {}
            }

            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            let err_lock: Arc<BoxedLockFile> = Arc::new(Box::new(ErrLockFile));
            // Cover all lock methods for coverage completeness.
            assert!(err_lock.is_failed().await.is_err());
            assert!(err_lock.is_waiting().await.is_err());
            let keeper = Arc::new(StateKeeper::new(err_lock, rx));
            let err = keeper.get_anonymous().await.err().unwrap();
            let err: BaseHttpError = err.into();
            assert_eq!(err.status(), ErrorCode::InternalServerError);
        }

        #[rstest]
        #[tokio::test]
        async fn test_wait_components_channel_closed() {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            drop(_tx);

            let keeper = Arc::new(StateKeeper::new(
                Arc::new(Box::new(NotReadyLockFile {})),
                rx,
            ));
            let err = keeper.get_anonymous().await.err().unwrap();
            let err: BaseHttpError = err.into();
            assert_eq!(err.status(), ErrorCode::ServiceUnavailable);
        }

        #[rstest]
        #[tokio::test]
        async fn test_wait_components_recv_none_when_channel_closed_and_capacity_zero() {
            let (_tx, mut rx) = tokio::sync::mpsc::channel(1);
            drop(_tx);
            rx.close();

            let keeper = Arc::new(StateKeeper::new(
                Arc::new(Box::new(NotReadyLockFile {})),
                rx,
            ));

            let err = keeper.get_anonymous().await.err().unwrap();
            let err: BaseHttpError = err.into();
            assert_eq!(err.status(), ErrorCode::ServiceUnavailable);
        }

        #[rstest]
        #[tokio::test]
        async fn test_wait_components_unlocked() {
            struct UnlockedLockFile;
            #[async_trait::async_trait]
            impl LockFile for UnlockedLockFile {
                async fn is_locked(&self) -> Result<bool, ReductError> {
                    Ok(false)
                }
                async fn is_failed(&self) -> Result<bool, ReductError> {
                    Ok(false)
                }
                async fn is_waiting(&self) -> Result<bool, ReductError> {
                    Ok(true)
                }
                async fn release(&self) {}
            }

            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            let unlocked: Arc<BoxedLockFile> = Arc::new(Box::new(UnlockedLockFile));
            assert!(!unlocked.is_failed().await.unwrap());
            assert!(unlocked.is_waiting().await.unwrap());
            let keeper = Arc::new(StateKeeper::new(unlocked, rx));
            let err = keeper.get_anonymous().await.err().unwrap();
            let err: BaseHttpError = err.into();
            assert_eq!(err.status(), ErrorCode::ServiceUnavailable);
        }
    }

    async fn test_components(cfg: Cfg) -> Components {
        let cfg_for_storage = cfg.clone();
        FILE_CACHE
            .set_storage_backend(
                Backend::builder()
                    .local_data_path(cfg.data_path.clone())
                    .try_build()
                    .await
                    .unwrap(),
            )
            .await;

        let storage = Arc::new(
            StorageEngine::builder()
                .with_data_path(cfg_for_storage.data_path.clone())
                .with_cfg(cfg_for_storage.clone())
                .build()
                .await,
        );

        let token_repo = TokenRepositoryBuilder::new(cfg.clone())
            .build(cfg.data_path.clone())
            .await;
        let replication_repo = ReplicationRepoBuilder::new(cfg.clone())
            .build(Arc::clone(&storage))
            .await;

        #[cfg(feature = "web-console")]
        let console_bytes: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/console.zip"));
        #[cfg(not(feature = "web-console"))]
        let console_bytes: &[u8] = &[];

        Components {
            storage,
            auth: TokenAuthorization::new("init-token"),
            token_repo: AsyncRwLock::new(token_repo),
            console: create_asset_manager(console_bytes),
            replication_repo: AsyncRwLock::new(replication_repo),
            ext_repo: create_ext_repository(
                None,
                vec![],
                ExtSettings::builder()
                    .server_info(ServerInfo::default())
                    .build(),
                cfg.io_conf.clone(),
            )
            .expect("Failed to create extension repo"),
            cfg,
            query_link_cache: AsyncRwLock::new(Cache::new(8, Duration::from_secs(60))),
        }
    }

    #[fixture]
    pub(crate) async fn keeper() -> Arc<StateKeeper> {
        let cfg = Cfg {
            data_path: tempfile::tempdir().unwrap().keep(),
            api_token: "init-token".to_string(),
            ..Cfg::default()
        };

        FILE_CACHE
            .set_storage_backend(
                Backend::builder()
                    .local_data_path(cfg.data_path.clone())
                    .try_build()
                    .await
                    .unwrap(),
            )
            .await;

        let storage = StorageEngine::builder()
            .with_data_path(cfg.data_path.clone())
            .with_cfg(cfg.clone())
            .build()
            .await;
        let mut token_repo = TokenRepositoryBuilder::new(cfg.clone())
            .build(cfg.data_path.clone())
            .await;

        storage
            .create_bucket("bucket-1", BucketSettings::default())
            .await
            .unwrap();
        storage
            .create_bucket("bucket-2", BucketSettings::default())
            .await
            .unwrap();

        let labels = HashMap::from_iter(vec![
            ("x".to_string(), "y".to_string()),
            ("b".to_string(), "[a,b]".to_string()),
        ]);

        let mut sender = storage
            .get_bucket("bucket-1")
            .await
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

        token_repo
            .generate_token("test", permissions)
            .await
            .unwrap();

        let storage = Arc::new(storage);
        let mut replication_repo = ReplicationRepoBuilder::new(cfg.clone())
            .build(Arc::clone(&storage))
            .await;
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
                    mode: ReplicationMode::Enabled,
                },
            )
            .await
            .unwrap();

        #[cfg(feature = "web-console")]
        let console_bytes: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/console.zip"));
        #[cfg(not(feature = "web-console"))]
        let console_bytes: &[u8] = &[];

        let components = Components {
            storage: Arc::clone(&storage),
            auth: TokenAuthorization::new("inti-token"),
            token_repo: AsyncRwLock::new(token_repo),
            console: create_asset_manager(console_bytes),
            replication_repo: AsyncRwLock::new(replication_repo),
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
            query_link_cache: AsyncRwLock::new(Cache::new(8, Duration::from_secs(60))),
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
    pub(crate) async fn not_ready_keeper() -> Arc<StateKeeper> {
        // Channel without components; lock file is acquired but indicates startup is still in progress.
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        Arc::new(StateKeeper::new(
            Arc::new(Box::new(NotReadyLockFile {})),
            rx,
        ))
    }

    #[fixture]
    pub(crate) fn headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.typed_insert(Authorization::bearer("init-token").unwrap());
        headers
    }

    #[fixture]
    pub fn path_to_bucket_1() -> Path<HashMap<String, String>> {
        Path(HashMap::from_iter(vec![(
            "bucket_name".to_string(),
            "bucket-1".to_string(),
        )]))
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
        async fn is_locked(&self) -> Result<bool, ReductError> {
            Ok(false)
        }

        async fn is_failed(&self) -> Result<bool, ReductError> {
            Ok(false)
        }

        async fn is_waiting(&self) -> Result<bool, ReductError> {
            Ok(true)
        }

        async fn release(&self) {}
    }

    struct NotReadyLockFile {}

    #[async_trait::async_trait]
    impl LockFile for NotReadyLockFile {
        async fn is_locked(&self) -> Result<bool, ReductError> {
            Ok(true)
        }

        async fn is_failed(&self) -> Result<bool, ReductError> {
            Ok(false)
        }

        async fn is_waiting(&self) -> Result<bool, ReductError> {
            Ok(true)
        }

        async fn release(&self) {}
    }
}
