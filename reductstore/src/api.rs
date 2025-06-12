// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1
//
mod bucket;
mod entry;
mod middleware;
mod replication;
mod server;
mod token;
mod ui;

use crate::api::ui::{redirect_to_index, show_ui};
use crate::asset::asset_manager::ManageStaticAsset;
use crate::auth::token_auth::TokenAuthorization;
use crate::auth::token_repository::ManageTokens;
use crate::cfg::io::IoConfig;
use crate::cfg::Cfg;
use crate::core::env::StdEnvGetter;
use crate::ext::ext_repository::ManageExtensions;
use crate::replication::ManageReplications;
use crate::storage::storage::Storage;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{middleware::from_fn, Router};
use bucket::create_bucket_api_routes;
use entry::create_entry_api_routes;
use hyper::http::HeaderValue;
use log::warn;
use middleware::{default_headers, print_statuses};
pub use reduct_base::error::ErrorCode;
use reduct_base::error::ReductError as BaseHttpError;
use reduct_macros::Twin;
use replication::create_replication_api_routes;
use serde::de::StdError;
use server::create_server_api_routes;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;
use token::create_token_api_routes;
use tokio::sync::RwLock;
use tower_http::cors::{Any, CorsLayer};

pub struct Components {
    pub storage: Arc<Storage>,
    pub(crate) auth: TokenAuthorization,
    pub(crate) token_repo: RwLock<Box<dyn ManageTokens + Send + Sync>>,
    pub(crate) console: Box<dyn ManageStaticAsset + Send + Sync>,
    pub(crate) replication_repo: RwLock<Box<dyn ManageReplications + Send + Sync>>,
    pub(crate) ext_repo: Box<dyn ManageExtensions + Send + Sync>,
    pub(crate) base_path: String,
    pub(crate) io_settings: IoConfig,
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

        let mut resp = (http_code, body).into_response();
        resp.headers_mut()
            .insert("content-type", "application/json".parse().unwrap());
        resp.headers_mut()
            .insert("x-reduct-error", err.message.parse().unwrap());
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

pub fn create_axum_app(cfg: &Cfg<StdEnvGetter>, components: Arc<Components>) -> Router {
    let b_route = create_bucket_api_routes().merge(create_entry_api_routes());
    let cors = configure_cors(&cfg.cors_allow_origin);

    let app = Router::new()
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
        // UI
        .route(&format!("{}", cfg.api_base_path), get(redirect_to_index))
        .fallback(get(show_ui))
        .layer(from_fn(default_headers))
        .layer(from_fn(print_statuses))
        .layer(cors)
        .with_state(components);
    app
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

#[cfg(test)]
mod tests {
    use crate::asset::asset_manager::create_asset_manager;
    use crate::auth::token_repository::create_token_repository;
    use crate::cfg::replication::ReplicationConfig;
    use crate::ext::ext_repository::create_ext_repository;
    use crate::replication::create_replication_repo;
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

    use super::*;

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
    }

    #[fixture]
    pub(crate) async fn components() -> Arc<Components> {
        let data_path = tempfile::tempdir().unwrap().keep();

        let storage = Storage::load(data_path.clone(), None);
        let mut token_repo = create_token_repository(data_path.clone(), "init-token");

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
            create_replication_repo(Arc::clone(&storage), ReplicationConfig::default());
        replication_repo
            .create_replication(
                "api-test",
                ReplicationSettings {
                    src_bucket: "bucket-1".to_string(),
                    dst_bucket: "bucket-2".to_string(),
                    dst_host: "http://localhost:8080".to_string(),
                    dst_token: "".to_string(),
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
            base_path: "/".to_string(),
            replication_repo: RwLock::new(replication_repo),
            io_settings: IoConfig::default(),
            ext_repo: create_ext_repository(
                None,
                vec![],
                ExtSettings::builder()
                    .server_info(ServerInfo::default())
                    .build(),
            )
            .expect("Failed to create extension repo"),
        };

        Arc::new(components)
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
}
