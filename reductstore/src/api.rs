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
use crate::replication::ManageReplications;
use crate::storage::storage::Storage;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{middleware::from_fn, Router};
use bucket::create_bucket_api_routes;
use entry::create_entry_api_routes;
use hyper::http::HeaderValue;
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
    pub auth: TokenAuthorization,
    pub token_repo: RwLock<Box<dyn ManageTokens + Send + Sync>>,
    pub console: Box<dyn ManageStaticAsset + Send + Sync>,
    pub replication_repo: RwLock<Box<dyn ManageReplications + Send + Sync>>,
    pub base_path: String,
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
        let body = format!("{{\"detail\": \"{}\"}}", err.message.to_string());

        // its often easiest to implement `IntoResponse` by calling other implementations
        let mut resp = (StatusCode::from_u16(err.status as u16).unwrap(), body).into_response();
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

pub fn create_axum_app(
    api_base_path: &String,
    cors_allow_origin: &Vec<String>,
    components: Arc<Components>,
) -> Router {
    let b_route = create_bucket_api_routes().merge(create_entry_api_routes());
    let cors = configure_cors(cors_allow_origin);

    let app = Router::new()
        // Server API
        .nest(
            &format!("{}api/v1", api_base_path),
            create_server_api_routes(),
        )
        // Token API
        .nest(
            &format!("{}api/v1/tokens", api_base_path),
            create_token_api_routes(),
        )
        // Bucket API + Entry API
        .nest(&format!("{}api/v1/b", api_base_path), b_route)
        // Replication API
        .nest(
            &format!("{}api/v1/replications", api_base_path),
            create_replication_api_routes(),
        )
        // UI
        .route(&format!("{}", api_base_path), get(redirect_to_index))
        .fallback(get(show_ui))
        .layer(from_fn(default_headers))
        .layer(from_fn(print_statuses))
        .layer(cors)
        .with_state(components);
    app
}

fn configure_cors(cors_allow_origin: &[String]) -> CorsLayer {
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
    use std::collections::HashMap;

    use axum::body::Body;
    use axum::extract::Path;
    use axum_extra::headers::{Authorization, HeaderMap, HeaderMapExt};
    use bytes::Bytes;
    use rstest::fixture;

    use reduct_base::msg::bucket_api::BucketSettings;
    use reduct_base::msg::replication_api::ReplicationSettings;
    use reduct_base::msg::token_api::Permissions;

    use crate::asset::asset_manager::create_asset_manager;
    use crate::auth::token_repository::create_token_repository;
    use crate::cfg::DEFAULT_PORT;
    use crate::replication::create_replication_repo;

    use super::*;

    #[fixture]
    pub(crate) async fn components() -> Arc<Components> {
        let data_path = tempfile::tempdir().unwrap().into_path();

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
        let mut replication_repo = create_replication_repo(Arc::clone(&storage), DEFAULT_PORT);
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
