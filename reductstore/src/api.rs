// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1
//

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{middleware::from_fn, Router};

use serde::de::StdError;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::api::bucket::create_bucket_api_routes;
use crate::api::entry::create_entry_api_routes;
use crate::api::middleware::{default_headers, print_statuses};
use crate::api::server::create_server_api_routes;
use crate::api::token::create_token_api_routes;
use crate::api::ui::{redirect_to_index, show_ui};
use crate::asset::asset_manager::ManageStaticAsset;
use crate::auth::token_auth::TokenAuthorization;
use crate::auth::token_repository::ManageTokens;
use crate::storage::storage::Storage;
use reduct_base::error::ReductError as BaseHttpError;
use reduct_macros::Twin;

use crate::api::replication::create_replication_api_routes;
use crate::replication::ManageReplications;
pub use reduct_base::error::ErrorCode;

mod bucket;
mod entry;
mod middleware;
mod replication;
mod server;
mod token;
mod ui;

pub struct Components {
    pub storage: Arc<RwLock<Storage>>,
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

pub fn create_axum_app(api_base_path: &String, components: Arc<Components>) -> Router {
    let b_route = create_bucket_api_routes().merge(create_entry_api_routes());

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
        .with_state(components);
    app
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Empty;
    use std::collections::HashMap;

    use crate::asset::asset_manager::create_asset_manager;
    use crate::auth::token_repository::create_token_repository;
    use crate::replication::create_replication_engine;
    use axum::extract::{BodyStream, FromRequest, Path};
    use axum::headers::{Authorization, HeaderMap, HeaderMapExt};
    use axum::http::Request;
    use bytes::Bytes;
    use reduct_base::msg::bucket_api::BucketSettings;
    use reduct_base::msg::token_api::Permissions;
    use rstest::fixture;

    #[fixture]
    pub(crate) async fn components() -> Arc<Components> {
        let data_path = tempfile::tempdir().unwrap().into_path();

        let mut storage = Storage::new(data_path.clone(), None);
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

        let sender = storage
            .get_mut_bucket("bucket-1")
            .unwrap()
            .write_record("entry-1", 0, 6, "text/plain".to_string(), labels)
            .await
            .unwrap();
        sender.send(Ok(Some(Bytes::from("Hey!!!")))).await.unwrap();
        sender.closed().await;

        let permissions = Permissions {
            read: vec!["bucket-1".to_string(), "bucket-2".to_string()],
            ..Default::default()
        };

        token_repo.generate_token("test", permissions).unwrap();

        let storage = Arc::new(RwLock::new(storage));
        let components = Components {
            storage: Arc::clone(&storage),
            auth: TokenAuthorization::new("inti-token"),
            token_repo: RwLock::new(token_repo),
            console: create_asset_manager(include_bytes!(concat!(env!("OUT_DIR"), "/console.zip"))),
            base_path: "/".to_string(),
            replication_repo: RwLock::new(create_replication_engine(storage).await),
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
    pub async fn empty_body() -> BodyStream {
        let emtpy_stream: Empty<Bytes> = Empty::new();
        let request = Request::builder().body(emtpy_stream).unwrap();
        BodyStream::from_request(request, &()).await.unwrap()
    }
}
