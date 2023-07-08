// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod create;
mod get;
mod list;
mod remove;

use axum::extract::{FromRequest, Path, State};
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{async_trait, headers};
use bytes::Bytes;

use crate::auth::policy::FullAccessPolicy;
use axum::headers::HeaderMapExt;
use hyper::HeaderMap;

use axum::routing::{get, post};
use std::sync::{Arc, RwLock};

use crate::auth::proto::token::Permissions;
use crate::auth::proto::{Token, TokenCreateResponse, TokenRepo};
use crate::core::status::HttpError;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::token_api::create::create_token;
use crate::http_frontend::token_api::get::get_token;
use crate::http_frontend::token_api::list::list;
use crate::http_frontend::token_api::remove::remove_token;
use crate::http_frontend::HttpServerState;

pub struct TokenApi {}

impl IntoResponse for TokenRepo {
    fn into_response(self) -> Response {
        let mut headers = HeaderMap::new();
        headers.typed_insert(headers::ContentType::json());

        (
            StatusCode::OK,
            headers,
            serde_json::to_string(&self).unwrap(),
        )
            .into_response()
    }
}

impl IntoResponse for TokenCreateResponse {
    fn into_response(self) -> Response {
        let mut headers = HeaderMap::new();
        headers.typed_insert(headers::ContentType::json());

        (
            StatusCode::OK,
            headers,
            serde_json::to_string(&self).unwrap(),
        )
            .into_response()
    }
}

impl IntoResponse for Token {
    fn into_response(self) -> Response {
        let mut headers = HeaderMap::new();
        headers.typed_insert(headers::ContentType::json());

        (
            StatusCode::OK,
            headers,
            serde_json::to_string(&self).unwrap(),
        )
            .into_response()
    }
}

#[async_trait]
impl<S, B> FromRequest<S, B> for Permissions
where
    Bytes: FromRequest<S, B>,
    B: Send + 'static,
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(req: Request<B>, state: &S) -> Result<Self, Self::Rejection> {
        let bytes = Bytes::from_request(req, state)
            .await
            .map_err(IntoResponse::into_response)?;
        serde_json::from_slice(&*bytes).map_err(|e| HttpError::from(e).into_response())
    }
}

pub fn create_token_api_routes() -> axum::Router<Arc<RwLock<HttpServerState>>> {
    axum::Router::new()
        .route("/", get(list))
        .route("/:token_name", post(create_token))
        .route("/:token_name", get(get_token))
        .route("/:token_name", post(remove_token))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::asset_manager::ZipAssetManager;
    use crate::auth::token_auth::TokenAuthorization;
    use crate::auth::token_repository::create_token_repository;
    use crate::storage::storage::Storage;

    use crate::http_frontend::bucket_api::BucketApi;
    use crate::http_frontend::token_api::get::get_token;
    use crate::storage::proto::BucketSettings;
    use axum::headers::Authorization;
    use rstest::fixture;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_remove_token() {
        let components = setup();
        let token = remove_token(State(components), Path("test".to_string()), auth_headers()).await;
        assert!(token.is_ok());
    }

    #[tokio::test]
    async fn test_remove_bucket_from_permission() {
        let components = setup();
        let token = get_token(
            State(Arc::clone(&components)),
            Path("test".to_string()),
            auth_headers(),
        )
        .await
        .unwrap();
        assert_eq!(
            token.permissions.unwrap().read,
            vec!["bucket-1".to_string(), "bucket-2".to_string()]
        );

        BucketApi::remove_bucket(
            State(Arc::clone(&components)),
            Path("bucket-1".to_string()),
            auth_headers(),
        )
        .await
        .unwrap();

        let token = get_token(State(components), Path("test".to_string()), auth_headers())
            .await
            .unwrap();
        assert_eq!(
            token.permissions.unwrap().read,
            vec!["bucket-2".to_string()]
        );
    }

    #[fixture]
    pub(crate) fn components() -> Arc<RwLock<HttpServerState>> {
        setup()
    }

    fn setup() -> Arc<RwLock<HttpServerState>> {
        let data_path = tempfile::tempdir().unwrap().into_path();

        let mut components = HttpServerState {
            storage: Storage::new(PathBuf::from(data_path.clone())),
            auth: TokenAuthorization::new("inti-token"),
            token_repo: create_token_repository(data_path.clone(), "init-token"),
            console: ZipAssetManager::new(&[]),
            base_path: "/".to_string(),
        };

        components
            .storage
            .create_bucket("bucket-1", BucketSettings::default())
            .unwrap();
        components
            .storage
            .create_bucket("bucket-2", BucketSettings::default())
            .unwrap();

        let permissions = Permissions {
            read: vec!["bucket-1".to_string(), "bucket-2".to_string()],
            ..Default::default()
        };
        components
            .token_repo
            .create_token("test", permissions)
            .unwrap();

        Arc::new(RwLock::new(components))
    }

    #[fixture]
    pub(crate) fn headers() -> HeaderMap {
        auth_headers()
    }

    fn auth_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.typed_insert(Authorization::bearer("init-token").unwrap());
        headers
    }
}
