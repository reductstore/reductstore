// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use axum::extract::{FromRequest, Path, State};
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{async_trait, headers};
use bytes::Bytes;

use crate::auth::policy::FullAccessPolicy;
use axum::headers::HeaderMapExt;
use hyper::HeaderMap;

use std::sync::{Arc, RwLock};

use crate::auth::proto::token::Permissions;
use crate::auth::proto::{Token, TokenCreateResponse, TokenRepo};
use crate::core::status::HttpError;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::HttpServerComponents;

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

impl TokenApi {
    // GET /tokens
    pub async fn token_list(
        State(components): State<Arc<RwLock<HttpServerComponents>>>,
        headers: HeaderMap,
    ) -> Result<TokenRepo, HttpError> {
        check_permissions(Arc::clone(&components), headers, FullAccessPolicy {})?;
        let components = components.write().unwrap();

        let mut list = TokenRepo::default();
        for x in components.token_repo.get_token_list()?.iter() {
            list.tokens.push(x.clone());
        }
        list.tokens.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(list)
    }

    // POST /tokens/:token_name
    pub async fn create_token(
        State(components): State<Arc<RwLock<HttpServerComponents>>>,
        Path(token_name): Path<String>,
        headers: HeaderMap,
        permissions: Permissions,
    ) -> Result<TokenCreateResponse, HttpError> {
        check_permissions(Arc::clone(&components), headers, FullAccessPolicy {})?;

        let mut components = components.write().unwrap();
        components.token_repo.create_token(&token_name, permissions)
    }

    // GET /tokens/:token_name
    pub async fn get_token(
        State(components): State<Arc<RwLock<HttpServerComponents>>>,
        Path(token_name): Path<String>,
        headers: HeaderMap,
    ) -> Result<Token, HttpError> {
        check_permissions(Arc::clone(&components), headers, FullAccessPolicy {})?;

        let components = components.read().unwrap();
        components.token_repo.find_by_name(&token_name)
    }

    // DELETE /tokens/:name
    pub async fn remove_token(
        State(components): State<Arc<RwLock<HttpServerComponents>>>,
        Path(token_name): Path<String>,
        headers: HeaderMap,
    ) -> Result<(), HttpError> {
        check_permissions(Arc::clone(&components), headers, FullAccessPolicy {})?;

        let mut components = components.write().unwrap();
        components.token_repo.remove_token(&token_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::asset_manager::ZipAssetManager;
    use crate::auth::token_auth::TokenAuthorization;
    use crate::auth::token_repository::create_token_repository;
    use crate::storage::storage::Storage;

    use crate::http_frontend::bucket_api::BucketApi;
    use crate::storage::proto::BucketSettings;
    use axum::headers::Authorization;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_token_list() {
        let components = setup();
        let list = TokenApi::token_list(State(components), auth_headers())
            .await
            .unwrap();
        assert_eq!(list.tokens.len(), 2);
        assert_eq!(list.tokens[0].name, "init-token");
        assert_eq!(list.tokens[1].name, "test");
    }

    #[tokio::test]
    async fn test_create_token() {
        let components = setup();
        let token = TokenApi::create_token(
            State(components),
            Path("new-token".to_string()),
            auth_headers(),
            Permissions::default(),
        )
        .await
        .unwrap();
        assert!(token.value.starts_with("new-token"));
    }

    #[tokio::test]
    async fn test_get_token() {
        let components = setup();
        let token =
            TokenApi::get_token(State(components), Path("test".to_string()), auth_headers())
                .await
                .unwrap();
        assert_eq!(token.name, "test");
    }

    #[tokio::test]
    async fn test_remove_token() {
        let components = setup();
        let token =
            TokenApi::remove_token(State(components), Path("test".to_string()), auth_headers())
                .await;
        assert!(token.is_ok());
    }

    #[tokio::test]
    async fn test_remove_bucket_from_permission() {
        let components = setup();
        let token = TokenApi::get_token(
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

        let token =
            TokenApi::get_token(State(components), Path("test".to_string()), auth_headers())
                .await
                .unwrap();
        assert_eq!(
            token.permissions.unwrap().read,
            vec!["bucket-2".to_string()]
        );
    }

    fn setup() -> Arc<RwLock<HttpServerComponents>> {
        let data_path = tempfile::tempdir().unwrap().into_path();

        let mut components = HttpServerComponents {
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

    fn auth_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.typed_insert(Authorization::bearer("init-token").unwrap());
        headers
    }
}
