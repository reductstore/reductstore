// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::auth::proto::token::Permissions;
use http_body_util::BodyExt;

use hyper::Request;
use prost::Message;

use std::fmt::Display;
use std::sync::{Arc, RwLock};

use crate::auth::proto::{Token, TokenCreateResponse, TokenRepo};
use crate::core::status::HttpError;
use crate::http_frontend::http_server::HttpServerComponents;

pub struct TokenApi {}

impl TokenApi {
    // GET /tokens
    pub async fn token_list<Body>(
        components: Arc<RwLock<HttpServerComponents>>,
        _: Request<Body>,
    ) -> Result<TokenRepo, HttpError> {
        let components = components.write().unwrap();

        let mut list = TokenRepo::default();
        for x in components.token_repo.get_token_list()?.iter() {
            list.tokens.push(x.clone());
        }
        list.tokens.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(list)
    }

    // POST /tokens/:name
    pub async fn create_token<Body>(
        components: Arc<RwLock<HttpServerComponents>>,
        req: Request<Body>,
    ) -> Result<TokenCreateResponse, HttpError>
    where
        Body: BodyExt,
        Body::Error: Display,
    {
        let name = req.uri().path().split("/").last().unwrap().to_string();

        let data = match req.into_body().collect().await {
            Ok(body) => body.to_bytes(),
            Err(e) => return Err(HttpError::bad_request(&e.to_string())),
        };

        let permissions = match Permissions::decode(data) {
            Ok(permissions) => permissions,
            Err(e) => return Err(HttpError::bad_request(&e.to_string())),
        };

        let mut components = components.write().unwrap();
        components.token_repo.create_token(&name, permissions)
    }

    // GET /tokens/:name
    pub async fn get_token<Body>(
        components: Arc<RwLock<HttpServerComponents>>,
        req: Request<Body>,
    ) -> Result<Token, HttpError> {
        let name = req.uri().path().split("/").last().unwrap().to_string();

        let components = components.read().unwrap();
        components.token_repo.find_by_name(&name)
    }

    // DELETE /tokens/:name
    pub async fn remove_token<Body>(
        components: Arc<RwLock<HttpServerComponents>>,
        req: Request<Body>,
    ) -> Result<(), HttpError> {
        let name = req.uri().path().split("/").last().unwrap().to_string();

        let mut components = components.write().unwrap();
        components.token_repo.remove_token(&name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::asset_manager::ZipAssetManager;
    use crate::auth::token_auth::TokenAuthorization;
    use crate::auth::token_repository::TokenRepository;
    use crate::storage::storage::Storage;
    use bytes::Bytes;
    use http_body_util::Full;
    use hyper::http::request::Builder;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_token_list() {
        let (components, req) = setup();

        let list = TokenApi::token_list(components, req).await.unwrap();
        assert_eq!(list.tokens.len(), 1);
        assert_eq!(list.tokens[0].name, "test");
    }

    #[tokio::test]
    async fn test_create_token() {
        let (components, _) = setup();

        let _permissions = Permissions::default();

        let req = Builder::new()
            .uri("/tokens/new-token")
            .body(Full::new(Bytes::from("")))
            .unwrap();

        let token = TokenApi::create_token(components, req).await.unwrap();
        assert!(token.value.starts_with("new-token"));
    }

    #[tokio::test]
    async fn test_get_token() {
        let (components, req) = setup();

        let token = TokenApi::get_token(components, req).await.unwrap();
        assert_eq!(token.name, "test");
    }

    #[tokio::test]
    async fn test_remove_token() {
        let (components, req) = setup();

        let token = TokenApi::get_token(components.clone(), req).await.unwrap();
        assert_eq!(token.name, "test");

        let req = Builder::new()
            .uri("/tokens/test")
            .body(Full::new(Bytes::from("")))
            .unwrap();

        let token = TokenApi::remove_token(components, req).await;
        assert!(token.is_ok());
    }

    fn setup() -> (Arc<RwLock<HttpServerComponents>>, Request<Full<Bytes>>) {
        let data_path = tempfile::tempdir().unwrap().into_path();

        let mut components = HttpServerComponents {
            storage: Storage::new(PathBuf::from(data_path.clone())),
            auth: TokenAuthorization::new("inti-token"),
            token_repo: TokenRepository::new(PathBuf::from(data_path), "init-token"),
            console: ZipAssetManager::new(""),
        };

        components
            .token_repo
            .create_token("test", Permissions::default())
            .unwrap();

        let req = Builder::new()
            .uri("/tokens/test")
            .body(Full::new(Bytes::from("")))
            .unwrap();
        (Arc::new(RwLock::new(components)), req)
    }
}
