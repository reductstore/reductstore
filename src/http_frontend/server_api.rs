// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::auth::proto::Token;
use crate::core::status::HttpError;
use crate::http_frontend::http_server::HttpServerComponents;
use crate::storage::proto::{BucketInfoList, ServerInfo};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use std::sync::{Arc, RwLock};

pub struct ServerApi {}

impl IntoResponse for ServerInfo {
    fn into_response(self) -> Response {
        (StatusCode::OK, serde_json::to_string(&self).unwrap()).into_response()
    }
}

impl IntoResponse for BucketInfoList {
    fn into_response(self) -> Response {
        (StatusCode::OK, serde_json::to_string(&self).unwrap()).into_response()
    }
}

impl ServerApi {
    // GET /info
    pub async fn info(
        State(components): State<Arc<RwLock<HttpServerComponents>>>,
    ) -> Result<ServerInfo, HttpError> {
        components.read().unwrap().storage.info()
    }

    // GET /list
    pub async fn list(
        State(components): State<Arc<RwLock<HttpServerComponents>>>,
    ) -> Result<BucketInfoList, HttpError> {
        components.read().unwrap().storage.get_bucket_list()
    }

    // // GET /me
    pub async fn me(
        State(components): State<Arc<RwLock<HttpServerComponents>>>,
        headers: HeaderMap,
    ) -> Result<Token, HttpError> {
        let header = match headers.get("Authorization") {
            Some(header) => header.to_str().ok(),
            None => None,
        };
        components.read().unwrap().token_repo.validate_token(header)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::asset_manager::ZipAssetManager;
    use crate::auth::token_auth::TokenAuthorization;
    use crate::auth::token_repository::TokenRepository;
    use crate::storage::proto::BucketSettings;
    use crate::storage::storage::Storage;
    use hyper::http::request::Builder;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_info() {
        let components = setup();
        let info = ServerApi::info(State(components)).await.unwrap();
        assert_eq!(info.bucket_count, 2);
    }

    #[tokio::test]
    async fn test_list() {
        let components = setup();
        let list = ServerApi::list(State(components)).await.unwrap();
        assert_eq!(list.buckets.len(), 2);
    }

    #[tokio::test]
    async fn test_me() {
        let components = setup();
        let token = ServerApi::me(State(components), HeaderMap::new())
            .await
            .unwrap();
        assert_eq!(token.name, "AUTHENTICATION-DISABLED");
    }

    fn setup() -> Arc<RwLock<HttpServerComponents>> {
        let data_path = tempfile::tempdir().unwrap().into_path();

        let mut components = HttpServerComponents {
            storage: Storage::new(PathBuf::from(data_path.clone())),
            auth: TokenAuthorization::new(""),
            token_repo: TokenRepository::new(PathBuf::from(data_path), ""),
            console: ZipAssetManager::new(""),
        };

        components
            .storage
            .create_bucket("bucket-1", BucketSettings::default())
            .unwrap();
        components
            .storage
            .create_bucket("bucket-2", BucketSettings::default())
            .unwrap();

        let _req = Builder::new().body(()).unwrap();
        Arc::new(RwLock::new(components))
    }
}
