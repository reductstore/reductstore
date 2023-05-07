// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::sync::{Arc, RwLock};

use axum::extract::State;
use axum::headers;
use axum::headers::HeaderMapExt;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::json;

use crate::auth::policy::AuthenticatedPolicy;
use crate::auth::proto::Token;
use crate::core::status::HttpError;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::HttpServerComponents;
use crate::storage::proto::bucket_settings::QuotaType;
use crate::storage::proto::{BucketInfoList, ServerInfo};

pub struct ServerApi {}

impl IntoResponse for ServerInfo {
    fn into_response(self) -> Response {
        let mut headers = HeaderMap::new();
        headers.typed_insert(headers::ContentType::json());

        let mut body = serde_json::to_value(&self).unwrap();
        *body
            .get_mut("defaults")
            .unwrap()
            .get_mut("bucket")
            .unwrap()
            .get_mut("quota_type")
            .unwrap() = json!(QuotaType::from_i32(
            self.defaults.unwrap().bucket.unwrap().quota_type.unwrap()
        )
        .unwrap()
        .as_str_name());
        (StatusCode::OK, headers, body.to_string()).into_response()
    }
}

impl IntoResponse for BucketInfoList {
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

impl ServerApi {
    // GET /info
    pub async fn info(
        State(components): State<Arc<RwLock<HttpServerComponents>>>,
        headers: HeaderMap,
    ) -> Result<ServerInfo, HttpError> {
        check_permissions(Arc::clone(&components), headers, AuthenticatedPolicy {})?;
        components.read().unwrap().storage.info()
    }

    // GET /list
    pub async fn list(
        State(components): State<Arc<RwLock<HttpServerComponents>>>,
        headers: HeaderMap,
    ) -> Result<BucketInfoList, HttpError> {
        check_permissions(Arc::clone(&components), headers, AuthenticatedPolicy {})?;
        components.read().unwrap().storage.get_bucket_list()
    }

    // // GET /me
    pub async fn me(
        State(components): State<Arc<RwLock<HttpServerComponents>>>,
        headers: HeaderMap,
    ) -> Result<Token, HttpError> {
        check_permissions(
            Arc::clone(&components),
            headers.clone(),
            AuthenticatedPolicy {},
        )?;
        let header = match headers.get("Authorization") {
            Some(header) => header.to_str().ok(),
            None => None,
        };
        components.read().unwrap().token_repo.validate_token(header)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use hyper::http::request::Builder;

    use crate::asset::asset_manager::ZipAssetManager;
    use crate::auth::token_auth::TokenAuthorization;
    use crate::auth::token_repository::TokenRepository;
    use crate::storage::proto::BucketSettings;
    use crate::storage::storage::Storage;

    use super::*;

    #[tokio::test]
    async fn test_info() {
        let components = setup();
        let info = ServerApi::info(State(components), HeaderMap::new())
            .await
            .unwrap();
        assert_eq!(info.bucket_count, 2);
    }

    #[tokio::test]
    async fn test_list() {
        let components = setup();
        let list = ServerApi::list(State(components), HeaderMap::new())
            .await
            .unwrap();
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
