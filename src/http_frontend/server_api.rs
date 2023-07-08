// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod info;
mod list;
mod me;

use std::sync::{Arc, RwLock};

use axum::headers;
use axum::headers::HeaderMapExt;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, head};
use serde_json::json;

use crate::http_frontend::HttpServerState;
use crate::storage::proto::bucket_settings::QuotaType;
use crate::storage::proto::{BucketInfoList, ServerInfo};

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

pub fn create_server_api_routes() -> axum::Router<Arc<RwLock<HttpServerState>>> {
    axum::Router::new()
        .route("/list", get(list::list))
        .route("/me", get(me::me))
        .route("/info", get(info::info))
        .route("/alive", head(|| async { StatusCode::OK }))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use hyper::http::request::Builder;
    use rstest::fixture;

    use crate::asset::asset_manager::ZipAssetManager;
    use crate::auth::token_auth::TokenAuthorization;
    use crate::auth::token_repository::create_token_repository;
    use crate::storage::proto::BucketSettings;
    use crate::storage::storage::Storage;

    use super::*;

    #[fixture]
    pub(crate) fn tmp_components() -> Arc<RwLock<HttpServerState>> {
        setup()
    }

    fn setup() -> Arc<RwLock<HttpServerState>> {
        let data_path = tempfile::tempdir().unwrap().into_path();

        let mut components = HttpServerState {
            storage: Storage::new(PathBuf::from(data_path.clone())),
            auth: TokenAuthorization::new(""),
            token_repo: create_token_repository(data_path.clone(), ""),
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

        let _req = Builder::new().body(()).unwrap();
        Arc::new(RwLock::new(components))
    }
}
