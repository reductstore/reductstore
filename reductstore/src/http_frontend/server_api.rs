// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod info;
mod list;
mod me;

use std::sync::Arc;

use axum::headers;
use axum::headers::HeaderMapExt;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, head};
use reduct_base::msg::bucket_api::BucketInfo;
use reduct_base::msg::server_api::{BucketInfoList, ServerInfo};
use serde_json::json;

use crate::http_frontend::HttpServerState;

pub struct ServerInfoAxum(ServerInfo);

impl IntoResponse for ServerInfoAxum {
    fn into_response(self) -> Response {
        let mut headers = HeaderMap::new();
        headers.typed_insert(headers::ContentType::json());
        (
            StatusCode::OK,
            headers,
            serde_json::to_string(&self.0).unwrap(),
        )
            .into_response()
    }
}

impl From<ServerInfo> for ServerInfoAxum {
    fn from(info: ServerInfo) -> Self {
        Self(info)
    }
}

pub struct BucketInfoListAxum(BucketInfoList);

impl IntoResponse for BucketInfoListAxum {
    fn into_response(self) -> Response {
        let mut headers = HeaderMap::new();
        headers.typed_insert(headers::ContentType::json());
        (
            StatusCode::OK,
            headers,
            serde_json::to_string(&self.0).unwrap(),
        )
            .into_response()
    }
}

impl From<BucketInfoList> for BucketInfoListAxum {
    fn from(info: BucketInfoList) -> Self {
        Self(info)
    }
}

pub fn create_server_api_routes() -> axum::Router<Arc<HttpServerState>> {
    axum::Router::new()
        .route("/list", get(list::list))
        .route("/me", get(me::me))
        .route("/info", get(info::info))
        .route("/alive", head(|| async { StatusCode::OK }))
}
