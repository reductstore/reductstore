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

use reduct_base::msg::server_api::{BucketInfoList, ServerInfo};
use reduct_macros::IntoResponse;

use crate::http_frontend::HttpServerState;

#[derive(IntoResponse)]
pub struct ServerInfoAxum(ServerInfo);

impl From<ServerInfo> for ServerInfoAxum {
    fn from(info: ServerInfo) -> Self {
        Self(info)
    }
}

#[derive(IntoResponse)]
pub struct BucketInfoListAxum(BucketInfoList);

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
