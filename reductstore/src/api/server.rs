// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

mod info;
mod list;

use std::sync::Arc;

use axum::headers;
use axum::headers::HeaderMapExt;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, head};

use reduct_base::msg::server_api::{BucketInfoList, ServerInfo};
use reduct_macros::{IntoResponse, Twin};

use crate::api::token::me::me;
use crate::api::Components;

#[derive(IntoResponse, Twin)]
pub struct ServerInfoAxum(ServerInfo);

#[derive(IntoResponse, Twin)]
pub struct BucketInfoListAxum(BucketInfoList);

pub(crate) fn create_server_api_routes() -> axum::Router<Arc<Components>> {
    axum::Router::new()
        .route("/list", get(list::list))
        .route("/info", get(info::info))
        .route("/alive", head(|| async { StatusCode::OK }))
        .route("/me", get(me))
}
