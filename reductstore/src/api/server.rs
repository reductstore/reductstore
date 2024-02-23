// Copyright 2023-2024 ReductStore
// Licensed under the Business Source License 1.1

mod info;
mod list;

use crate::api::token::me::me;
use crate::api::Components;

use axum::http::StatusCode;
use axum::routing::{get, head};
use axum_extra::headers::HeaderMapExt;
use reduct_base::msg::server_api::{BucketInfoList, ServerInfo};
use reduct_macros::{IntoResponse, Twin};
use std::sync::Arc;

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
