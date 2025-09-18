// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

mod alive;
mod info;
mod list;

use crate::api::Components;

use crate::api::token::me::me;
use axum::routing::{get, head};
use axum_extra::headers::HeaderMapExt;
use reduct_base::msg::server_api::{BucketInfoList, ServerInfo};
use reduct_macros::{IntoResponse, Twin};
use std::sync::Arc;

#[derive(IntoResponse, Twin)]
pub(super) struct ServerInfoAxum(ServerInfo);

#[derive(IntoResponse, Twin)]
pub(super) struct BucketInfoListAxum(BucketInfoList);

pub(super) fn create_server_api_routes() -> axum::Router<Arc<Components>> {
    axum::Router::new()
        .route("/list", get(list::list))
        .route("/info", get(info::info))
        .route("/alive", head(alive::alive))
        .route("/alive", get(alive::alive))
        .route("/me", get(me))
}
