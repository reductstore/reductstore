// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

mod alive;
mod info;
mod list;
mod ready;

use crate::api::http::StateKeeper;

use crate::api::http::token::me::me;
use axum::routing::{get, head};
use axum_extra::headers::HeaderMapExt;
use reduct_base::msg::server_api::{BucketInfoList, ServerInfo};
use reduct_macros::{IntoResponse, Twin};
use std::sync::Arc;

#[derive(IntoResponse, Twin)]
pub(super) struct ServerInfoAxum(ServerInfo);

#[derive(IntoResponse, Twin)]
pub(super) struct BucketInfoListAxum(BucketInfoList);

pub(super) fn create_server_api_routes() -> axum::Router<Arc<StateKeeper>> {
    axum::Router::new()
        .route("/list", get(list::list))
        .route("/info", get(info::info))
        .route("/alive", head(alive::alive))
        .route("/alive", get(alive::alive))
        .route("/ready", head(ready::ready))
        .route("/ready", get(ready::ready))
        .route("/me", get(me))
}
