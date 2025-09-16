// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::query_link::create::create;
use crate::api::Components;
use axum::routing::post;
use axum_extra::headers::HeaderMapExt;
use reduct_base::msg::query_link_api::{QueryLinkCreateRequest, QueryLinkCreateResponse};
use reduct_macros::{IntoResponse, Twin};
use std::sync::Arc;

mod create;

#[derive(IntoResponse, Twin)]
pub(super) struct QueryLinkCreateRequestAxum(QueryLinkCreateRequest);

#[derive(IntoResponse, Twin)]
pub(super) struct QueryLinkCreateResponseAxum(QueryLinkCreateResponse);

pub(super) fn create_query_link_api_routes() -> axum::Router<Arc<Components>> {
    axum::Router::new().route("/", post(create))
}
