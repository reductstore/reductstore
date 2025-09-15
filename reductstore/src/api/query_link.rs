// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use axum_extra::headers::HeaderMapExt;
use reduct_base::msg::query_link_api::{QueryLinkCreateRequest, QueryLinkCreateResponse};
use reduct_macros::{IntoResponse, Twin};

mod create;

#[derive(IntoResponse, Twin)]
pub(super) struct QueryLinkCreateRequestAxum(QueryLinkCreateRequest);

#[derive(IntoResponse, Twin)]
pub(super) struct QueryLinkCreateResponseAxum(QueryLinkCreateResponse);
