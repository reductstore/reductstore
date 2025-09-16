// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::query_link::create::create;
use crate::api::replication::ReplicationSettingsAxum;
use crate::api::{Components, HttpError};
use axum::extract::FromRequest;
use axum::routing::post;
use axum_extra::headers::HeaderMapExt;
use bytes::Bytes;
use reduct_base::msg::query_link_api::{QueryLinkCreateRequest, QueryLinkCreateResponse};
use reduct_macros::{IntoResponse, Twin};
use std::sync::Arc;

mod create;

#[derive(Twin)]
pub(super) struct QueryLinkCreateRequestAxum(QueryLinkCreateRequest);

#[derive(IntoResponse, Twin)]
pub(super) struct QueryLinkCreateResponseAxum(QueryLinkCreateResponse);

impl<S> FromRequest<S> for QueryLinkCreateRequestAxum
where
    Bytes: FromRequest<S>,
    S: Send + Sync,
{
    type Rejection = HttpError;

    async fn from_request(
        req: axum::http::Request<axum::body::Body>,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        let bytes = Bytes::from_request(req, state).await.map_err(|_| {
            HttpError::new(
                reduct_base::error::ErrorCode::UnprocessableEntity,
                "Invalid body",
            )
        })?;
        let response = match serde_json::from_slice::<QueryLinkCreateRequest>(&*bytes) {
            Ok(x) => Ok(QueryLinkCreateRequestAxum::from(x)),
            Err(e) => Err(crate::api::HttpError::from(e)),
        };
        response
    }
}

pub(super) fn create_query_link_api_routes() -> axum::Router<Arc<Components>> {
    axum::Router::new().route("/", post(create))
}
