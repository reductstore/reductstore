// Copyright 2024 ReductStore
// Licensed under the Business Source License 1.1
mod create;

use crate::api::{Components, HttpError};
use axum::headers::HeaderMapExt;

use crate::api::replication::create::create_replication;
use async_trait::async_trait;
use axum::extract::FromRequest;
use axum::http::Request;
use axum::routing::post;
use bytes::Bytes;
use reduct_base::msg::replication_api::ReplicationSettings;
use reduct_macros::{IntoResponse, Twin};
use std::sync::Arc;

#[derive(IntoResponse, Twin, Default)]
pub struct ReplicationSettingsAxum(ReplicationSettings);
#[async_trait]
impl<S, B> FromRequest<S, B> for ReplicationSettingsAxum
where
    Bytes: FromRequest<S, B>,
    B: Send + 'static,
    S: Send + Sync,
{
    type Rejection = HttpError;

    async fn from_request(req: Request<B>, state: &S) -> Result<Self, Self::Rejection> {
        let bytes = Bytes::from_request(req, state).await.map_err(|_| {
            crate::api::HttpError::new(
                reduct_base::error::ErrorCode::UnprocessableEntity,
                "Invalid body",
            )
        })?;
        let response = match serde_json::from_slice::<ReplicationSettings>(&*bytes) {
            Ok(x) => Ok(ReplicationSettingsAxum::from(x)),
            Err(e) => Err(crate::api::HttpError::from(e)),
        };
        response
    }
}

pub(crate) fn create_replication_api_routes() -> axum::Router<Arc<Components>> {
    axum::Router::new().route("/:replication_name", post(create_replication))
}
