// Copyright 2024 ReductStore
// Licensed under the Business Source License 1.1
mod create;
mod get;
mod list;
mod remove;
mod update;

use crate::api::{Components, HttpError};
use axum_extra::headers::HeaderMapExt;

use crate::api::replication::create::create_replication;
use crate::api::replication::get::get_replication;
use crate::api::replication::list::list_replications;
use crate::api::replication::remove::remove_replication;
use crate::api::replication::update::update_replication;
use async_trait::async_trait;
use axum::body::Body;
use axum::extract::FromRequest;
use axum::http::Request;
use axum::routing::{delete, get, post, put};
use bytes::Bytes;
use reduct_base::msg::replication_api::{
    FullReplicationInfo, ReplicationList, ReplicationSettings,
};
use reduct_macros::{IntoResponse, Twin};
use std::sync::Arc;

#[derive(IntoResponse, Twin)]
pub struct ReplicationSettingsAxum(ReplicationSettings);

#[async_trait]
impl<S> FromRequest<S> for ReplicationSettingsAxum
where
    Bytes: FromRequest<S>,
    S: Send + Sync,
{
    type Rejection = HttpError;

    async fn from_request(req: Request<Body>, state: &S) -> Result<Self, Self::Rejection> {
        let bytes = Bytes::from_request(req, state).await.map_err(|_| {
            HttpError::new(
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

#[derive(IntoResponse, Twin, Default)]
pub struct ReplicationListAxum(ReplicationList);

#[derive(IntoResponse, Twin)]
pub struct ReplicationFullInfoAxum(FullReplicationInfo);

pub(crate) fn create_replication_api_routes() -> axum::Router<Arc<Components>> {
    axum::Router::new()
        .route("/", get(list_replications))
        .route("/:replication_name", get(get_replication))
        .route("/:replication_name", post(create_replication))
        .route("/:replication_name", put(update_replication))
        .route("/:replication_name", delete(remove_replication))
}

#[cfg(test)]
mod tests {
    use reduct_base::msg::replication_api::ReplicationSettings;
    use reduct_base::Labels;
    use rstest::fixture;

    #[fixture]
    pub(super) fn settings() -> ReplicationSettings {
        ReplicationSettings {
            src_bucket: "bucket-1".to_string(),
            dst_bucket: "bucket-2".to_string(),
            dst_host: "http://localhost".to_string(),
            dst_token: "token".to_string(),
            entries: vec![],
            include: Labels::default(),
            exclude: Labels::default(),
            each_n: None,
            each_s: None,
        }
    }
}
