// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1
mod create;
mod get;
mod list;
mod remove;
mod set_mode;
mod update;

use crate::api::{HttpError, StateKeeper};
use axum_extra::headers::HeaderMapExt;

use crate::api::replication::create::create_replication;
use crate::api::replication::get::get_replication;
use crate::api::replication::list::list_replications;
use crate::api::replication::remove::remove_replication;
use crate::api::replication::set_mode::set_mode;
use crate::api::replication::update::update_replication;
use axum::body::Body;
use axum::extract::FromRequest;
use axum::http::Request;
use axum::routing::{delete, get, patch, post, put};
use bytes::Bytes;
use reduct_base::msg::replication_api::{
    FullReplicationInfo, ReplicationList, ReplicationModePayload, ReplicationSettings,
};
use reduct_macros::{IntoResponse, Twin};
use std::sync::Arc;

#[derive(IntoResponse, Twin, Debug)]
pub struct ReplicationSettingsAxum(ReplicationSettings);

#[derive(IntoResponse, Twin, Debug)]
pub struct ReplicationModePayloadAxum(ReplicationModePayload);

impl<S> FromRequest<S> for ReplicationModePayloadAxum
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
        let response = match serde_json::from_slice::<ReplicationModePayload>(&*bytes) {
            Ok(x) => Ok(ReplicationModePayloadAxum::from(x)),
            Err(e) => Err(crate::api::HttpError::from(e)),
        };
        response
    }
}

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
pub(super) struct ReplicationListAxum(ReplicationList);

#[derive(IntoResponse, Twin)]
pub(super) struct ReplicationFullInfoAxum(FullReplicationInfo);

pub(super) fn create_replication_api_routes() -> axum::Router<Arc<StateKeeper>> {
    axum::Router::new()
        .route("/", get(list_replications))
        .route("/{replication_name}", get(get_replication))
        .route("/{replication_name}", post(create_replication))
        .route("/{replication_name}", put(update_replication))
        .route("/{replication_name}/mode", patch(set_mode))
        .route("/{replication_name}", delete(remove_replication))
}

#[cfg(test)]
mod tests {
    use reduct_base::msg::replication_api::{ReplicationMode, ReplicationSettings};
    use reduct_base::Labels;
    use rstest::fixture;

    #[fixture]
    pub(super) fn settings() -> ReplicationSettings {
        ReplicationSettings {
            src_bucket: "bucket-1".to_string(),
            dst_bucket: "bucket-2".to_string(),
            dst_host: "http://localhost".to_string(),
            dst_token: Some("token".to_string()),
            entries: vec![],
            include: Labels::default(),
            exclude: Labels::default(),
            each_n: None,
            each_s: None,
            when: None,
            mode: ReplicationMode::Enabled,
        }
    }

    mod from_request {
        use super::*;
        use crate::api::replication::ReplicationModePayloadAxum;
        use axum::body::Body;
        use axum::extract::FromRequest;
        use axum::http::Request;
        use reduct_base::error::ErrorCode::UnprocessableEntity;
        use rstest::rstest;

        #[rstest]
        #[tokio::test]
        async fn test_replication_mode_payload_ok() {
            let req = Request::builder()
                .body(Body::from(r#"{"mode":"paused"}"#))
                .unwrap();

            let payload = ReplicationModePayloadAxum::from_request(req, &())
                .await
                .expect("parse payload");
            assert_eq!(payload.0.mode, ReplicationMode::Paused);
        }

        #[rstest]
        #[tokio::test]
        async fn test_replication_mode_payload_invalid_json() {
            let req = Request::builder().body(Body::from("{bad json")).unwrap();

            let err = ReplicationModePayloadAxum::from_request(req, &())
                .await
                .expect_err("should fail");
            assert_eq!(err.0.status, UnprocessableEntity);
        }
    }
}
