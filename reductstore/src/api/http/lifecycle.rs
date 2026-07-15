// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

mod create;
mod get;
mod list;
mod remove;
mod set_mode;
mod update;

use crate::api::http::lifecycle::create::create_lifecycle_policy;
use crate::api::http::lifecycle::get::get_lifecycle_policy;
use crate::api::http::lifecycle::list::list_lifecycle_policies;
use crate::api::http::lifecycle::remove::remove_lifecycle_policy;
use crate::api::http::lifecycle::set_mode::set_mode;
use crate::api::http::lifecycle::update::update_lifecycle_policy;
use crate::api::http::{HttpError, StateKeeper};
use axum::body::Body;
use axum::extract::FromRequest;
use axum::http::Request;
use axum::routing::{delete, get, patch, post, put};
use axum_extra::headers::HeaderMapExt;
use bytes::Bytes;
use reduct_base::msg::lifecycle_api::{
    FullLifecycleInfo, LifecycleList, LifecycleModePayload, LifecycleSettings,
};
use reduct_macros::{IntoResponse, Twin};
use std::sync::Arc;

#[derive(IntoResponse, Twin, Debug)]
pub(super) struct LifecycleSettingsAxum(LifecycleSettings);

#[derive(IntoResponse, Twin, Debug)]
pub(super) struct LifecycleModePayloadAxum(LifecycleModePayload);

impl<S> FromRequest<S> for LifecycleModePayloadAxum
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
        let response = match serde_json::from_slice::<LifecycleModePayload>(&*bytes) {
            Ok(x) => Ok(LifecycleModePayloadAxum::from(x)),
            Err(e) => Err(crate::api::http::HttpError::from(e)),
        };
        response
    }
}

impl<S> FromRequest<S> for LifecycleSettingsAxum
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
        let response = match serde_json::from_slice::<LifecycleSettings>(&*bytes) {
            Ok(x) => Ok(LifecycleSettingsAxum::from(x)),
            Err(e) => Err(crate::api::http::HttpError::from(e)),
        };
        response
    }
}

#[derive(IntoResponse, Twin, Default)]
pub(super) struct LifecyclePolicyListAxum(LifecycleList);

#[derive(IntoResponse, Twin, Debug)]
pub(super) struct FullLifecyclePolicyInfoAxum(FullLifecycleInfo);

pub(super) fn create_lifecycle_policy_api_routes() -> axum::Router<Arc<StateKeeper>> {
    axum::Router::new()
        .route("/", get(list_lifecycle_policies))
        .route("/{policy_name}", get(get_lifecycle_policy))
        .route("/{policy_name}", post(create_lifecycle_policy))
        .route("/{policy_name}", put(update_lifecycle_policy))
        .route("/{policy_name}/mode", patch(set_mode))
        .route("/{policy_name}", delete(remove_lifecycle_policy))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::tests::keeper;
    use reduct_base::msg::lifecycle_api::{LifecycleMode, LifecycleSettings};
    use rstest::fixture;
    use std::sync::Arc;

    #[fixture]
    pub(super) fn settings() -> LifecycleSettings {
        LifecycleSettings {
            bucket: "bucket-1".to_string(),
            entries: vec![],
            older_than: "1d".to_string(),
            interval: "1h".to_string(),
            when: None,
            mode: LifecycleMode::Enabled,
            ..LifecycleSettings::default()
        }
    }

    #[fixture]
    pub(super) async fn keeper_with_policy(#[future] keeper: Arc<StateKeeper>) -> Arc<StateKeeper> {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        components
            .lifecycle_repo
            .write()
            .await
            .unwrap()
            .create_lifecycle("test-policy", settings())
            .await
            .unwrap();
        keeper
    }

    mod from_request {
        use super::*;
        use axum::body::Body;
        use axum::extract::FromRequest;
        use axum::http::Request;
        use futures_util::stream;
        use reduct_base::error::ErrorCode::UnprocessableEntity;
        use rstest::rstest;
        use std::io;

        #[rstest]
        #[tokio::test]
        async fn test_settings_ok() {
            let json = r#"{"type":"delete","bucket":"bucket-1","entries":["sensors/*"],"older_than":"P30D"}"#;
            let req = Request::builder().body(Body::from(json)).unwrap();
            let body = LifecycleSettingsAxum::from_request(req, &()).await.unwrap();
            assert_eq!(body.0.older_than, "P30D");
            assert_eq!(body.0.interval, "3600s");
        }

        #[rstest]
        #[tokio::test]
        async fn test_settings_with_per_run_limits() {
            let json = r#"{"type":"delete","bucket":"bucket-1","older_than":"30d","max_span_per_run":"6h","max_records_per_run":1000}"#;
            let req = Request::builder().body(Body::from(json)).unwrap();
            let body = LifecycleSettingsAxum::from_request(req, &()).await.unwrap();
            assert_eq!(body.0.max_span_per_run, Some("6h".to_string()));
            assert_eq!(body.0.max_records_per_run, Some(1000));
        }

        #[rstest]
        #[tokio::test]
        async fn test_settings_per_run_limits_default_to_none() {
            let json = r#"{"type":"delete","bucket":"bucket-1","older_than":"30d"}"#;
            let req = Request::builder().body(Body::from(json)).unwrap();
            let body = LifecycleSettingsAxum::from_request(req, &()).await.unwrap();
            assert_eq!(body.0.max_span_per_run, None);
            assert_eq!(body.0.max_records_per_run, None);
        }

        #[rstest]
        #[tokio::test]
        async fn test_settings_invalid_json() {
            let req = Request::builder().body(Body::from("{bad")).unwrap();
            let err = LifecycleSettingsAxum::from_request(req, &())
                .await
                .unwrap_err();
            assert_eq!(err.status(), UnprocessableEntity);
        }

        #[rstest]
        #[tokio::test]
        async fn test_settings_stream_error() {
            let stream = stream::once(async {
                Err::<Bytes, _>(io::Error::new(io::ErrorKind::Other, "boom"))
            });
            let req = Request::builder().body(Body::from_stream(stream)).unwrap();
            let err = LifecycleSettingsAxum::from_request(req, &())
                .await
                .unwrap_err();
            assert_eq!(err.status(), UnprocessableEntity);
            assert_eq!(err.message(), "Invalid body");
        }

        #[rstest]
        #[tokio::test]
        async fn test_mode_payload_ok() {
            let req = Request::builder()
                .body(Body::from(r#"{"mode":"disabled"}"#))
                .unwrap();

            let payload = LifecycleModePayloadAxum::from_request(req, &())
                .await
                .expect("parse payload");
            assert_eq!(payload.0.mode, LifecycleMode::Disabled);
        }

        #[rstest]
        #[tokio::test]
        async fn test_mode_payload_invalid_json() {
            let req = Request::builder().body(Body::from("{bad json")).unwrap();

            let err = LifecycleModePayloadAxum::from_request(req, &())
                .await
                .expect_err("should fail");
            assert_eq!(err.status(), UnprocessableEntity);
        }

        #[rstest]
        #[tokio::test]
        async fn test_mode_payload_body_error() {
            let stream = stream::once(async {
                Err::<Bytes, _>(io::Error::new(io::ErrorKind::Other, "boom"))
            });
            let req = Request::builder().body(Body::from_stream(stream)).unwrap();

            let err = LifecycleModePayloadAxum::from_request(req, &())
                .await
                .expect_err("should fail");
            assert_eq!(err.status(), UnprocessableEntity);
            assert_eq!(err.message(), "Invalid body");
        }
    }
}
