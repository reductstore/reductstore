// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

mod create;
mod get;
mod list;
mod remove;
mod update;

use crate::api::http::lifecycle::create::create_lifecycle_policy;
use crate::api::http::lifecycle::get::get_lifecycle_policy;
use crate::api::http::lifecycle::list::list_lifecycle_policies;
use crate::api::http::lifecycle::remove::remove_lifecycle_policy;
use crate::api::http::lifecycle::update::update_lifecycle_policy;
use crate::api::http::{HttpError, StateKeeper};
use axum::body::Body;
use axum::extract::FromRequest;
use axum::http::Request;
use axum::routing::{delete, get, post, put};
use axum_extra::headers::HeaderMapExt;
use bytes::Bytes;
use reduct_base::msg::lifecycle_api::{FullLifecycleInfo, LifecycleList};
use reduct_macros::{IntoResponse, Twin};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(super) struct BucketLifecyclePolicyBody {
    #[serde(default)]
    pub entries: Vec<String>,
    pub max_age: String,
    #[serde(default)]
    pub when: Option<serde_json::Value>,
}

#[derive(IntoResponse, Twin, Debug)]
pub(super) struct BucketLifecyclePolicyBodyAxum(BucketLifecyclePolicyBody);

impl<S> FromRequest<S> for BucketLifecyclePolicyBodyAxum
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
        match serde_json::from_slice::<BucketLifecyclePolicyBody>(&*bytes) {
            Ok(x) => Ok(BucketLifecyclePolicyBodyAxum::from(x)),
            Err(e) => Err(e.into()),
        }
    }
}

#[derive(IntoResponse, Twin, Default)]
pub(super) struct LifecyclePolicyListAxum(LifecycleList);

#[derive(IntoResponse, Twin, Debug)]
pub(super) struct FullLifecyclePolicyInfoAxum(FullLifecycleInfo);

#[derive(Deserialize)]
pub(super) struct PolicyPath {
    pub bucket_name: String,
    pub policy_id: String,
}

pub(super) fn create_lifecycle_policy_api_routes() -> axum::Router<Arc<StateKeeper>> {
    axum::Router::new()
        .route(
            "/{bucket_name}/lifecycle-policies",
            get(list_lifecycle_policies),
        )
        .route(
            "/{bucket_name}/lifecycle-policies/{policy_id}",
            post(create_lifecycle_policy),
        )
        .route(
            "/{bucket_name}/lifecycle-policies/{policy_id}",
            get(get_lifecycle_policy),
        )
        .route(
            "/{bucket_name}/lifecycle-policies/{policy_id}",
            put(update_lifecycle_policy),
        )
        .route(
            "/{bucket_name}/lifecycle-policies/{policy_id}",
            delete(remove_lifecycle_policy),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::fixture;

    #[fixture]
    pub(super) fn policy_body() -> BucketLifecyclePolicyBody {
        BucketLifecyclePolicyBody {
            entries: vec![],
            max_age: "1d".to_string(),
            when: None,
        }
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
        async fn test_body_ok() {
            let json = r#"{"entries":["sensors/*"],"max_age":"P30D"}"#;
            let req = Request::builder().body(Body::from(json)).unwrap();
            let body = BucketLifecyclePolicyBodyAxum::from_request(req, &())
                .await
                .unwrap();
            assert_eq!(body.0.max_age, "P30D");
        }

        #[rstest]
        #[tokio::test]
        async fn test_body_invalid_json() {
            let req = Request::builder().body(Body::from("{bad")).unwrap();
            let err = BucketLifecyclePolicyBodyAxum::from_request(req, &())
                .await
                .unwrap_err();
            assert_eq!(err.status(), UnprocessableEntity);
        }

        #[rstest]
        #[tokio::test]
        async fn test_body_stream_error() {
            let stream = stream::once(async {
                Err::<Bytes, _>(io::Error::new(io::ErrorKind::Other, "boom"))
            });
            let req = Request::builder().body(Body::from_stream(stream)).unwrap();
            let err = BucketLifecyclePolicyBodyAxum::from_request(req, &())
                .await
                .unwrap_err();
            assert_eq!(err.status(), UnprocessableEntity);
            assert_eq!(err.message(), "Invalid body");
        }
    }
}
