// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

mod create;
mod get;
mod head;
mod remove;
mod rename;
mod update;

use crate::api::bucket::create::create_bucket;
use crate::api::bucket::get::get_bucket;
use crate::api::bucket::head::head_bucket;
use crate::api::bucket::remove::remove_bucket;
use crate::api::bucket::rename::rename_bucket;
use crate::api::bucket::update::update_bucket;
use crate::api::{Components, HttpError};
use axum::async_trait;
use axum::body::Body;
use axum::extract::FromRequest;
use axum::http::Request;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, head, post, put};
use axum_extra::headers::HeaderMapExt;
use bytes::Bytes;
use reduct_base::msg::bucket_api::{BucketSettings, FullBucketInfo};
use reduct_macros::{IntoResponse, Twin};
use std::sync::Arc;

//
// BucketSettings wrapper
//
#[derive(IntoResponse, Twin)]
pub struct BucketSettingsAxum(BucketSettings);

impl Default for BucketSettingsAxum {
    fn default() -> Self {
        Self(BucketSettings::default())
    }
}

#[derive(IntoResponse, Twin)]
pub struct FullBucketInfoAxum(FullBucketInfo);

#[async_trait]
impl<S> FromRequest<S> for BucketSettingsAxum
where
    Bytes: FromRequest<S>,
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(req: Request<Body>, state: &S) -> Result<Self, Self::Rejection> {
        let body = Bytes::from_request(req, state)
            .await
            .map_err(IntoResponse::into_response)?;

        if body.is_empty() {
            return Ok(Self::default());
        }

        let settings: BucketSettings =
            serde_json::from_slice(&body).map_err(|e| HttpError::from(e).into_response())?;
        Ok(Self(settings))
    }
}

pub(crate) fn create_bucket_api_routes() -> axum::Router<Arc<Components>> {
    axum::Router::new()
        .route("/:bucket_name", get(get_bucket))
        .route("/:bucket_name", head(head_bucket))
        .route("/:bucket_name", post(create_bucket))
        .route("/:bucket_name", put(update_bucket))
        .route("/:bucket_name", delete(remove_bucket))
        .route("/:bucket_name/rename", put(rename_bucket))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Method;

    #[tokio::test]
    async fn test_bucket_settings_quota_parsing() {
        let req = Request::builder()
            .method(Method::POST)
            .uri("/b/bucket-1")
            .header("Content-Type", "application/json")
            .body(Body::from(r#"{"quota_type": 1}"#))
            .unwrap();
        let resp = BucketSettingsAxum::from_request(req, &())
            .await
            .err()
            .unwrap();
        assert_eq!(resp.status(), 422);
        assert_eq!(
            resp.headers()
                .get("x-reduct-error")
                .unwrap()
                .to_str()
                .unwrap(),
            "Invalid JSON: expected value at line 1 column 16"
        );
    }
}
