// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

mod create;
mod get;
mod head;
mod remove;
mod update;

use std::sync::Arc;

use axum::extract::FromRequest;
use axum::headers::HeaderMapExt;
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, head, post, put};
use axum::{async_trait, headers};
use bytes::Bytes;
use hyper::HeaderMap;

use crate::api::bucket::create::create_bucket;
use crate::api::bucket::get::get_bucket;
use crate::api::bucket::head::head_bucket;
use crate::api::bucket::remove::remove_bucket;
use crate::api::bucket::update::update_bucket;

use crate::api::{Componentes, HttpError};
use reduct_base::msg::bucket_api::{BucketSettings, FullBucketInfo};
use reduct_macros::{IntoResponse, Twin};
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
impl<S, B> FromRequest<S, B> for BucketSettingsAxum
where
    Bytes: FromRequest<S, B>,
    B: Send + 'static,
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(req: Request<B>, state: &S) -> Result<Self, Self::Rejection> {
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

pub fn create_bucket_api_routes() -> axum::Router<Arc<Componentes>> {
    axum::Router::new()
        .route("/:bucket_name", get(get_bucket))
        .route("/:bucket_name", head(head_bucket))
        .route("/:bucket_name", post(create_bucket))
        .route("/:bucket_name", put(update_bucket))
        .route("/:bucket_name", delete(remove_bucket))
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::http::Method;
    use hyper::Body;

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
