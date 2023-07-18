// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod create;
mod get;
mod head;
mod remove;
mod update;

use std::sync::{Arc, RwLock};

use axum::extract::FromRequest;
use axum::headers::HeaderMapExt;
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, head, post, put};
use axum::{async_trait, headers};
use bytes::Bytes;
use hyper::HeaderMap;
use serde_json::{json, Value};

use crate::core::status::HttpError;
use crate::http_frontend::bucket_api::create::create_bucket;
use crate::http_frontend::bucket_api::get::get_bucket;
use crate::http_frontend::bucket_api::head::head_bucket;
use crate::http_frontend::bucket_api::remove::remove_bucket;
use crate::http_frontend::bucket_api::update::update_bucket;

use crate::http_frontend::HttpServerState;
use crate::storage::proto::bucket_settings::QuotaType;
use crate::storage::proto::BucketSettings;
use crate::storage::proto::FullBucketInfo;

impl IntoResponse for FullBucketInfo {
    fn into_response(self) -> Response {
        // Work around for string enum
        let mut body = serde_json::to_value(&self).unwrap();
        *body
            .get_mut("settings")
            .unwrap()
            .get_mut("quota_type")
            .unwrap() = json!(
            QuotaType::from_i32(self.settings.unwrap().quota_type.unwrap())
                .unwrap()
                .as_str_name()
        );

        let mut headers = HeaderMap::new();
        headers.typed_insert(headers::ContentType::json());
        (StatusCode::OK, headers, body.to_string()).into_response()
    }
}

impl IntoResponse for BucketSettings {
    fn into_response(self) -> Response {
        // Work around for string enum
        let mut body = serde_json::to_value(&self).unwrap();
        *body.get_mut("quota_type").unwrap() = json!(QuotaType::from_i32(self.quota_type.unwrap())
            .unwrap()
            .as_str_name());

        let mut headers = HeaderMap::new();
        headers.typed_insert(headers::ContentType::json());
        (StatusCode::OK, headers, body.to_string()).into_response()
    }
}

#[async_trait]
impl<S, B> FromRequest<S, B> for BucketSettings
where
    Bytes: FromRequest<S, B>,
    B: Send + 'static,
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(req: Request<B>, state: &S) -> Result<Self, Self::Rejection> {
        let bytes = Bytes::from_request(req, state)
            .await
            .map_err(IntoResponse::into_response)?;
        let settings = if bytes.is_empty() {
            BucketSettings::default()
        } else {
            let mut json: Value =
                serde_json::from_slice(&bytes).map_err(|e| HttpError::from(e).into_response())?;
            match json.get_mut("quota_type") {
                Some(quota_type) => {
                    if !quota_type.is_null() {
                        if let Some(quota_as_str) = quota_type.as_str() {
                            let val = QuotaType::from_str_name(quota_as_str).ok_or(
                                HttpError::unprocessable_entity("Invalid quota type")
                                    .into_response(),
                            )? as i32;
                            *quota_type = json!(val);
                        } else {
                            return Err(HttpError::unprocessable_entity(&format!(
                                "Failed to parse quota type: {:}",
                                quota_type
                            ))
                            .into_response());
                        }
                    }
                }
                None => {}
            }

            serde_json::from_value(json).map_err(|e| HttpError::from(e).into_response())?
        };
        Ok(settings)
    }
}

pub fn create_bucket_api_routes() -> axum::Router<Arc<HttpServerState>> {
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

    use crate::storage::proto::BucketSettings;

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
        let resp = BucketSettings::from_request(req, &()).await.err().unwrap();
        assert_eq!(resp.status(), 422);
        assert_eq!(
            resp.headers()
                .get("x-reduct-error")
                .unwrap()
                .to_str()
                .unwrap(),
            "Failed to parse quota type: 1"
        );
    }
}
