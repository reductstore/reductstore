// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::sync::{Arc, RwLock};

use crate::auth::policy::{AuthenticatedPolicy, FullAccessPolicy};
use axum::extract::{FromRequest, Path, State};
use axum::headers::HeaderMapExt;
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{async_trait, headers};
use bytes::Bytes;
use hyper::HeaderMap;
use serde_json::{json, Value};

use crate::core::status::HttpError;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::HttpServerComponents;
use crate::storage::proto::bucket_settings::QuotaType;
use crate::storage::proto::BucketSettings;
use crate::storage::proto::FullBucketInfo;

pub struct BucketApi {}

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
                        let val = QuotaType::from_str_name(quota_type.as_str().unwrap()).ok_or(
                            HttpError::unprocessable_entity("Invalid quota type").into_response(),
                        )? as i32;
                        *quota_type = json!(val);
                    }
                }
                None => {}
            }

            serde_json::from_value(json).map_err(|e| HttpError::from(e).into_response())?
        };
        Ok(settings)
    }
}

impl BucketApi {
    // GET /b/:bucket_name
    pub async fn get_bucket(
        State(components): State<Arc<RwLock<HttpServerComponents>>>,
        Path(bucket_name): Path<String>,
        headers: HeaderMap,
    ) -> Result<FullBucketInfo, HttpError> {
        check_permissions(Arc::clone(&components), headers, AuthenticatedPolicy {})?;
        let mut components = components.write().unwrap();
        components.storage.get_bucket(&bucket_name)?.info()
    }

    // HEAD /b/:bucket_name
    pub async fn head_bucket(
        State(components): State<Arc<RwLock<HttpServerComponents>>>,
        Path(bucket_name): Path<String>,
        headers: HeaderMap,
    ) -> Result<(), HttpError> {
        check_permissions(Arc::clone(&components), headers, AuthenticatedPolicy {})?;
        let mut components = components.write().unwrap();
        components.storage.get_bucket(&bucket_name)?;
        Ok(())
    }

    // POST /b/:bucket_name
    pub async fn create_bucket(
        State(components): State<Arc<RwLock<HttpServerComponents>>>,
        Path(bucket_name): Path<String>,
        headers: HeaderMap,
        settings: BucketSettings,
    ) -> Result<(), HttpError> {
        check_permissions(Arc::clone(&components), headers, FullAccessPolicy {})?;
        let mut components = components.write().unwrap();
        components
            .storage
            .create_bucket(&bucket_name, settings.into())?;
        Ok(())
    }

    // PUT /b/:bucket_name
    pub async fn update_bucket(
        State(components): State<Arc<RwLock<HttpServerComponents>>>,
        Path(bucket_name): Path<String>,
        headers: HeaderMap,
        settings: BucketSettings,
    ) -> Result<(), HttpError> {
        check_permissions(Arc::clone(&components), headers, FullAccessPolicy {})?;
        let mut components = components.write().unwrap();
        let bucket = components.storage.get_bucket(&bucket_name)?;
        bucket.set_settings(settings.into())
    }

    // DELETE /b/:bucket_name
    pub async fn remove_bucket(
        State(components): State<Arc<RwLock<HttpServerComponents>>>,
        Path(bucket_name): Path<String>,
        headers: HeaderMap,
    ) -> Result<(), HttpError> {
        check_permissions(Arc::clone(&components), headers, FullAccessPolicy {})?;
        let mut components = components.write().unwrap();
        components.storage.remove_bucket(&bucket_name)?;
        components
            .token_repo
            .remove_bucket_from_tokens(&bucket_name)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::asset_manager::ZipAssetManager;
    use crate::auth::token_auth::TokenAuthorization;
    use crate::auth::token_repository::create_token_repository;
    use crate::http_frontend::HttpServerComponents;
    use crate::storage::proto::BucketSettings;
    use crate::storage::storage::Storage;

    use std::path::PathBuf;
    use std::sync::{Arc, RwLock};

    #[tokio::test]
    async fn test_get_bucket() {
        let components = setup();
        let info = BucketApi::get_bucket(
            State(components),
            Path("bucket-1".to_string()),
            HeaderMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(info.info.unwrap().name, "bucket-1");
    }

    #[tokio::test]
    async fn test_head_bucket() {
        let components = setup();
        BucketApi::head_bucket(
            State(components),
            Path("bucket-1".to_string()),
            HeaderMap::new(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_create_bucket() {
        let components = setup();
        BucketApi::create_bucket(
            State(components),
            Path("bucket-2".to_string()),
            HeaderMap::new(),
            BucketSettings::default(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_update_bucket() {
        let components = setup();
        BucketApi::update_bucket(
            State(components),
            Path("bucket-1".to_string()),
            HeaderMap::new(),
            BucketSettings::default(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_remove_bucket() {
        {
            let components = setup();
            BucketApi::remove_bucket(
                State(components),
                Path("bucket-1".to_string()),
                HeaderMap::new(),
            )
            .await
            .unwrap();
        }
    }

    fn setup() -> Arc<RwLock<HttpServerComponents>> {
        let data_path = tempfile::tempdir().unwrap().into_path();

        let mut components = HttpServerComponents {
            storage: Storage::new(PathBuf::from(data_path.clone())),
            auth: TokenAuthorization::new(""),
            token_repo: create_token_repository(data_path.clone(), ""),
            console: ZipAssetManager::new(&[]),
            base_path: "/".to_string(),
        };

        components
            .storage
            .create_bucket("bucket-1", BucketSettings::default())
            .unwrap();

        Arc::new(RwLock::new(components))
    }
}
