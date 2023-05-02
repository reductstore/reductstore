// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::core::status::HttpError;
use crate::http_frontend::HttpServerComponents;
use crate::storage::proto::{BucketInfo, BucketSettings};
use axum::async_trait;
use axum::extract::{FromRequest, Path, State};
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use std::sync::{Arc, RwLock};

pub struct BucketApi {}

impl IntoResponse for BucketInfo {
    fn into_response(self) -> Response {
        (StatusCode::OK, serde_json::to_string(&self).unwrap()).into_response()
    }
}

impl IntoResponse for BucketSettings {
    fn into_response(self) -> Response {
        (StatusCode::OK, "").into_response()
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
        let settings =
            serde_json::from_slice(&bytes).map_err(|e| HttpError::from(e).into_response())?;
        Ok(settings)
    }
}

impl BucketApi {
    // GET /b/:bucket_name
    pub async fn get_bucket(
        State(components): State<Arc<RwLock<HttpServerComponents>>>,
        Path(bucket_name): Path<String>,
    ) -> Result<BucketInfo, HttpError> {
        let mut components = components.write().unwrap();
        components.storage.get_bucket(&bucket_name)?.info()
    }

    // HEAD /b/:bucket_name
    pub async fn head_bucket(
        State(components): State<Arc<RwLock<HttpServerComponents>>>,
        Path(bucket_name): Path<String>,
    ) -> Result<(), HttpError> {
        let mut components = components.write().unwrap();
        components.storage.get_bucket(&bucket_name)?;
        Ok(())
    }

    // POST /b/:bucket_name
    pub async fn create_bucket(
        State(components): State<Arc<RwLock<HttpServerComponents>>>,
        Path(bucket_name): Path<String>,
        settings: BucketSettings,
    ) -> Result<(), HttpError> {
        let mut components = components.write().unwrap();
        components.storage.create_bucket(&bucket_name, settings)?;
        Ok(())
    }

    // PUT /b/:bucket_name
    pub async fn update_bucket(
        State(components): State<Arc<RwLock<HttpServerComponents>>>,
        Path(bucket_name): Path<String>,
        settings: BucketSettings,
    ) -> Result<(), HttpError> {
        let mut components = components.write().unwrap();
        let bucket = components.storage.get_bucket(&bucket_name)?;
        bucket.set_settings(settings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::asset_manager::ZipAssetManager;
    use crate::auth::token_auth::TokenAuthorization;
    use crate::auth::token_repository::TokenRepository;
    use crate::http_frontend::HttpServerComponents;
    use crate::storage::proto::BucketSettings;
    use crate::storage::storage::Storage;
    use std::path::PathBuf;
    use std::sync::{Arc, RwLock};

    #[tokio::test]
    async fn test_get_bucket() {
        let components = setup();
        let info = BucketApi::get_bucket(State(components), Path("bucket-1".to_string()))
            .await
            .unwrap();
        assert_eq!(info.name, "bucket-1");
    }

    #[tokio::test]
    async fn test_head_bucket() {
        let components = setup();
        BucketApi::head_bucket(State(components), Path("bucket-1".to_string()))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_create_bucket() {
        let components = setup();
        BucketApi::create_bucket(
            State(components),
            Path("bucket-2".to_string()),
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
            BucketSettings::default(),
        )
        .await
        .unwrap();
    }

    fn setup() -> Arc<RwLock<HttpServerComponents>> {
        let data_path = tempfile::tempdir().unwrap().into_path();

        let mut components = HttpServerComponents {
            storage: Storage::new(PathBuf::from(data_path.clone())),
            auth: TokenAuthorization::new(""),
            token_repo: TokenRepository::new(PathBuf::from(data_path), ""),
            console: ZipAssetManager::new(""),
        };

        components
            .storage
            .create_bucket("bucket-1", BucketSettings::default())
            .unwrap();

        Arc::new(RwLock::new(components))
    }
}
