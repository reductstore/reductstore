// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::auth::proto::Token;
use crate::core::status::HttpError;
use crate::http_frontend::http_server::HttpServerComponents;
use crate::storage::proto::{BucketInfoList, ServerInfo};
use hyper::{body::Incoming as IncomingBody, Request};
use std::sync::{Arc, RwLock};

pub struct ServerApi {}

impl ServerApi {
    // GET /info
    pub async fn info<Body>(
        components: Arc<RwLock<HttpServerComponents>>,
        _: Request<Body>,
    ) -> Result<ServerInfo, HttpError> {
        components.read().unwrap().storage.info()
    }

    // GET /list
    pub async fn list<Body>(
        components: Arc<RwLock<HttpServerComponents>>,
        _: Request<Body>,
    ) -> Result<BucketInfoList, HttpError> {
        components.read().unwrap().storage.get_bucket_list()
    }

    // GET /me
    pub async fn me<Body>(
        components: Arc<RwLock<HttpServerComponents>>,
        req: Request<Body>,
    ) -> Result<Token, HttpError> {
        let headers = req.headers().clone();
        let header = match headers.get("Authorization") {
            Some(header) => header.to_str().ok(),
            None => None,
        };
        components.read().unwrap().token_repo.validate_token(header)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::asset_manager::ZipAssetManager;
    use crate::auth::token_auth::TokenAuthorization;
    use crate::auth::token_repository::TokenRepository;
    use crate::storage::proto::BucketSettings;
    use crate::storage::storage::Storage;
    use hyper::http::request::Builder;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_info() {
        let (mut components, req) = setup();
        let info = ServerApi::info(components, req).await.unwrap();
        assert_eq!(info.bucket_count, 2);
    }

    #[tokio::test]
    async fn test_list() {
        let (mut components, req) = setup();

        let list = ServerApi::list(components, req).await.unwrap();
        assert_eq!(list.buckets.len(), 2);
    }

    #[tokio::test]
    async fn test_me() {
        let (mut components, req) = setup();
        let token = ServerApi::me(components, req).await.unwrap();
        assert_eq!(token.name, "AUTHENTICATION-DISABLED");
    }

    fn setup() -> (Arc<RwLock<HttpServerComponents>>, Request<()>) {
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
        components
            .storage
            .create_bucket("bucket-2", BucketSettings::default())
            .unwrap();

        let req = Builder::new().body(()).unwrap();
        (Arc::new(RwLock::new(components)), req)
    }
}
