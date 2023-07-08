// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::auth::policy::FullAccessPolicy;
use crate::auth::proto::token::Permissions;
use crate::auth::proto::TokenCreateResponse;
use crate::core::status::HttpError;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::HttpServerState;
use axum::extract::{Path, State};
use axum::headers::HeaderMap;
use std::sync::{Arc, RwLock};

// POST /tokens/:token_name
pub async fn create_token(
    State(components): State<Arc<RwLock<HttpServerState>>>,
    Path(token_name): Path<String>,
    headers: HeaderMap,
    permissions: Permissions,
) -> Result<TokenCreateResponse, HttpError> {
    check_permissions(Arc::clone(&components), headers, FullAccessPolicy {})?;

    let mut components = components.write().unwrap();
    components.token_repo.create_token(&token_name, permissions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::asset_manager::ZipAssetManager;
    use crate::auth::token_auth::TokenAuthorization;
    use crate::auth::token_repository::create_token_repository;
    use crate::storage::storage::Storage;

    use crate::http_frontend::bucket_api::BucketApi;
    use crate::http_frontend::token_api::tests::{components, headers};
    use crate::storage::proto::BucketSettings;
    use axum::headers::Authorization;
    use rstest::{fixture, rstest};
    use std::path::PathBuf;

    #[rstest]
    #[tokio::test]
    async fn test_create_token(components: Arc<RwLock<HttpServerState>>, headers: HeaderMap) {
        let token = create_token(
            State(components),
            Path("new-token".to_string()),
            headers,
            Permissions::default(),
        )
        .await
        .unwrap();
        assert!(token.value.starts_with("new-token"));
    }
}
