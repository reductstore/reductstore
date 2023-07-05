// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::auth::policy::AuthenticatedPolicy;
use crate::core::status::HttpError;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::HttpServerState;
use crate::storage::proto::BucketInfoList;
use axum::extract::State;
use axum::headers::HeaderMap;
use std::sync::{Arc, RwLock};

// GET /list
pub async fn list(
    State(components): State<Arc<RwLock<HttpServerState>>>,
    headers: HeaderMap,
) -> Result<BucketInfoList, HttpError> {
    check_permissions(Arc::clone(&components), headers, AuthenticatedPolicy {})?;
    components.read().unwrap().storage.get_bucket_list()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_frontend::server_api::tests::tmp_components;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_list(tmp_components: Arc<RwLock<HttpServerState>>) {
        let list = list(State(tmp_components), HeaderMap::new()).await.unwrap();
        assert_eq!(list.buckets.len(), 2);
    }
}
