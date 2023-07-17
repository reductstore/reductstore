// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::auth::policy::AuthenticatedPolicy;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::HttpServerState;
use crate::storage::proto::FullBucketInfo;
use axum::extract::{Path, State};
use axum::headers::HeaderMap;
use reduct_base::error::HttpError;
use std::sync::{Arc, RwLock};

// GET /b/:bucket_name
pub async fn get_bucket(
    State(components): State<Arc<RwLock<HttpServerState>>>,
    Path(bucket_name): Path<String>,
    headers: HeaderMap,
) -> Result<FullBucketInfo, HttpError> {
    check_permissions(Arc::clone(&components), headers, AuthenticatedPolicy {})?;
    let mut components = components.write().unwrap();
    components.storage.get_bucket(&bucket_name)?.info()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::http_frontend::HttpServerState;

    use crate::http_frontend::tests::{components, headers};

    use rstest::rstest;

    use std::sync::{Arc, RwLock};

    #[rstest]
    #[tokio::test]
    async fn test_get_bucket(components: Arc<RwLock<HttpServerState>>, headers: HeaderMap) {
        let info = get_bucket(State(components), Path("bucket-1".to_string()), headers)
            .await
            .unwrap();
        assert_eq!(info.info.unwrap().name, "bucket-1");
    }
}
