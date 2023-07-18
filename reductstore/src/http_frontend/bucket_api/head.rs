// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::auth::policy::AuthenticatedPolicy;
use crate::core::status::HttpError;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::HttpServerState;
use axum::extract::{Path, State};
use axum::headers::HeaderMap;
use std::sync::{Arc, RwLock};

// HEAD /b/:bucket_name
pub async fn head_bucket(
    State(components): State<Arc<HttpServerState>>,
    Path(bucket_name): Path<String>,
    headers: HeaderMap,
) -> Result<(), HttpError> {
    check_permissions(&components, headers, AuthenticatedPolicy {}).await?;
    components.storage.read().await.get_bucket(&bucket_name)?;
    Ok(())
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
    async fn test_head_bucket(components: Arc<HttpServerState>, headers: HeaderMap) {
        head_bucket(State(components), Path("bucket-1".to_string()), headers)
            .await
            .unwrap();
    }
}
