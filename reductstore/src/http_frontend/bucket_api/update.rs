// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::auth::policy::FullAccessPolicy;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::{HttpError, HttpServerState};
use crate::storage::proto::BucketSettings;
use axum::extract::{Path, State};
use axum::headers::HeaderMap;
use std::sync::Arc;

// PUT /b/:bucket_name
pub async fn update_bucket(
    State(components): State<Arc<HttpServerState>>,
    Path(bucket_name): Path<String>,
    headers: HeaderMap,
    settings: BucketSettings,
) -> Result<(), HttpError> {
    check_permissions(&components, headers, FullAccessPolicy {}).await?;
    let mut storage = components.storage.write().await;

    Ok(storage
        .get_mut_bucket(&bucket_name)?
        .set_settings(settings.into())?)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::http_frontend::HttpServerState;
    use crate::storage::proto::BucketSettings;

    use crate::http_frontend::tests::{components, headers};

    use rstest::rstest;

    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_update_bucket(components: Arc<HttpServerState>, headers: HeaderMap) {
        update_bucket(
            State(components),
            Path("bucket-1".to_string()),
            headers,
            BucketSettings::default(),
        )
        .await
        .unwrap();
    }
}
