// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::auth::policy::FullAccessPolicy;
use crate::core::status::HttpError;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::HttpServerState;
use crate::storage::proto::BucketSettings;
use axum::extract::{Path, State};
use axum::headers::HeaderMap;
use std::sync::{Arc, RwLock};

// POST /b/:bucket_name
pub async fn create_bucket(
    State(components): State<Arc<RwLock<HttpServerState>>>,
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::http_frontend::HttpServerState;
    use crate::storage::proto::BucketSettings;

    use crate::http_frontend::tests::{components, headers};

    use rstest::rstest;

    use std::sync::{Arc, RwLock};

    #[rstest]
    #[tokio::test]
    async fn test_create_bucket(components: Arc<RwLock<HttpServerState>>, headers: HeaderMap) {
        create_bucket(
            State(components),
            Path("bucket-3".to_string()),
            headers,
            BucketSettings::default(),
        )
        .await
        .unwrap();
    }
}
