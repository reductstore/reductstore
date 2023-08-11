// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::auth::policy::AuthenticatedPolicy;
use crate::http_frontend::bucket_api::FullBucketInfoAxum;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::HttpError;
use crate::http_frontend::HttpServerState;
use axum::extract::{Path, State};
use axum::headers::HeaderMap;
use std::sync::Arc;

// GET /b/:bucket_name
pub async fn get_bucket(
    State(components): State<Arc<HttpServerState>>,
    Path(bucket_name): Path<String>,
    headers: HeaderMap,
) -> Result<FullBucketInfoAxum, HttpError> {
    check_permissions(&components, headers, AuthenticatedPolicy {}).await?;
    let bucket_info = components
        .storage
        .read()
        .await
        .get_bucket(&bucket_name)?
        .info()?;
    Ok(FullBucketInfoAxum::from(bucket_info))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::http_frontend::HttpServerState;

    use crate::http_frontend::tests::{components, headers};

    use rstest::rstest;

    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_get_bucket(components: Arc<HttpServerState>, headers: HeaderMap) {
        let info = get_bucket(State(components), Path("bucket-1".to_string()), headers)
            .await
            .unwrap();
        assert_eq!(info.0.info.name, "bucket-1");
    }
}
